/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.client

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.JavaUtils

/**
 * Driver-side hot state of one (shuffleId, partitionId), maintained by the
 * PartitionHotnessTracker to decide whether a partition needs more than one writable
 * location.
 *
 * <p>Synchronization strategy (per field): `activeEpochs` is a plain LinkedHashSet mutated only
 * under the instance monitor (in `onEpochRetired`, `retireUnavailableWorkerEpochs`, and the
 * allocation-registration path in `ChangePartitionManager`). `hardRetiredEpochs` and
 * `splitReported` are concurrent sets, mutated under the same monitor but read lock-free
 * (`desiredLocationCount` reads `desired`). `allocTimeMs` is a CHM mutated by `putIfAbsent`
 * (lock-free, from allocation registration) and read under the monitor (in `onEpochRetired`).
 * `desired` and `judgedSplits` are volatile, written under the monitor and read lock-free.
 * The split-cause storage on the executor side (`PartitionLocationGroup`) is separate.
 */
private[client] class HotState {
  // Writable epochs, insertion-ordered. SOFT_SPLIT epochs of available workers stay here
  // (still writable until hard split); hard-split / failed epochs are removed. Registered only
  // for partitions that ever revived in adaptive-parallelism mode; other partitions derive their
  // active set as { latestPartitionLocation.epoch }.
  val activeEpochs = new util.LinkedHashSet[Integer]()
  // Epochs retired by a non-soft cause (or a soft split of an unavailable worker). A late
  // SOFT_SPLIT report of such an epoch (already in flight when the hard retire happened)
  // must not resurrect it into the writable set.
  val hardRetiredEpochs: util.Set[Integer] = ConcurrentHashMap.newKeySet[Integer]()
  // epoch -> when the location of the epoch was allocated (slots reserved) by this manager.
  val allocTimeMs = JavaUtils.newConcurrentHashMap[Int, java.lang.Long]()
  // Epochs whose first split report has been judged (dedupe repeated reports).
  val splitReported: util.Set[Integer] = ConcurrentHashMap.newKeySet[Integer]()
  // Desired total number of active locations of the partition. Monotone increasing,
  // capped at maxLocationsPerPartition.
  @volatile var desired: Int = 1
  // Number of splits judged as hot fills (for the stage-end summary log).
  @volatile var judgedSplits: Int = 0
}

/**
 * Tracks the per-partition hot state behind adaptive partition write parallelism: how many writable locations
 * each partition should have. Extracted from ChangePartitionManager, which holds one
 * instance and delegates all hot state handling to it. Dependencies on the
 * LifecycleManager (latest partition locations, worker availability) are injected as
 * functions, and all timestamps are passed in by the caller (driven by an injectable
 * clock) so the tracker can be tested in isolation.
 */
private[client] class PartitionHotnessTracker(
    conf: CelebornConf,
    latestEpoch: (Int, Int) => Option[Int],
    workerAvailableByLocation: PartitionLocation => Boolean) extends Logging {

  private val adaptivePartitionWriteParallelismMaxLocations =
    conf.clientShuffleAdaptivePartitionWriteParallelismMaxLocations
  private val adaptivePartitionWriteParallelismHotWindowMs =
    conf.clientShuffleAdaptivePartitionWriteParallelismHotWindowMs

  // shuffleId -> (partitionId -> hot state). Sparse: only partitions that have ever been
  // revived in adaptive-parallelism mode get an entry; the active set of any other partition is
  // derived as { latestPartitionLocation.epoch }.
  private val partitionHotStates =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[Integer, HotState]]()

  // shuffleId -> when its initial (epoch 0) locations were allocated at registerShuffle.
  private val shuffleInitialAllocTimeMs =
    JavaUtils.newConcurrentHashMap[Int, java.lang.Long]()

  /**
   * Process the retire report of one epoch.
   *
   * Active-set maintenance: a SOFT_SPLIT location of an available worker stays writable (the
   * file keeps accepting writes until it reaches partitionSplitMaximumSize and hard-splits),
   * so the epoch is RETAINED in the active epoch set and remains a routing target; every other
   * cause (HARD_SPLIT, push failures, or a soft split of an unavailable worker) removes the
   * epoch from the writable set. Removal is final: a late SOFT_SPLIT report of an already
   * removed epoch (sent before its hard retire) does not resurrect it.
   *
   * Hotness judgment: when the retire is measure-eligible, judge whether the partition is hot:
   * fillTime = now - allocTime(epoch). A location filled faster than the configured hot
   * partition window raises the desired location count proportionally (see below), capped at
   * maxLocationsPerPartition.
   *
   * A retire is measure-eligible when the cause is SOFT_SPLIT or HARD_SPLIT and the worker
   * of the retired location is still available: both split kinds reflect a threshold
   * crossing (i.e. a fast fill), but only when the worker itself is healthy. A HARD_SPLIT
   * of a known-unavailable worker, a null oldPartition, and all push failure causes only
   * retire and never boost. Only the first eligible report of an epoch is judged; epochs
   * with unknown allocTime (e.g. legacy data) conservatively never boost.
   *
   * Proportional step-up (no per-window debounce): with K active locations each one fills
   * in ~K * fillTime, so K = ceil(K_measured * window / fillTime_measured) locations push the
   * per-location fill time above the window — the measured fillTime is taken under the current
   * parallelism, so the K factor rescales it to the aggregate fill rate. The judgment jumps
   * straight to that level instead of climbing +1 per window, so a very hot partition reaches
   * the cap after its first split report. desired is monotone and capped, and each epoch is
   * judged at most once, so no debounce is needed to bound the total number of boosts.
   */
  private[client] def onEpochRetired(
      shuffleId: Int,
      partitionId: Int,
      epoch: Int,
      oldPartition: PartitionLocation,
      cause: Option[StatusCode],
      nowMs: Long): Unit = {
    val workerAvailable = workerAvailableByLocation(oldPartition)
    val hotState = getOrCreateHotState(shuffleId, partitionId)
    // Whether the retired epoch was in the active set before this report, whether the report
    // retained it (SOFT_SPLIT of an available worker), and the active set size after applying
    // the report — together these reconstruct the parallelism K under which the location filled
    // (see the step-up formula in the judgment below). All captured under one monitor so a
    // concurrent report cannot skew the values.
    val (wasActiveBefore: Boolean, epochRetained: Boolean, activeCountAfterRetire: Int) =
      hotState.synchronized {
        val boxed = Integer.valueOf(epoch)
        val wasActive = hotState.activeEpochs.contains(boxed)
        val retained =
          if (cause.contains(StatusCode.SOFT_SPLIT) && workerAvailable
            && !hotState.hardRetiredEpochs.contains(boxed)) {
            hotState.activeEpochs.add(boxed)
            true
          } else {
            // A retire that drops the epoch from the writable set is final: a late SOFT_SPLIT
            // report of the same epoch (sent before the hard retire) must not resurrect it.
            hotState.activeEpochs.remove(boxed)
            hotState.hardRetiredEpochs.add(boxed)
            false
          }
        (wasActive, retained, hotState.activeEpochs.size())
      }
    val measureEligible =
      (cause.contains(StatusCode.SOFT_SPLIT) || cause.contains(StatusCode.HARD_SPLIT)) &&
        workerAvailable
    if (!measureEligible) {
      // Log only when the report changed the active set — with full retire forwarding (and the
      // blocking revive attaching every outstanding retire), many executors report the same
      // (partition, epoch) independently; those duplicates are idempotent no-ops and would
      // otherwise flood the driver log (observed: thousands of identical lines per epoch).
      if (epochRetained) {
        logInfo(s"Partition $shuffleId-$partitionId epoch $epoch retired, not measured " +
          s"(cause ${cause.getOrElse("unknown")} not split-related or worker unavailable).")
      } else {
        logDebug(s"Partition $shuffleId-$partitionId epoch $epoch retired (duplicate report), " +
          s"not measured (cause ${cause.getOrElse("unknown")} not split-related or worker unavailable).")
      }
      return
    }
    // First-eligible-report dedupe and judgment happen under one monitor: splitReported.add
    // returns true only for the first eligible report of an epoch, so both the not-hot and the
    // boost branches log exactly once per epoch (the old lock-free contains-precheck followed
    // by a lock-free markSplitReported re-logged on repeat reports and was a benign but noisy
    // double check).
    hotState.synchronized {
      if (hotState.splitReported.add(Integer.valueOf(epoch))) {
        val allocTime = {
          val recorded = hotState.allocTimeMs.get(epoch)
          if (recorded != null) {
            recorded
          } else if (epoch == 0) {
            shuffleInitialAllocTimeMs.get(shuffleId)
          } else {
            null
          }
        }
        if (allocTime == null || nowMs - allocTime >= adaptivePartitionWriteParallelismHotWindowMs) {
          logDebug(s"Partition $shuffleId-$partitionId epoch $epoch retired " +
            s"(cause ${cause.get}), not hot: " +
            (if (allocTime == null) "alloc time unknown."
             else s"fill time ${nowMs - allocTime}ms >= window " +
               s"${adaptivePartitionWriteParallelismHotWindowMs}ms."))
        } else {
          // Guard against fillTime == 0 (allocTime == nowMs, e.g. a split report arriving in the
          // same millisecond as the allocation): ceil(window / 0) = Infinity would pin desired to
          // maxLocations on a single spurious report. Floor fillTime at 1ms so the step-up is
          // bounded by ceil(window / 1ms) = window(ms), still capped by maxLocations.
          val fillTimeMs = math.max(1L, nowMs - allocTime)
          // The measured fillTime is the per-location fill time under the CURRENT parallelism K
          // (the location was one of K active locations sharing the partition's write load), so
          // the aggregate fill rate the formula needs is K / fillTime. Without the K factor the
          // target is systematically underestimated K-fold once K > 1: a partition with K active
          // locations each filling in window/9 ms computes target 9 forever and never boosts
          // (observed in production: K = 38, fillTime = 1146ms, window = 10s, target stuck at 9).
          // K counts the locations that were active while this one filled. After applying the
          // retire report, a SOFT_SPLIT epoch is retained in the active set (already counted),
          // while a removed epoch (HARD_SPLIT / failure / unavailable worker) must be added
          // back once; floor 1 (K = 1 reproduces the single-location formula exactly).
          val parallelismDuringFill =
            math.max(1, activeCountAfterRetire + (if (wasActiveBefore && !epochRetained) 1 else 0))
          val target = math.ceil(
            parallelismDuringFill * adaptivePartitionWriteParallelismHotWindowMs.toDouble / fillTimeMs).toInt
          val newDesired = math.min(adaptivePartitionWriteParallelismMaxLocations, target)
          hotState.judgedSplits += 1
          if (newDesired > hotState.desired) {
            hotState.desired = newDesired
            logInfo(s"Partition $shuffleId-$partitionId epoch $epoch filled one of " +
              s"$parallelismDuringFill active location(s) in ${fillTimeMs}ms (< window " +
              s"${adaptivePartitionWriteParallelismHotWindowMs}ms), boost desired location count to " +
              s"${hotState.desired}.")
          } else {
            logDebug(s"Partition $shuffleId-$partitionId epoch $epoch filled one of " +
              s"$parallelismDuringFill active location(s) in ${fillTimeMs}ms (< window " +
              s"${adaptivePartitionWriteParallelismHotWindowMs}ms), computed target $newDesired " +
              s"location(s) does not exceed current desired ${hotState.desired}, no boost.")
          }
        }
      }
    }
  }

  /**
   * Record when the initial (epoch 0) locations of a shuffle were allocated at
   * registerShuffle, so that fill times of epoch 0 can be measured for hot partition
   * detection. putIfAbsent semantics: a repeated registration does not overwrite.
   */
  private[client] def recordInitialAllocTime(
      shuffleId: Int,
      partitionLocations: Array[PartitionLocation],
      nowMs: Long): Unit = {
    if (partitionLocations != null && partitionLocations.nonEmpty) {
      shuffleInitialAllocTimeMs.putIfAbsent(shuffleId, nowMs)
    }
  }

  /** Record when the location of a newly allocated epoch was reserved. */
  private[client] def recordAllocTime(
      shuffleId: Int,
      partitionId: Int,
      epoch: Int,
      nowMs: Long): Unit = {
    getOrCreateHotState(shuffleId, partitionId).allocTimeMs.putIfAbsent(epoch, nowMs)
  }

  /**
   * Register a batch of newly reserved epochs of a partition: add each to the active epoch set
   * (if not already present) and record its allocation time. This is the single entry point for
   * allocation registration, so the HotState internal representation does not leak to
   * ChangePartitionManager. The epochs MUST be the actually-reserved epochs (post any reserve
   * retry), not a pre-reserve plan — see B1.
   */
  private[client] def registerAllocation(
      shuffleId: Int,
      partitionId: Int,
      epochs: Set[Int],
      nowMs: Long): Unit = {
    if (epochs.isEmpty) return
    val entry = getOrCreateHotState(shuffleId, partitionId)
    entry.synchronized {
      epochs.foreach { epoch =>
        val boxed = Integer.valueOf(epoch)
        if (!entry.activeEpochs.contains(boxed)) {
          entry.activeEpochs.add(boxed)
        }
        entry.allocTimeMs.putIfAbsent(epoch, nowMs)
      }
    }
  }

  /** The desired total location count of a partition: the registered value, 1 if none. */
  private[client] def desiredLocationCount(shuffleId: Int, partitionId: Int): Int = {
    val map = partitionHotStates.get(shuffleId)
    val entry = if (map == null) null else map.get(partitionId)
    if (entry == null) 1 else entry.desired
  }

  private def getOrCreateHotState(shuffleId: Int, partitionId: Int): HotState = {
    val map = partitionHotStates.computeIfAbsent(
      shuffleId,
      new util.function.Function[Int, ConcurrentHashMap[Integer, HotState]]() {
        override def apply(s: Int): ConcurrentHashMap[Integer, HotState] =
          JavaUtils.newConcurrentHashMap()
      })
    map.computeIfAbsent(
      partitionId,
      new util.function.Function[Integer, HotState]() {
        override def apply(p: Integer): HotState = new HotState()
      })
  }

  /**
   * The active epochs of a partition: the registered entry if the partition was ever
   * revived in adaptive-parallelism mode, otherwise derived as { latestPartitionLocation.epoch }.
   */
  private[client] def currentActiveEpochs(shuffleId: Int, partitionId: Int): Set[Int] = {
    val map = partitionHotStates.get(shuffleId)
    val entry = if (map == null) null else map.get(partitionId)
    if (entry != null) {
      entry.synchronized {
        entry.activeEpochs.asScala.map(_.intValue()).toSet
      }
    } else {
      latestEpoch(shuffleId, partitionId).toSet
    }
  }

  /**
   * Hard-retire the given epochs of a partition when their hosting worker has gone
   * unavailable (excluded / shutting). This keeps the surviving active set from being
   * inflated by dead-worker epochs (which would silently shrink the allocation gap) and
   * stops the full-set reply from advertising locations on dead workers as writable targets
   * (M-L1). Removal is final: a late SOFT_SPLIT report of such an epoch must not resurrect it.
   * Idempotent: no-op if the partition has no hot state entry.
   */
  private[client] def retireUnavailableWorkerEpochs(
      shuffleId: Int,
      partitionId: Int,
      epochs: Set[Int]): Unit = {
    if (epochs.isEmpty) return
    val map = partitionHotStates.get(shuffleId)
    if (map == null) return
    val entry = map.get(partitionId)
    if (entry == null) return
    entry.synchronized {
      epochs.foreach { epoch =>
        val boxed = Integer.valueOf(epoch)
        entry.activeEpochs.remove(boxed)
        entry.hardRetiredEpochs.add(boxed)
      }
    }
  }

  /** Remove all hot state of a shuffle, logging a one-line summary of its hot partitions. */
  private[client] def removeShuffle(shuffleId: Int): Unit = {
    val map = partitionHotStates.remove(shuffleId)
    shuffleInitialAllocTimeMs.remove(shuffleId)
    if (map != null) {
      val hot = map.asScala.filter(_._2.desired > 1)
      if (hot.nonEmpty) {
        val details = hot.toSeq
          .sortBy(_._1.intValue())
          .map { case (partitionId, state) =>
            s"$partitionId(desired=${state.desired},judgedSplits=${state.judgedSplits})"
          }
          .mkString(", ")
        logInfo(s"Shuffle $shuffleId adaptive partition write parallelism summary: " +
          s"${hot.size} hot partition(s): $details")
      }
    }
  }
}
