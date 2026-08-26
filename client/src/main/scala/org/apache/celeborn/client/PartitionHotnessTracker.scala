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
 * location. All fields are mutated under this instance's monitor.
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
 * LifecycleManager (latest partition locations, worker availability, mapper counts) are
 * injected as functions, and all timestamps are passed in by the caller (driven by an
 * injectable clock) so the tracker can be tested in isolation.
 */
private[client] class PartitionHotnessTracker(
    conf: CelebornConf,
    latestEpoch: (Int, Int) => Option[Int],
    workerAvailableByLocation: PartitionLocation => Boolean,
    numMappersOf: Int => Int) extends Logging {

  private val adaptivePartitionWriteParallelismMaxLocations =
    conf.clientShuffleAdaptivePartitionWriteParallelismMaxLocations
  private val adaptivePartitionWriteParallelismHotWindowMs =
    conf.clientShuffleAdaptivePartitionWriteParallelismHotWindowMs

  /**
   * The cap on a partition's desired location count: the configured maxLocations when set to a
   * positive value, otherwise the shuffle's number of mappers (routing is mapId % activeCount,
   * so more locations than mappers can never be utilized). Falls back to the configured value
   * when the mapper count is not (yet) available.
   */
  private def locationCap(shuffleId: Int): Int =
    if (adaptivePartitionWriteParallelismMaxLocations > 0) {
      adaptivePartitionWriteParallelismMaxLocations
    } else {
      math.max(1, numMappersOf(shuffleId))
    }

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
   * file keeps accepting writes until it hard-splits), so the epoch is retained in the active
   * set as a routing target; every other cause removes it. Removal is final — a late
   * SOFT_SPLIT report of an already-removed epoch does not resurrect it.
   *
   * Hotness judgment: a retire is measured when the cause is SOFT_SPLIT or HARD_SPLIT (both
   * reflect a threshold crossing, i.e. a fast fill) and the worker is still available; push
   * failure causes and unavailable workers only retire. fillTime = now - allocTime(epoch);
   * a fill faster than the hot window raises desired proportionally: the measured fillTime
   * is per-location under the current parallelism K, so the aggregate fill rate is
   * K / fillTime and target = ceil(K * window / fillTime) pushes the per-location fill
   * time above the window (without the K factor the target is underestimated K-fold once
   * K > 1 and desired freezes at the value judged once under K ~ 1). The judgment jumps
   * straight to the target; desired is monotone and capped, and each epoch is judged at
   * most once, so no debounce is needed.
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
    // Capture under one monitor: whether the epoch was active before the report, whether the
    // report retained it, and the active set size after — together these reconstruct the
    // parallelism K under which the location filled, and whether the report changed the active
    // set (duplicate reports from other executors are idempotent no-ops).
    val (epochWasActive: Boolean, epochRetained: Boolean, activeCountAfterRetire: Int) =
      hotState.synchronized {
        val boxed = Integer.valueOf(epoch)
        val wasActive = hotState.activeEpochs.contains(boxed)
        val retained =
          if (cause.contains(StatusCode.SOFT_SPLIT) && workerAvailable
            && !hotState.hardRetiredEpochs.contains(boxed)) {
            hotState.activeEpochs.add(boxed)
            true
          } else {
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
      if (epochWasActive) {
        logInfo(s"Partition $shuffleId-$partitionId epoch $epoch retired, not measured " +
          s"(cause ${cause.getOrElse("unknown")} not split-related or worker unavailable).")
      } else {
        logDebug(s"Partition $shuffleId-$partitionId epoch $epoch retired (duplicate report), " +
          s"not measured (cause ${cause.getOrElse("unknown")} not split-related or worker unavailable).")
      }
      return
    }
    if (hotState.splitReported.contains(Integer.valueOf(epoch))) {
      return
    }
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
      markSplitReported(hotState, epoch)
      logInfo(s"Partition $shuffleId-$partitionId epoch $epoch retired " +
        s"(cause ${cause.get}), not hot: " +
        (if (allocTime == null) "alloc time unknown."
         else s"fill time ${nowMs - allocTime}ms >= window " +
           s"${adaptivePartitionWriteParallelismHotWindowMs}ms."))
      return
    }
    hotState.synchronized {
      if (hotState.splitReported.add(Integer.valueOf(epoch))) {
        // Floor fillTime at 1ms: a report in the same millisecond as the allocation would
        // otherwise compute ceil(window / 0) = Infinity and pin desired to Int.MaxValue.
        val fillTimeMs = math.max(1L, nowMs - allocTime)
        // K = locations active while this one filled: the post-report set size, plus the
        // retired epoch itself when the report removed it (a retained SOFT epoch is already
        // counted). K = 1 reproduces the single-location formula exactly.
        val parallelismDuringFill =
          math.max(1, activeCountAfterRetire + (if (epochWasActive && !epochRetained) 1 else 0))
        val target = math.ceil(
          parallelismDuringFill * adaptivePartitionWriteParallelismHotWindowMs.toDouble / fillTimeMs).toInt
        val newDesired = math.min(locationCap(shuffleId), target)
        hotState.judgedSplits += 1
        if (newDesired > hotState.desired) {
          hotState.desired = newDesired
          logInfo(s"Partition $shuffleId-$partitionId epoch $epoch filled one of " +
            s"$parallelismDuringFill active location(s) in ${fillTimeMs}ms (< window " +
            s"${adaptivePartitionWriteParallelismHotWindowMs}ms), boost desired location count " +
            s"to ${hotState.desired}.")
        } else {
          // Log every measured fill time (not only boosts) so the hot window can be tuned
          // from observed fill times.
          logInfo(s"Partition $shuffleId-$partitionId epoch $epoch filled one of " +
            s"$parallelismDuringFill active location(s) in ${fillTimeMs}ms (< window " +
            s"${adaptivePartitionWriteParallelismHotWindowMs}ms), computed target $newDesired " +
            s"location(s) does not exceed current desired ${hotState.desired}, no boost.")
        }
      }
    }
  }

  private def markSplitReported(hotState: HotState, epoch: Int): Unit = {
    hotState.synchronized {
      hotState.splitReported.add(Integer.valueOf(epoch))
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

  /**
   * Record when the location of a newly allocated epoch was reserved. Test-only: production
   * registers allocation times via [[registerAllocation]].
   */
  private[client] def recordAllocTime(
      shuffleId: Int,
      partitionId: Int,
      epoch: Int,
      nowMs: Long): Unit = {
    getOrCreateHotState(shuffleId, partitionId).allocTimeMs.putIfAbsent(epoch, nowMs)
  }

  /**
   * Register the actual epochs reserved for a partition: add each to the active epoch set (if
   * not already present) and record its allocation time. The epochs MUST be read back from the
   * reserve result (post any reserve retry) — reserveSlotsWithRetry retries a failed location
   * with a different epoch, so a pre-reserve epoch plan diverges from the reserved reality and
   * leaks slots / inflates the active set.
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
   * unavailable (excluded / shutting). This keeps the active set from being inflated by
   * dead-worker epochs (which would silently shrink the allocation gap) and stops the
   * full-set reply from advertising locations on dead workers as writable targets.
   * Removal is final: a late SOFT_SPLIT report of such an epoch must not resurrect it.
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
