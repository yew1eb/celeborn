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
 * Driver-side hot state of one (shuffleId, partitionId). All fields are mutated under this
 * instance's monitor.
 */
private[client] class HotState {
  // Writable epochs, insertion-ordered. SOFT_SPLIT epochs of available workers stay here
  // (still writable until hard split); hard-split / failed epochs are removed.
  val activeEpochs = new util.LinkedHashSet[Integer]()
  // Epochs retired by a non-soft cause (or a soft split of an unavailable worker). A late
  // SOFT_SPLIT report of such an epoch must not resurrect it into the writable set.
  val hardRetiredEpochs: util.Set[Integer] = ConcurrentHashMap.newKeySet[Integer]()
  // epoch -> when the location of the epoch was allocated (slots reserved) by this manager.
  val allocTimeMs = JavaUtils.newConcurrentHashMap[Int, java.lang.Long]()
  // Epochs whose first split report has been judged: each epoch is judged at most once.
  val splitReported: util.Set[Integer] = ConcurrentHashMap.newKeySet[Integer]()
  // Desired total number of active locations of the partition. Monotone increasing,
  // capped at maxLocationsPerPartition.
  @volatile var desired: Int = 1
}

/**
 * Tracks the per-partition hot state behind adaptive partition write parallelism: how many
 * writable locations each partition should have. LifecycleManager dependencies (latest
 * partition locations, worker availability, mapper counts) are injected as functions and all
 * timestamps are passed in by the caller, so the tracker can be tested in isolation.
 */
private[client] class PartitionHotnessTracker(
    conf: CelebornConf,
    latestEpoch: (Int, Int) => Option[Int],
    workerAvailableByLocation: PartitionLocation => Boolean) extends Logging {

  private val adaptivePartitionWriteParallelismMaxLocations =
    conf.clientShuffleAdaptivePartitionWriteParallelismMaxLocations
  private val adaptivePartitionWriteParallelismMinSplitIntervalMs =
    conf.clientShuffleAdaptivePartitionWriteParallelismMinSplitIntervalMs

  // shuffleId -> (partitionId -> hot state). Sparse: only partitions revived in
  // adaptive-parallelism mode get an entry (see currentActiveEpochs).
  private val partitionHotStates =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[Integer, HotState]]()

  // shuffleId -> when its initial (epoch 0) locations were allocated at registerShuffle.
  private val shuffleInitialAllocTimeMs =
    JavaUtils.newConcurrentHashMap[Int, java.lang.Long]()

  // shuffleId -> cap on any partition's desired location count. Stable per shuffle (numMappers
  // is fixed at registerShuffle and the configured maxLocations is static), so it is computed
  // once in recordInitialAllocTime: min(configured maxLocations when positive, numMappers).
  // Routing is mapId % activeCount, so more locations than mappers can never be utilized.
  private val shuffleParallelismCap =
    JavaUtils.newConcurrentHashMap[Int, java.lang.Integer]()

  /**
   * Process the retire report of one epoch.
   *
   * Active-set maintenance: a SOFT_SPLIT location of an available worker stays writable (the
   * file keeps accepting writes until it hard-splits), so the epoch is retained in the active
   * set; every other cause removes it. Removal is final — a late SOFT_SPLIT report of an
   * already-removed epoch does not resurrect it, and each epoch is judged at most once.
   *
   * Hotness judgment: a retire is measured when the cause is SOFT_SPLIT or HARD_SPLIT and the
   * worker is still available; push failure causes and unavailable workers only retire. A fill
   * faster than the minimum split interval raises desired to ceil(K * interval / fillTime):
   * the measured fillTime is the single-location fill time under parallelism K, so the target
   * must be scaled by K. Desired is monotone and capped, so no debounce is needed.
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
    // report retained it, and the active set size after — these reconstruct the parallelism K.
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
    if (allocTime == null || nowMs - allocTime >= adaptivePartitionWriteParallelismMinSplitIntervalMs) {
      hotState.synchronized {
        hotState.splitReported.add(Integer.valueOf(epoch))
      }
      return
    }
    hotState.synchronized {
      if (hotState.splitReported.add(Integer.valueOf(epoch))) {
        // Floor fillTime at 1ms: a report in the same millisecond as the allocation would
        // otherwise compute ceil(interval / 0) = Infinity and pin desired to Int.MaxValue.
        val fillTimeMs = math.max(1L, nowMs - allocTime)
        // K = locations active while this one filled: the post-report set size, plus the
        // retired epoch itself when the report removed it.
        val parallelismDuringFill =
          math.max(1, activeCountAfterRetire + (if (epochWasActive && !epochRetained) 1 else 0))
        val target = math.ceil(
          parallelismDuringFill * adaptivePartitionWriteParallelismMinSplitIntervalMs.toDouble / fillTimeMs).toInt
        // Unregistered shuffle defaults to 1 (unreachable in practice: registerShuffle
        // precedes any revive).
        val cap = shuffleParallelismCap.getOrDefault(shuffleId, 1).intValue()
        val newDesired = math.min(cap, target)
        if (newDesired > hotState.desired) {
          hotState.desired = newDesired
          logInfo(s"Partition $shuffleId-$partitionId: fillTime ${fillTimeMs}ms under " +
            s"parallelism $parallelismDuringFill, boost desired location count to $newDesired.")
        }
      }
    }
  }

  /**
   * Record when the initial (epoch 0) locations of a shuffle were allocated at
   * registerShuffle, so the fill time of epoch 0 can be measured, and compute the shuffle's
   * parallelism cap from its (fixed) number of mappers. The registerShuffle RPC is sent lazily
   * by the executor's first pushing mapper, so this timestamp is the write start of the
   * shuffle — the same semantics as the slot-reservation time recorded for later epochs. A
   * repeated registration does not overwrite (the earliest writer wins).
   */
  private[client] def recordInitialAllocTime(
      shuffleId: Int,
      partitionLocations: Array[PartitionLocation],
      numMappers: Int,
      nowMs: Long): Unit = {
    if (partitionLocations != null && partitionLocations.nonEmpty) {
      shuffleInitialAllocTimeMs.putIfAbsent(shuffleId, nowMs)
      shuffleParallelismCap.computeIfAbsent(
        shuffleId,
        new util.function.Function[Int, java.lang.Integer]() {
          override def apply(s: Int): java.lang.Integer =
            if (adaptivePartitionWriteParallelismMaxLocations > 0) {
              math.max(1, math.min(adaptivePartitionWriteParallelismMaxLocations, numMappers))
            } else {
              math.max(1, numMappers)
            }
        })
    }
  }

  /**
   * Register the actual epochs reserved for a partition: add each to the active epoch set (if
   * not already present) and record its allocation time. The epochs MUST be read back from the
   * reserve result, because reserveSlotsWithRetry may retry a failed location with a different
   * epoch and a pre-reserve plan would diverge from the reserved reality.
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

  /** The active epochs: the registered entry, else derived as { latestPartitionLocation.epoch }. */
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
   * unavailable: no retire report ever arrives for a dead worker, so its epochs would
   * otherwise inflate the active set. Removal is final; no-op if the partition has no
   * hot state entry.
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

  /** Remove all hot state of a shuffle. */
  private[client] def removeShuffle(shuffleId: Int): Unit = {
    partitionHotStates.remove(shuffleId)
    shuffleInitialAllocTimeMs.remove(shuffleId)
    shuffleParallelismCap.remove(shuffleId)
  }
}
