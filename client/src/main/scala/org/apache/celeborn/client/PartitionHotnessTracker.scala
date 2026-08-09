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
  // Active epochs, insertion-ordered. Registered only for partitions that ever revived in
  // adaptive-parallelism mode; other partitions derive their active set as
  // { latestPartitionLocation.epoch }.
  val activeEpochs = new util.LinkedHashSet[Integer]()
  // epoch -> when the location of the epoch was allocated (slots reserved) by this manager.
  val allocTimeMs = JavaUtils.newConcurrentHashMap[Int, java.lang.Long]()
  // Epochs whose first split report has been judged (dedupe repeated reports).
  val splitReported: util.Set[Integer] = ConcurrentHashMap.newKeySet[Integer]()
  // Desired total number of active locations of the partition. Monotone increasing,
  // capped at maxLocationsPerPartition.
  @volatile var desired: Int = 1
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

  private def removeActiveEpoch(shuffleId: Int, partitionId: Int, epoch: Int): HotState = {
    val map = partitionHotStates.get(shuffleId)
    if (map != null) {
      val entry = map.get(partitionId)
      if (entry != null) {
        entry.synchronized {
          entry.activeEpochs.remove(Integer.valueOf(epoch))
        }
      }
      entry
    } else {
      null
    }
  }

  /**
   * Retire the epoch from the active epoch set and, when the retire is measure-eligible,
   * judge whether the partition is hot: fillTime = now - allocTime(epoch). A location
   * filled faster than the configured hot partition window raises the desired location
   * count proportionally (see below), capped at maxLocationsPerPartition.
   *
   * A retire is measure-eligible when the cause is SOFT_SPLIT or HARD_SPLIT and the worker
   * of the retired location is still available: both split kinds reflect a threshold
   * crossing (i.e. a fast fill), but only when the worker itself is healthy. A HARD_SPLIT
   * of a known-unavailable worker, a null oldPartition, and all push failure causes only
   * retire and never boost. Only the first eligible report of an epoch is judged; epochs
   * with unknown allocTime (e.g. legacy data) conservatively never boost.
   *
   * Proportional step-up (no per-window debounce): with K active locations each one fills
   * in ~K * fillTime, so K = ceil(window / fillTime) locations push the per-location fill
   * time above the window. The judgment jumps straight to that level instead of climbing
   * +1 per window, so a very hot partition reaches the cap after its first split report.
   * desired is monotone and capped, and each epoch is judged at most once, so no debounce
   * is needed to bound the total number of boosts.
   */
  private[client] def onEpochRetired(
      shuffleId: Int,
      partitionId: Int,
      epoch: Int,
      oldPartition: PartitionLocation,
      cause: Option[StatusCode],
      nowMs: Long): Unit = {
    val hotState = removeActiveEpoch(shuffleId, partitionId, epoch)
    val measureEligible =
      (cause.contains(StatusCode.SOFT_SPLIT) || cause.contains(StatusCode.HARD_SPLIT)) &&
        workerAvailableByLocation(oldPartition)
    if (!measureEligible) {
      return
    }
    if (hotState != null && hotState.splitReported.contains(Integer.valueOf(epoch))) {
      return
    }
    val allocTime = {
      val recorded = if (hotState == null) null else hotState.allocTimeMs.get(epoch)
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
      return
    }
    val state = getOrCreateHotState(shuffleId, partitionId)
    state.synchronized {
      if (state.splitReported.add(Integer.valueOf(epoch))) {
        val fillTimeMs = nowMs - allocTime
        val target =
          math.ceil(adaptivePartitionWriteParallelismHotWindowMs.toDouble / fillTimeMs).toInt
        val newDesired = math.min(adaptivePartitionWriteParallelismMaxLocations, target)
        if (newDesired > state.desired) {
          state.desired = newDesired
          logInfo(s"Partition $shuffleId-$partitionId filled a location in " +
            s"${fillTimeMs}ms (< ${adaptivePartitionWriteParallelismHotWindowMs}ms), " +
            s"boost desired location count to ${state.desired}.")
        }
      }
    }
  }

  private def markSplitReported(hotState: HotState, epoch: Int): Unit = {
    if (hotState != null) {
      hotState.synchronized {
        hotState.splitReported.add(Integer.valueOf(epoch))
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

  /** The desired total location count of a partition: the registered value, 1 if none. */
  private[client] def desiredLocationCount(shuffleId: Int, partitionId: Int): Int = {
    val map = partitionHotStates.get(shuffleId)
    val entry = if (map == null) null else map.get(partitionId)
    if (entry == null) 1 else entry.desired
  }

  private[client] def getOrCreateHotState(shuffleId: Int, partitionId: Int): HotState = {
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

  /** Remove all hot state of a shuffle. */
  private[client] def removeShuffle(shuffleId: Int): Unit = {
    partitionHotStates.remove(shuffleId)
    shuffleInitialAllocTimeMs.remove(shuffleId)
  }
}
