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

package org.apache.spark.sql.celeborn

import java.util
import java.util.concurrent.{ConcurrentHashMap, Delayed, DelayQueue, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging

import org.apache.celeborn.common.util.ThreadUtils

/**
 * A shuffle deletion item in the delayed deletion queue, expiring at `expireAtMs`.
 */
private[celeborn] class ShuffleDeletionItem(val shuffleId: Int, delayMs: Long)
  extends Delayed {

  private val expireAtMs: Long = System.currentTimeMillis() + math.max(delayMs, 0L)

  override def getDelay(unit: TimeUnit): Long =
    unit.convert(expireAtMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS)

  override def compareTo(other: Delayed): Int = {
    if (other eq this) {
      0
    } else {
      java.lang.Long.compare(
        this.getDelay(TimeUnit.MILLISECONDS),
        other.getDelay(TimeUnit.MILLISECONDS))
    }
  }
}

/**
 * Tracks the dependencies between stages to manage the lifecycle of shuffle data. It maintains
 * the mapping of writer stages to their shuffles and the reference counts from reader stages, so
 * that a shuffle can be unregistered as soon as its last reader stage has completed, instead of
 * waiting for the whole query to end or for the driver GC. For the shuffle-reuse scenario, a
 * delayed deletion mechanism avoids the too-early deletion issue: when the item is dequeued, the
 * readers are checked again and the deletion is skipped if new reader stages have linked.
 *
 * This is a port of Apache Uniffle's StageDependencyTracker (apache/uniffle#2704) with a daemon
 * deletion thread, and it is still a best-effort mechanism that may not cover all edge cases.
 *
 * @param delayMs delayed milliseconds before deleting a shuffle whose reader count dropped to
 *                zero; non-positive value deletes immediately
 * @param deletionFunc callback to unregister a shuffle, invoked on a single daemon thread
 */
private[celeborn] class CelebornStageShuffleDependencyTracker private[celeborn] (
    delayMs: Long,
    deletionFunc: Int => Unit)
  extends Logging {

  // key: stageId, value: shuffleId of the writer of this stage
  private val stageIdToShuffleIdOfWriters = new ConcurrentHashMap[Int, Int]()

  // key: shuffleId, value: stageIds of readers
  private val shuffleIdToStageIdsOfReaders = new ConcurrentHashMap[Int, util.Set[Int]]()
  // reverse link by the stageId
  private val stageIdToShuffleIdOfReaders = new ConcurrentHashMap[Int, util.Set[Int]]()

  private val deletionDelayQueue = new DelayQueue[ShuffleDeletionItem]()

  private val deletionExecutor =
    ThreadUtils.newDaemonSingleThreadExecutor("celeborn-stage-shuffle-cleaner")

  // for test cases
  @volatile private var cleanedShuffleCount = 0

  deletionExecutor.execute(() => deletionLoop())

  private def deletionLoop(): Unit = {
    while (true) {
      try {
        val item = deletionDelayQueue.take()
        val shuffleId = item.shuffleId
        // check references again to guard against shuffle reuse
        val readers = shuffleIdToStageIdsOfReaders.get(shuffleId)
        if (readers == null || readers.isEmpty) {
          logInfo(s"Deleting shuffle data for shuffleId: $shuffleId")
          deletionFunc(shuffleId)
          cleanedShuffleCount += 1
          shuffleIdToStageIdsOfReaders.remove(shuffleId)
        } else {
          logInfo(s"Skipping deletion for shuffleId: $shuffleId as it has new readers")
        }
      } catch {
        case e: InterruptedException =>
          logInfo("Interrupted while waiting for deletion delay queue", e)
          Thread.currentThread().interrupt()
          return
        case e: Exception =>
          logError("Errors on deleting shuffle", e)
      }
    }
  }

  def getShuffleIdByStageIdOfWriter(stageId: Int): Int = {
    if (stageIdToShuffleIdOfWriters.containsKey(stageId)) {
      stageIdToShuffleIdOfWriters.get(stageId)
    } else {
      // the stage does not write a shuffle, ignore it
      -1
    }
  }

  def linkWriter(shuffleId: Int, writerStageId: Int): Unit = {
    stageIdToShuffleIdOfWriters.put(writerStageId, shuffleId)
  }

  def linkReader(shuffleId: Int, readerStageId: Int): Unit = {
    shuffleIdToStageIdsOfReaders
      .computeIfAbsent(shuffleId, _ => ConcurrentHashMap.newKeySet[Int]())
      .add(readerStageId)
    stageIdToShuffleIdOfReaders
      .computeIfAbsent(readerStageId, _ => ConcurrentHashMap.newKeySet[Int]())
      .add(shuffleId)
  }

  def removeStage(stageId: Int): Unit = {
    val upstreamShuffleIds = stageIdToShuffleIdOfReaders.get(stageId)
    if (upstreamShuffleIds != null) {
      upstreamShuffleIds.asScala.foreach { shuffleId =>
        val readers = shuffleIdToStageIdsOfReaders.get(shuffleId)
        if (readers != null) {
          readers.remove(stageId)
          if (readers.isEmpty) {
            // add into the delayed deletion queue
            deletionDelayQueue.offer(new ShuffleDeletionItem(shuffleId, delayMs))
          }
        }
      }
    }
  }

  def getActiveShuffleCount: Int = shuffleIdToStageIdsOfReaders.size

  def getCleanedShuffleCount: Int = cleanedShuffleCount
}

private[celeborn] object CelebornStageShuffleDependencyTracker {

  def apply(
      delayedMinutes: Int,
      deletionFunc: Int => Unit): CelebornStageShuffleDependencyTracker = {
    new CelebornStageShuffleDependencyTracker(delayedMinutes * 60L * 1000L, deletionFunc)
  }
}
