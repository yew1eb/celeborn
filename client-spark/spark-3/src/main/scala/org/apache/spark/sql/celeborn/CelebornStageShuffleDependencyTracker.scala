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
 * Tracks stage-level shuffle dependencies so that a shuffle can be unregistered as soon as its
 * last reader stage has completed (after an optional delay guarding shuffle reuse), instead of
 * waiting for the whole query to end or for driver GC.
 */
private[celeborn] class CelebornStageShuffleDependencyTracker private[celeborn] (
    delayMs: Long,
    deletionFunc: Int => Unit)
  extends Logging {

  private val stageIdToShuffleIdOfWriters = new ConcurrentHashMap[Int, Int]()
  private val shuffleIdToStageIdsOfReaders = new ConcurrentHashMap[Int, util.Set[Int]]()
  private val stageIdToShuffleIdOfReaders = new ConcurrentHashMap[Int, util.Set[Int]]()

  private val deletionDelayQueue = new DelayQueue[ShuffleDeletionItem]()
  private val deletionExecutor =
    ThreadUtils.newDaemonSingleThreadExecutor("celeborn-stage-shuffle-cleaner")

  @volatile private var cleanedShuffleCount = 0

  deletionExecutor.execute(() => deletionLoop())

  private def deletionLoop(): Unit = {
    while (true) {
      try {
        val shuffleId = deletionDelayQueue.take().shuffleId
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
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          return
        case e: Exception =>
          logError("Errors on deleting shuffle", e)
      }
    }
  }

  def getShuffleIdByStageIdOfWriter(stageId: Int): Int = {
    stageIdToShuffleIdOfWriters.getOrDefault(stageId, -1)
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
    Option(stageIdToShuffleIdOfReaders.get(stageId)).foreach { upstreamShuffleIds =>
      upstreamShuffleIds.asScala.foreach { shuffleId =>
        Option(shuffleIdToStageIdsOfReaders.get(shuffleId)).foreach { readers =>
          readers.remove(stageId)
          if (readers.isEmpty) {
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
