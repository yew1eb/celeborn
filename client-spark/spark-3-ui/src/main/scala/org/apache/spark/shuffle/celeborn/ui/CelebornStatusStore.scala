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

package org.apache.spark.shuffle.celeborn.ui

import org.apache.spark.util.kvstore.{KVStore, KVStoreView}

/**
 * Thin wrapper over Spark's status KVStore exposing typed read accessors for the
 *  Celeborn UI entities written by [[CelebornListener]]. Accepts the base KVStore so it
 *  works for both the live UI (ElementTrackingStore) and HistoryServer replay.
 */
private[celeborn] class CelebornStatusStore(val store: KVStore) {

  /** Build info summary, or an empty entity if not yet written. */
  def buildInfo(): CelebornBuildInfoUIData = {
    val kClass = classOf[CelebornBuildInfoUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => new CelebornBuildInfoUIData(Seq.empty)
    }
  }

  /** All recorded shuffle assignments (shuffle -> worker topology), newest last. */
  def assignmentInfos(): Seq[CelebornShuffleAssignmentUIData] = {
    viewToSeq(store.view(classOf[CelebornShuffleAssignmentUIData]))
  }

  /** Per-policy fallback counts snapshot, or empty if no fallback recorded. */
  def fallbackStats(): CelebornFallbackStatsUIData = {
    val kClass = classOf[CelebornFallbackStatsUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException =>
        new CelebornFallbackStatsUIData(new java.util.HashMap[String, java.lang.Long]())
    }
  }

  /** Celeborn properties (spark.celeborn.*) captured at job start, or empty if none. */
  def celebornProperties(): CelebornPropertiesUIData = {
    val kClass = classOf[CelebornPropertiesUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => new CelebornPropertiesUIData(Seq.empty)
    }
  }

  /** Reassign status snapshot, or all-false if no reassign recorded. */
  def reassignStats(): CelebornReassignStatsUIData = {
    val kClass = classOf[CelebornReassignStatsUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException =>
        new CelebornReassignStatsUIData(false, false, false, 0L)
    }
  }

  /** Aggregated write-path timing breakdown, or all-zero if none recorded. */
  def writeTimes(): CelebornWriteTimesUIData = {
    val kClass = classOf[CelebornWriteTimesUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => new CelebornWriteTimesUIData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    }
  }

  /** Per-worker push stats, newest last. */
  def perWorkerWriteStats(): Seq[CelebornPerWorkerWriteStatsUIData] = {
    viewToSeq(store.view(classOf[CelebornPerWorkerWriteStatsUIData]))
  }

  /** Aggregated read-path timing breakdown, or all-zero if none recorded. */
  def readTimes(): CelebornReadTimesUIData = {
    val kClass = classOf[CelebornReadTimesUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException =>
        new CelebornReadTimesUIData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    }
  }

  /** Per-worker read stats, newest last. */
  def perWorkerReadStats(): Seq[CelebornPerWorkerReadStatsUIData] = {
    viewToSeq(store.view(classOf[CelebornPerWorkerReadStatsUIData]))
  }

  /** Per-shuffle write metrics snapshot, or empty if none recorded. */
  def aggregatedWriteMetrics(): CelebornAggregatedWriteMetricsUIData = {
    val kClass = classOf[CelebornAggregatedWriteMetricsUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException =>
        new CelebornAggregatedWriteMetricsUIData(new java.util.HashMap[
          Int,
          AggregatedShuffleWriteMetric]())
    }
  }

  /** Global read/write totals snapshot, or zeros if none recorded. */
  def aggregatedTaskInfo(): CelebornAggregatedTaskInfoUIData = {
    val kClass = classOf[CelebornAggregatedTaskInfoUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => new CelebornAggregatedTaskInfoUIData(0L, 0L, 0L, 0L, 0L)
    }
  }

  private def viewToSeq[T](view: KVStoreView[T]): Seq[T] = {
    import scala.collection.JavaConverters._
    org.apache.spark.util.Utils.tryWithResource(view.closeableIterator())(iter =>
      iter.asScala.toList)
  }
}
