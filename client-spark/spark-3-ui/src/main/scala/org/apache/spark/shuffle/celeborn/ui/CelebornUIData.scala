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

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.kvstore.KVIndex

/**
 * KVStore entity holding Celeborn build/version info for the UI summary.
 *  Singleton: keyed by the class name.
 */
private[celeborn] class CelebornBuildInfoUIData(val info: Seq[(String, String)]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornBuildInfoUIData].getName
}

/**
 * Per-shuffle assignment record. `appShuffleId` is the natural key (multiple rows).
 *  @KVIndexParam makes appShuffleId the primary index; no separate @KVIndex id needed
 *  (having both causes "Duplicate index __main__").
 */
private[celeborn] class CelebornShuffleAssignmentUIData(
    @KVIndexParam val appShuffleId: Int,
    val celebornShuffleId: Int,
    val workers: java.util.List[String],
    val numPartitions: Int,
    val timestamp: Long)

/** Snapshot of per-policy fallback counts. Singleton, overwritten on each fallback. */
private[celeborn] class CelebornFallbackStatsUIData(
    val counts: java.util.Map[String, java.lang.Long]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornFallbackStatsUIData].getName
}

/**
 * Celeborn-related SparkConf properties (spark.celeborn.*) captured at job start.
 *  Singleton, overwritten on each onJobStart. Mirrors Uniffle's UniffleProperties.
 */
private[celeborn] class CelebornPropertiesUIData(val info: Seq[(String, String)]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornPropertiesUIData].getName
}

/**
 * Reassign status snapshot: whether partition split / block send failure / stage retry
 *  have been triggered. Singleton, overwritten on each reassign event. Mirrors Uniffle's
 *  ReassignInfoUIData.
 */
private[celeborn] class CelebornReassignStatsUIData(
    val partitionSplit: Boolean,
    val blockSendFailure: Boolean,
    val stageRetry: Boolean,
    val timestamp: Long) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornReassignStatsUIData].getName
}

/** Per-shuffle write metrics aggregated from onTaskEnd TaskMetrics. Keyed by shuffleDepId. */
private[celeborn] class CelebornAggregatedWriteMetricsUIData(
    val metrics: java.util.Map[Int, AggregatedShuffleWriteMetric]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornAggregatedWriteMetricsUIData].getName
}

/**
 * Per-shuffle write metrics (sourced from Spark native ShuffleWriteMetrics).
 *  Mutable fields updated by the single status-queue thread; serialized as a snapshot.
 */
private[celeborn] class AggregatedShuffleWriteMetric(
    var bytesWritten: Long,
    var recordsWritten: Long,
    var writeTimeMs: Long) {
  def this() = this(0L, 0L, 0L)
}

/** Global read/write totals aggregated from onTaskEnd TaskMetrics. Singleton. */
private[celeborn] class CelebornAggregatedTaskInfoUIData(
    val shuffleWriteBytes: Long,
    val shuffleWriteTimeMs: Long,
    val shuffleReadBytes: Long,
    val shuffleFetchWaitTimeMs: Long,
    val taskCpuTimeMs: Long) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornAggregatedTaskInfoUIData].getName
}

/**
 * Aggregated write-path timing breakdown (ms) across all tasks, mirroring Uniffle's
 *  ShuffleWriteTimes. Singleton, overwritten on each CelebornWriteMetricsEvent.
 */
private[celeborn] class CelebornWriteTimesUIData(
    val copyTimeMs: Long,
    val serializeTimeMs: Long,
    val compressTimeMs: Long,
    val queueWaitTimeMs: Long,
    val queueStallTimeMs: Long,
    val inflightWaitTimeMs: Long,
    val drainWaitTimeMs: Long,
    val slowPushCount: Long,
    val maxPushRttMs: Long) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornWriteTimesUIData].getName
}

/**
 * Per-worker push stats aggregated from CelebornWriteMetricsEvent's PushWorkerStats.
 *  `workerId` is the natural key (multiple rows).
 */
private[celeborn] class CelebornPerWorkerWriteStatsUIData(
    @KVIndexParam val workerId: String,
    val pushCount: Long,
    val pushBytes: Long,
    val totalPushRttNanos: Long,
    val softSplitCount: Long,
    val hardSplitCount: Long,
    val primaryCongestedCount: Long,
    val replicaCongestedCount: Long,
    val lastPushFailureReason: String)
