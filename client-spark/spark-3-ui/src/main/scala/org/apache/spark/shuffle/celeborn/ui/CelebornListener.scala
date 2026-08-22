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

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.shuffle.celeborn.events.{CelebornBuildInfoEvent, CelebornFallbackEvent, CelebornReadMetricsEvent, CelebornReassignEvent, CelebornShuffleAssignmentEvent, CelebornWriteMetricsEvent}
import org.apache.spark.status.ElementTrackingStore

/**
 * Aggregates Celeborn shuffle assignment, fallback and task-level shuffle metric events
 *  into the Spark status KVStore so the Celeborn UI tab can render them. Registered to the
 *  APP_STATUS_QUEUE so events flow into the event log (for HistoryServer replay).
 *
 *  Data sources (plan A — no executor->driver metric RPC):
 *   - onTaskEnd: Spark native TaskMetrics (already written by Celeborn writer/reader).
 *   - onOtherEvent: CelebornBuildInfoEvent / CelebornShuffleAssignmentEvent / CelebornFallbackEvent
 *     posted from the driver-side plugin / SparkShuffleManager / FallbackPolicyRunner.
 */
class CelebornListener(conf: SparkConf, kvstore: ElementTrackingStore)
  extends SparkListener with Logging {

  // Per-shuffle write aggregation (key = shuffleDepId, only ShuffleMapStage has one).
  private val perShuffleWrite =
    new java.util.concurrent.ConcurrentHashMap[Int, AggregatedShuffleWriteMetric]
  // Global totals (write + read) across all tasks.
  private val totalWriteBytes = new java.util.concurrent.atomic.AtomicLong(0)
  private val totalWriteTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val totalReadBytes = new java.util.concurrent.atomic.AtomicLong(0)
  private val totalFetchWaitTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val totalTaskCpuTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  // Aggregated write-path timing breakdown (ms) from CelebornWriteMetricsEvent. Uses AtomicLong
  // so concurrent events on the status queue accumulate safely.
  private val aggCopyTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggSerializeTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggCompressTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggQueueWaitTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggQueueStallTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggInflightWaitTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggDrainWaitTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggSlowPushCount = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggMaxPushRttMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggUncompressedBytes = new java.util.concurrent.atomic.AtomicLong(0)
  // Per-worker push stats (workerId -> mutable accumulator), merged from each event.
  private val perWorkerWriteStats =
    new java.util.concurrent.ConcurrentHashMap[String, PerWorkerWriteAccumulator]
  // Aggregated read-path timing breakdown (ms) from CelebornReadMetricsEvent.
  private val aggDecompressTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggChunkWaitTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggDeserializeTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggCopyTimeMsRead = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggRetryCount = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggRetryWaitTimeMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggPeerSwitchCount = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggExcludeCount = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggSlowChunkCount = new java.util.concurrent.atomic.AtomicLong(0)
  private val aggMaxChunkRttMs = new java.util.concurrent.atomic.AtomicLong(0)
  private val perWorkerReadStats =
    new java.util.concurrent.ConcurrentHashMap[String, PerWorkerReadAccumulator]
  // stageId -> shuffleDepId (only ShuffleMapStage has a shuffleDepId). Instance-level (not
  // companion-object static) so that different applications replayed on a shared HistoryServer
  // do not cross-contaminate each other's stageId namespace.
  private val stageToShuffleMappings =
    new java.util.concurrent.ConcurrentHashMap[Int, Int]()

  private val updateIntervalMillis = 5000L
  @volatile private var lastUpdateNanos: Long = -1L

  // Register a final flush so the last <=5s of aggregations (within the throttle window) are
  // persisted on store close / replay end, mirroring AppStatusListener's onFlush hook. Without
  // this, the trailing TaskMetrics would be lost from both the live store rebuild and the
  // HistoryServer replay.
  kvstore.onFlush {
    mayUpdate(true)
  }

  // Cap the per-shuffle assignment rows to bound KVStore / event-log growth on long-running
  // jobs with many shuffles (mirrors Gluten's UI_RETAINED_EXECUTIONS trigger and Spark's
  // retainedStages). When the count exceeds the threshold, evict the oldest rows.
  private val retainedShuffles: Int =
    conf.getInt("celeborn.client.spark.ui.retainedShuffles", 1000)
  kvstore.addTrigger(classOf[CelebornShuffleAssignmentUIData], retainedShuffles.toLong) {
    count => cleanupAssignments(count)
  }

  private def cleanupAssignments(count: Long): Unit = {
    import org.apache.spark.status.KVUtils
    val toDelete = count - retainedShuffles
    if (toDelete <= 0) {
      return
    }
    val view = kvstore.view(classOf[CelebornShuffleAssignmentUIData])
    KVUtils.viewToSeq(view, toDelete.toInt)(_ => true).foreach { e =>
      kvstore.delete(classOf[CelebornShuffleAssignmentUIData], e.appShuffleId)
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    // ShuffleMapStage carries shuffleDepId = the Spark shuffle dependency id; result stages
    // have None. Used in onTaskEnd to attribute write metrics to a shuffle.
    stageSubmitted.stageInfo.shuffleDepId.foreach { sid =>
      stageToShuffleMappings.put(stageSubmitted.stageInfo.stageId, sid.asInstanceOf[Int])
    }
    mayUpdate(false)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // Capture celeborn-related SparkConf properties for the UI (mirrors Uniffle's
    // onJobStart capturing spark.rss.*). Pure driver-side, no executor RPC.
    val props = conf.getAll.filter(_._1.startsWith("spark.celeborn.")).toSeq.sortBy(_._1)
    kvstore.write(new CelebornPropertiesUIData(props))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    mayUpdate(false)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val tm = taskEnd.taskMetrics
    if (tm != null) {
      val write = tm.shuffleWriteMetrics
      val read = tm.shuffleReadMetrics
      // Write: per-shuffle (ShuffleMapStage only) + global. Read: global only (result stage
      // has no shuffleDepId, so per-shuffle attribution is unreliable per plan A).
      totalWriteBytes.addAndGet(write.bytesWritten)
      totalWriteTimeMs.addAndGet(write.writeTime / 1000000)
      totalReadBytes.addAndGet(read.remoteBytesRead + read.localBytesRead)
      totalFetchWaitTimeMs.addAndGet(read.fetchWaitTime)
      if (taskEnd.taskInfo != null) {
        totalTaskCpuTimeMs.addAndGet(taskEnd.taskInfo.duration)
      }
      // ShuffleMapTask carries shuffle write; ShuffleMapStage has a shuffleDepId mapped above.
      if (stageToShuffleMappings.containsKey(taskEnd.stageId)
        && taskEnd.taskType == "ShuffleMapTask") {
        val shuffleId = stageToShuffleMappings.get(taskEnd.stageId)
        val metric = perShuffleWrite.computeIfAbsent(
          shuffleId,
          _ => new AggregatedShuffleWriteMetric(0L, 0L, 0L))
        metric.bytesWritten += write.bytesWritten
        metric.recordsWritten += write.recordsWritten
        metric.writeTimeMs += write.writeTime / 1000000
      }
    }
    mayUpdate(false)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: CelebornBuildInfoEvent =>
      kvstore.write(new CelebornBuildInfoUIData(e.info.toSeq.sortBy(_._1)))
      mayUpdate(false)
    case e: CelebornShuffleAssignmentEvent =>
      kvstore.write(new CelebornShuffleAssignmentUIData(
        e.appShuffleId,
        e.celebornShuffleId,
        e.workers,
        e.numPartitions,
        e.timestamp))
      mayUpdate(false)
    case e: CelebornFallbackEvent =>
      kvstore.write(new CelebornFallbackStatsUIData(e.fallbackCounts))
      mayUpdate(false)
    case e: CelebornReassignEvent =>
      kvstore.write(new CelebornReassignStatsUIData(
        e.partitionSplit,
        e.reviveTriggered,
        e.stageRetry,
        e.timestamp))
      mayUpdate(false)
    case e: CelebornWriteMetricsEvent =>
      import scala.collection.JavaConverters._
      val w = e.writeMetrics
      aggCopyTimeMs.addAndGet(w.copyTimeMs)
      aggSerializeTimeMs.addAndGet(w.serializeTimeMs)
      aggCompressTimeMs.addAndGet(w.compressTimeMs)
      aggQueueWaitTimeMs.addAndGet(w.queueWaitTimeMs)
      aggQueueStallTimeMs.addAndGet(w.queueStallTimeMs)
      aggInflightWaitTimeMs.addAndGet(w.inflightWaitTimeMs)
      aggDrainWaitTimeMs.addAndGet(w.drainWaitTimeMs)
      aggSlowPushCount.addAndGet(w.slowPushCount)
      aggMaxPushRttMs.accumulateAndGet(w.maxPushRttMs, math.max)
      aggUncompressedBytes.addAndGet(w.uncompressedBytes)
      // Merge per-worker push stats.
      e.pushWorkerStats.asScala.foreach { s =>
        val acc =
          perWorkerWriteStats.computeIfAbsent(s.workerId, _ => new PerWorkerWriteAccumulator)
        acc.merge(s)
      }
      mayUpdate(false)
    case e: CelebornReadMetricsEvent =>
      import scala.collection.JavaConverters._
      val rm = e.readMetrics
      aggDecompressTimeMs.addAndGet(rm.decompressTimeMs)
      aggChunkWaitTimeMs.addAndGet(rm.chunkWaitTimeMs)
      aggDeserializeTimeMs.addAndGet(rm.deserializeTimeMs)
      aggCopyTimeMsRead.addAndGet(rm.copyTimeMs)
      aggRetryCount.addAndGet(rm.retryCount)
      aggRetryWaitTimeMs.addAndGet(rm.retryWaitTimeMs)
      aggPeerSwitchCount.addAndGet(rm.peerSwitchCount)
      aggExcludeCount.addAndGet(rm.excludeCount)
      aggSlowChunkCount.addAndGet(rm.slowChunkCount)
      aggMaxChunkRttMs.accumulateAndGet(rm.maxChunkRttMs, math.max)
      e.workerReadCosts.asScala.foreach { w =>
        val acc = perWorkerReadStats.computeIfAbsent(w.workerId, _ => new PerWorkerReadAccumulator)
        acc.merge(w)
      }
      mayUpdate(false)
    case _ => // ignore unknown events
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    // Force a final flush so the live UI's in-memory aggregations are persisted before the
    // application context tears down.
    mayUpdate(true)
  }

  /** Throttle KVStore writes: at most one flush per updateIntervalMillis unless force. */
  private def mayUpdate(force: Boolean): Unit = {
    val now = System.nanoTime()
    if (force || now - lastUpdateNanos > TimeUnit.MILLISECONDS.toNanos(updateIntervalMillis)) {
      lastUpdateNanos = now
      flushAggregations()
    }
  }

  /** Snapshot the in-memory aggregations into the KVStore. */
  private def flushAggregations(): Unit = {
    import scala.collection.JavaConverters._
    val writeSnapshot = new java.util.HashMap[Int, AggregatedShuffleWriteMetric]()
    perShuffleWrite.asScala.foreach { case (k, v) =>
      writeSnapshot.put(
        k,
        new AggregatedShuffleWriteMetric(v.bytesWritten, v.recordsWritten, v.writeTimeMs))
    }
    kvstore.write(new CelebornAggregatedWriteMetricsUIData(writeSnapshot))
    kvstore.write(new CelebornAggregatedTaskInfoUIData(
      totalWriteBytes.get(),
      totalWriteTimeMs.get(),
      totalReadBytes.get(),
      totalFetchWaitTimeMs.get(),
      totalTaskCpuTimeMs.get()))
    // Write-path timing breakdown (ms) + per-worker push stats, from CelebornWriteMetricsEvent.
    kvstore.write(new CelebornWriteTimesUIData(
      aggCopyTimeMs.get(),
      aggSerializeTimeMs.get(),
      aggCompressTimeMs.get(),
      aggQueueWaitTimeMs.get(),
      aggQueueStallTimeMs.get(),
      aggInflightWaitTimeMs.get(),
      aggDrainWaitTimeMs.get(),
      aggSlowPushCount.get(),
      aggMaxPushRttMs.get(),
      aggUncompressedBytes.get()))
    perWorkerWriteStats.asScala.foreach { case (workerId, acc) =>
      kvstore.write(new CelebornPerWorkerWriteStatsUIData(
        workerId,
        acc.pushCount.get(),
        acc.pushBytes.get(),
        acc.totalPushRttNanos.get(),
        acc.softSplitCount.get(),
        acc.hardSplitCount.get(),
        acc.primaryCongestedCount.get(),
        acc.replicaCongestedCount.get(),
        acc.lastPushFailureReason))
    }
    // Read-path timing breakdown (ms) + per-worker read stats, from CelebornReadMetricsEvent.
    kvstore.write(new CelebornReadTimesUIData(
      aggDecompressTimeMs.get(),
      aggChunkWaitTimeMs.get(),
      aggDeserializeTimeMs.get(),
      aggCopyTimeMsRead.get(),
      aggRetryCount.get(),
      aggRetryWaitTimeMs.get(),
      aggPeerSwitchCount.get(),
      aggExcludeCount.get(),
      aggSlowChunkCount.get(),
      aggMaxChunkRttMs.get()))
    perWorkerReadStats.asScala.foreach { case (workerId, acc) =>
      kvstore.write(new CelebornPerWorkerReadStatsUIData(
        workerId,
        acc.chunkCount.get(),
        acc.bytes.get(),
        acc.totalRttNanos.get(),
        acc.maxRttNanos.get()))
    }
  }
}

private[ui] class PerWorkerWriteAccumulator {
  private[ui] val pushCount = new java.util.concurrent.atomic.AtomicLong(0)
  private[ui] val pushBytes = new java.util.concurrent.atomic.AtomicLong(0)
  private[ui] val totalPushRttNanos = new java.util.concurrent.atomic.AtomicLong(0)
  private[ui] val softSplitCount = new java.util.concurrent.atomic.AtomicLong(0)
  private[ui] val hardSplitCount = new java.util.concurrent.atomic.AtomicLong(0)
  private[ui] val primaryCongestedCount = new java.util.concurrent.atomic.AtomicLong(0)
  private[ui] val replicaCongestedCount = new java.util.concurrent.atomic.AtomicLong(0)
  @volatile private[ui] var lastPushFailureReason: String = ""

  def merge(s: org.apache.celeborn.common.protocol.message.PushWorkerStats): Unit = {
    pushCount.addAndGet(s.pushCount)
    pushBytes.addAndGet(s.pushBytes)
    totalPushRttNanos.addAndGet(s.totalPushRttNanos)
    softSplitCount.addAndGet(s.softSplitCount)
    hardSplitCount.addAndGet(s.hardSplitCount)
    primaryCongestedCount.addAndGet(s.primaryCongestedCount)
    replicaCongestedCount.addAndGet(s.replicaCongestedCount)
    if (s.lastPushFailureReason != null && s.lastPushFailureReason.nonEmpty) {
      lastPushFailureReason = s.lastPushFailureReason
    }
  }
}

object CelebornListener extends Logging {

  /** Register a listener to the status queue so its events enter the event log. */
  def register(sc: SparkContext): Unit = {
    val kvstore = sc.statusStore.store.asInstanceOf[ElementTrackingStore]
    sc.listenerBus.addToStatusQueue(new CelebornListener(sc.conf, kvstore))
  }
}

private[ui] class PerWorkerReadAccumulator {
  private[ui] val chunkCount = new java.util.concurrent.atomic.AtomicLong(0)
  private[ui] val bytes = new java.util.concurrent.atomic.AtomicLong(0)
  private[ui] val totalRttNanos = new java.util.concurrent.atomic.AtomicLong(0)
  private[ui] val maxRttNanos = new java.util.concurrent.atomic.AtomicLong(0)

  def merge(w: org.apache.celeborn.common.protocol.message.WorkerReadCost): Unit = {
    chunkCount.addAndGet(w.chunkCount)
    bytes.addAndGet(w.bytes)
    totalRttNanos.addAndGet(w.totalRttNanos)
    maxRttNanos.accumulateAndGet(w.maxRttNanos, math.max)
  }
}
