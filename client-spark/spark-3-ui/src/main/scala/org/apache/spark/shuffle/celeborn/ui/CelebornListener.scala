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
import org.apache.spark.shuffle.celeborn.events.{CelebornBuildInfoEvent, CelebornFallbackEvent, CelebornShuffleAssignmentEvent}
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

  import CelebornListener._

  private val updateIntervalMillis = 5000L
  @volatile private var lastUpdateNanos: Long = -1L

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    // ShuffleMapStage carries shuffleDepId = the Spark shuffle dependency id; result stages
    // have None. Used in onTaskEnd to attribute write metrics to a shuffle.
    stageSubmitted.stageInfo.shuffleDepId.foreach { sid =>
      stageToShuffleMappings.put(stageSubmitted.stageInfo.stageId, sid.asInstanceOf[Int])
    }
    mayUpdate(false)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    mayUpdate(false)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    mayUpdate(false)
    // Per-shuffle write + global read/write metric aggregation is filled in step 5.
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: CelebornBuildInfoEvent =>
      kvstore.write(new CelebornBuildInfoUIData(e.info.toSeq.sortBy(_._1)))
      mayUpdate(true)
    case _: CelebornShuffleAssignmentEvent =>
      // persisted in step 3
      mayUpdate(true)
    case _: CelebornFallbackEvent =>
      // persisted in step 4
      mayUpdate(true)
    case _ => // ignore unknown events
  }

  /** Throttle KVStore writes: at most one flush per updateIntervalMillis unless force. */
  private def mayUpdate(force: Boolean): Unit = {
    val now = System.nanoTime()
    if (force || now - lastUpdateNanos > TimeUnit.MILLISECONDS.toNanos(updateIntervalMillis)) {
      lastUpdateNanos = now
      // aggregated metric entities (step 5) are written here
    }
  }
}

object CelebornListener extends Logging {
  // stageId -> shuffleDepId (only ShuffleMapStage has a shuffleDepId)
  private[ui] val stageToShuffleMappings =
    new java.util.concurrent.ConcurrentHashMap[Int, Int]()

  /** Register a listener to the status queue so its events enter the event log. */
  def register(sc: SparkContext): Unit = {
    val kvstore = sc.statusStore.store.asInstanceOf[ElementTrackingStore]
    sc.listenerBus.addToStatusQueue(new CelebornListener(sc.conf, kvstore))
  }
}
