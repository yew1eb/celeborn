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

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.spark.shuffle.celeborn.SparkShuffleManager
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

import org.apache.celeborn.common.CelebornConf

/**
 * A [[SparkListener]] that proactively unregisters shuffles instead of waiting for the driver GC
 * to trigger Spark's ContextCleaner or for the whole application to end. It supports two modes,
 * controlled by `celeborn.client.spark.shuffleCleanup.*` configurations:
 *
 *   1. Query-end cleanup (always on when the listener is registered): all shuffles written by a
 *      SQL query (or Dataset action) are unregistered when the query completes.
 *   2. Stage-level cleanup (opt-in via `celeborn.client.spark.shuffleCleanup.stageLevel.enabled`):
 *      a shuffle is unregistered once its last reader stage has completed and the configured
 *      delay has elapsed, so shuffles of long-running queries do not pile up until the query
 *      ends. This is a port of Apache Uniffle's eager shuffle deletion (apache/uniffle#2704).
 *
 * Note: this class lives in package `org.apache.spark.sql` because
 * [[SparkListenerSQLExecutionEnd.qe]] is `private[sql]`.
 */
class CelebornShuffleCleanupListener(sparkContext: SparkContext, celebornConf: CelebornConf)
  extends SparkListener
  with Logging {

  private val stageDependencyTracker: Option[CelebornStageShuffleDependencyTracker] =
    if (celebornConf.clientSparkShuffleCleanupStageLevelEnabled) {
      Some(
        CelebornStageShuffleDependencyTracker(
          celebornConf.clientSparkShuffleCleanupStageLevelDelayedMinutes,
          unregisterShuffleIfStillRegistered))
    } else {
      None
    }

  logInfo(
    s"CelebornShuffleCleanupListener created, stageLevelCleanupEnabled: " +
      s"${stageDependencyTracker.isDefined}, " +
      s"stageLevelDelayedMinutes: " +
      s"${celebornConf.clientSparkShuffleCleanupStageLevelDelayedMinutes}")

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case end: SparkListenerSQLExecutionEnd =>
        try {
          cleanupShufflesOnQueryEnd(end)
        } catch {
          case t: Throwable =>
            logWarning(
              s"Failed to cleanup shuffles on completion of SQL execution ${end.executionId}.",
              t)
        }
      case _ => // ignore other events
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    stageDependencyTracker.foreach { tracker =>
      try {
        val stageInfo = stageSubmitted.stageInfo
        // 1. if this is a shuffle map stage, mark this stage as the writer stage
        stageInfo.shuffleDepId.foreach(shuffleId =>
          tracker.linkWriter(shuffleId, stageInfo.stageId))
        // 2. if parent stages exist, mark this stage as the reader of their shuffles
        stageInfo.parentIds.foreach { parentStageId =>
          val shuffleId = tracker.getShuffleIdByStageIdOfWriter(parentStageId)
          if (shuffleId >= 0) {
            tracker.linkReader(shuffleId, stageInfo.stageId)
          }
        }
      } catch {
        case t: Throwable =>
          logWarning(
            s"Failed to track stage ${stageSubmitted.stageInfo.stageId} submission.",
            t)
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stageDependencyTracker.foreach { tracker =>
      try {
        tracker.removeStage(stageCompleted.stageInfo.stageId)
      } catch {
        case t: Throwable =>
          logWarning(s"Failed to track stage ${stageCompleted.stageInfo.stageId} completion.", t)
      }
    }
  }

  private def shuffleIdOfExchange(plan: SparkPlan): Option[Int] = {
    plan match {
      case exchange: ShuffleExchangeExec => Some(exchange.shuffleDependency.shuffleId)
      case reused: ReusedExchangeExec => shuffleIdOfExchange(reused.child)
      case _ => None
    }
  }

  private def cleanupShufflesOnQueryEnd(end: SparkListenerSQLExecutionEnd): Unit = {
    val qe = end.qe
    if (qe == null) {
      // The event can arrive with a null QueryExecution on the driver, e.g. when the SQL
      // execution has already been cleaned up. Nothing can be collected in this case.
      logWarning(
        s"QueryExecution is null in SparkListenerSQLExecutionEnd ${end.executionId}, " +
          s"skip shuffle cleanup for this execution.")
      return
    }

    val lifecycleManager = SparkEnv.get.shuffleManager match {
      case manager: SparkShuffleManager => manager.getLifecycleManager
      case _ => null
    }
    if (lifecycleManager == null) {
      logWarning(
        s"No Celeborn LifecycleManager found on driver when SQL execution ${end.executionId} " +
          s"ended, skip shuffle cleanup for this execution.")
      return
    }

    val plan = qe.executedPlan match {
      // AdaptiveSparkPlanExec is a LeafExecNode, so `collect` cannot descend into its
      // final physical plan directly.
      case adaptivePlan: AdaptiveSparkPlanExec => adaptivePlan.executedPlan
      case other: SparkPlan => other
    }
    val hasMapping = lifecycleManager.conf.clientStageRerunEnabled
    val shuffleIds = plan
      .collect {
        case exchange: ShuffleExchangeExec => shuffleIdOfExchange(exchange)
        // With AQE, exchanges are replaced by query stages in the final plan.
        case stage: ShuffleQueryStageExec => shuffleIdOfExchange(stage.plan)
      }
      .flatten
      .distinct
      .filter(lifecycleManager.isAppShuffleRegistered(_, hasMapping))

    if (shuffleIds.isEmpty) {
      logInfo(
        s"No registered shuffles found in the final plan of SQL execution ${end.executionId}, " +
          s"nothing to cleanup.")
      return
    }

    logInfo(
      s"Cleaning up shuffles [${shuffleIds.mkString(", ")}] on completion of " +
        s"SQL execution ${end.executionId}.")
    // Go through the standard Spark shuffle cleanup entry point, which broadcasts
    // RemoveShuffle to all block managers (including the driver's) and finally reaches
    // SparkShuffleManager.unregisterShuffle -> LifecycleManager.unregisterAppShuffle.
    shuffleIds.foreach(shuffleId =>
      sparkContext.shuffleDriverComponents.removeShuffle(shuffleId, false))
  }

  // Called by the stage-level dependency tracker (on its own daemon thread) once the last
  // reader stage of a shuffle has completed and the deletion delay has elapsed. Reuses the
  // standard Spark cleanup entry point, guarded by the registration check so that shuffles
  // already unregistered (e.g. by the query-end cleanup) or never registered with Celeborn
  // (e.g. sort-shuffle fallback) are not touched.
  private def unregisterShuffleIfStillRegistered(shuffleId: Int): Unit = {
    try {
      val lifecycleManager = SparkEnv.get.shuffleManager match {
        case manager: SparkShuffleManager => manager.getLifecycleManager
        case _ => null
      }
      if (lifecycleManager == null) {
        return
      }
      val hasMapping = lifecycleManager.conf.clientStageRerunEnabled
      if (lifecycleManager.isAppShuffleRegistered(shuffleId, hasMapping)) {
        logInfo(s"Eagerly cleaning up shuffle $shuffleId after its last reader stage completed.")
        sparkContext.shuffleDriverComponents.removeShuffle(shuffleId, false)
      } else {
        logDebug(
          s"Skip eager cleanup for shuffle $shuffleId as it is not registered in " +
            s"LifecycleManager (already cleaned up or sort-shuffle fallback).")
      }
    } catch {
      case t: Throwable =>
        logWarning(s"Failed to eagerly cleanup shuffle $shuffleId on stage completion.", t)
    }
  }
}
