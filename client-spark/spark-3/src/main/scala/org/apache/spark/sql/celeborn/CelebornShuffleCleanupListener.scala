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
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

import org.apache.celeborn.common.CelebornConf

/**
 * A [[SparkListener]] that proactively unregisters shuffles instead of waiting for driver GC
 * or application end. Query-end cleanup (unregisters all shuffles of a finished SQL query)
 * is always on once the listener is registered; stage-level cleanup (a port of Apache
 * Uniffle's eager shuffle deletion, apache/uniffle#2704) is opt-in via
 * `celeborn.client.spark.shuffleCleanup.stageLevel.enabled`.
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
        stageInfo.shuffleDepId.foreach(shuffleId =>
          tracker.linkWriter(shuffleId, stageInfo.stageId))
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

  // Same approach as Spark's SQLExecution.extractShuffleIds: unwrap the AdaptiveSparkPlanExec
  // once (collectFirst also reaches one nested under a DataWritingCommandExec of CTAS/INSERT),
  // where every exchange shows up as a shuffle query stage in the final plan.
  private def extractShuffleIds(plan: SparkPlan): Seq[Int] = {
    val shuffleIdOf: PartialFunction[SparkPlan, Int] = {
      case exchange: ShuffleExchangeExec => exchange.shuffleDependency.shuffleId
    }
    plan.collectFirst { case adaptivePlan: AdaptiveSparkPlanExec => adaptivePlan } match {
      case Some(adaptivePlan) =>
        adaptivePlan.executedPlan
          .collect { case stage: ShuffleQueryStageExec => stage.plan }
          .collect(shuffleIdOf)
      case None =>
        plan.collect(shuffleIdOf)
    }
  }

  private def cleanupShufflesOnQueryEnd(end: SparkListenerSQLExecutionEnd): Unit = {
    val qe = end.qe
    if (qe == null) {
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

    val hasMapping = lifecycleManager.conf.clientStageRerunEnabled
    val allShuffleIds = extractShuffleIds(qe.executedPlan)
    val shuffleIds = allShuffleIds.filter(lifecycleManager.isAppShuffleRegistered(_, hasMapping))

    if (shuffleIds.isEmpty) {
      // Empty allShuffleIds: the plan has no exchange; otherwise the shuffles were never
      // registered with Celeborn (e.g. sort-shuffle fallback) or already unregistered.
      logInfo(
        s"Found shuffle ids [${allShuffleIds.mkString(", ")}] in the final plan of SQL " +
          s"execution ${end.executionId}, but none is registered in Celeborn LifecycleManager " +
          s"(stageRerunEnabled: $hasMapping, registeredShuffles: " +
          s"${lifecycleManager.registeredShuffle.size()}, shuffleIdMappings: " +
          s"${lifecycleManager.getShuffleIdMapping.size()}), nothing to cleanup.")
      return
    }

    logInfo(
      s"Cleaning up shuffles [${shuffleIds.mkString(", ")}] on completion of " +
        s"SQL execution ${end.executionId}.")
    // Standard Spark shuffle cleanup entry point: RemoveShuffle reaches the driver's
    // SparkShuffleManager.unregisterShuffle -> LifecycleManager.unregisterAppShuffle.
    shuffleIds.foreach(shuffleId =>
      sparkContext.shuffleDriverComponents.removeShuffle(shuffleId, false))
  }

  // Invoked by the stage-level tracker (on its daemon thread) after the deletion delay, guarded
  // by the registration check so shuffles already unregistered or never registered are skipped.
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
