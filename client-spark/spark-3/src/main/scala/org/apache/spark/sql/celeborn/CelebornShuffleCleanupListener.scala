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

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.CelebornConf

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

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case end: SparkListenerSQLExecutionEnd =>
        safe(s"cleanup shuffles on completion of SQL execution ${end.executionId}") {
          cleanupShufflesOnQueryEnd(end)
        }
      case _ =>
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    stageDependencyTracker.foreach { tracker =>
      val stageInfo = stageSubmitted.stageInfo
      safe(s"track stage ${stageInfo.stageId} submission") {
        stageInfo.shuffleDepId.foreach(shuffleId =>
          tracker.linkWriter(shuffleId, stageInfo.stageId))
        stageInfo.parentIds.foreach { parentStageId =>
          val shuffleId = tracker.getShuffleIdByStageIdOfWriter(parentStageId)
          if (shuffleId >= 0) {
            tracker.linkReader(shuffleId, stageInfo.stageId)
          }
        }
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stageDependencyTracker.foreach { tracker =>
      safe(s"track stage ${stageCompleted.stageInfo.stageId} completion") {
        tracker.removeStage(stageCompleted.stageInfo.stageId)
      }
    }
  }

  private def safe(what: => String)(body: => Unit): Unit = {
    try {
      body
    } catch {
      case t: Throwable => logWarning(s"Failed to $what.", t)
    }
  }

  private def lifecycleManager: Option[LifecycleManager] = {
    Option(SparkEnv.get).flatMap { env =>
      env.shuffleManager match {
        case manager: SparkShuffleManager => Option(manager.getLifecycleManager)
        case _ => None
      }
    }
  }

  private def unregisterShuffles(shuffleIds: Seq[Int]): Unit = {
    shuffleIds.foreach(shuffleId =>
      sparkContext.shuffleDriverComponents.removeShuffle(shuffleId, false))
  }

  private def extractShuffleIds(plan: SparkPlan): Seq[Int] = {
    plan.collect {
      case adaptivePlan: AdaptiveSparkPlanExec => extractShuffleIds(adaptivePlan.executedPlan)
      case stage: ShuffleQueryStageExec => extractShuffleIds(stage.plan)
      case reused: ReusedExchangeExec => extractShuffleIds(reused.child)
      case exchange: ShuffleExchangeExec => Seq(exchange.shuffleDependency.shuffleId)
    }.flatten.distinct
  }

  private def cleanupShufflesOnQueryEnd(end: SparkListenerSQLExecutionEnd): Unit = {
    lifecycleManager.foreach { lifecycleManager =>
      val shuffleIds = extractShuffleIds(end.qe.executedPlan).filter(
        lifecycleManager.isAppShuffleRegistered(_, lifecycleManager.conf.clientStageRerunEnabled))
      if (shuffleIds.nonEmpty) {
        logInfo(
          s"Cleaning up shuffles [${shuffleIds.mkString(", ")}] on completion of " +
            s"SQL execution ${end.executionId}.")
        unregisterShuffles(shuffleIds)
      }
    }
  }

  private def unregisterShuffleIfStillRegistered(shuffleId: Int): Unit = {
    safe(s"eagerly cleanup shuffle $shuffleId on stage completion") {
      lifecycleManager.foreach { lifecycleManager =>
        if (lifecycleManager.isAppShuffleRegistered(
            shuffleId,
            lifecycleManager.conf.clientStageRerunEnabled)) {
          logInfo(s"Eagerly cleaning up shuffle $shuffleId after its last reader stage completed.")
          unregisterShuffles(Seq(shuffleId))
        } else {
          logDebug(s"Skip eager cleanup for shuffle $shuffleId as it is not registered.")
        }
      }
    }
  }
}
