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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.shuffle.celeborn.SparkShuffleManager
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

class CelebornQueryEndShuffleCleaner extends SparkListener with Logging {

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
      case _ =>
    }
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
    val qe = end.qe
    if (qe == null) {
      logDebug(
        s"QueryExecution is null in SparkListenerSQLExecutionEnd ${end.executionId}, skipping.")
      return
    }

    val lifecycleManager =
      SparkEnv.get.shuffleManager.asInstanceOf[SparkShuffleManager].getLifecycleManager
    val shuffleIds = extractShuffleIds(qe.executedPlan)
      .filter(
        lifecycleManager.isAppShuffleRegistered(_, lifecycleManager.conf.clientStageRerunEnabled))

    if (shuffleIds.nonEmpty) {
      logInfo(
        s"Cleaning up shuffles [${shuffleIds.mkString(", ")}] on completion of " +
          s"SQL execution ${end.executionId}.")
      shuffleIds.foreach(shuffleId =>
        qe.sparkSession.sparkContext.shuffleDriverComponents.removeShuffle(shuffleId, false))
    }
  }
}
