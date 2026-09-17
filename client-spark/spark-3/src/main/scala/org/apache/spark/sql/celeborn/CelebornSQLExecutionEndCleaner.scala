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

/**
 * A [[SparkListener]] that proactively unregisters shuffles written by a SQL query (or a
 * Dataset action) when the query completes, instead of waiting for the driver GC to trigger
 * Spark's ContextCleaner or for the whole application to end.
 *
 * This is safe at query granularity because when a query completes, all of its stages
 * (including AQE-created ones) have finished and all reads of its shuffles are done. Exchange
 * reuse only happens within a single query execution, and each new SQL statement allocates
 * fresh shuffle ids, so there is no "too-early deletion" window.
 *
 * Note: this class lives in package `org.apache.spark.sql` because
 * [[SparkListenerSQLExecutionEnd.qe]] is `private[sql]`.
 */
class CelebornSQLExecutionEndCleaner extends SparkListener with Logging {

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
      logDebug(
        s"QueryExecution is null in SparkListenerSQLExecutionEnd ${end.executionId}, skipping.")
      return
    }

    val lifecycleManager = SparkEnv.get.shuffleManager match {
      case manager: SparkShuffleManager => manager.getLifecycleManager
      case _ => null
    }
    if (lifecycleManager == null) {
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
      return
    }

    logInfo(
      s"Cleaning up shuffles [${shuffleIds.mkString(", ")}] on completion of " +
        s"SQL execution ${end.executionId}.")
    // Go through the standard Spark shuffle cleanup entry point, which broadcasts
    // RemoveShuffle to all block managers (including the driver's) and finally reaches
    // SparkShuffleManager.unregisterShuffle -> LifecycleManager.unregisterAppShuffle.
    // Note that SparkContext.getActive cannot be used here because listener events are
    // delivered on the listener bus thread, which has no active SparkContext.
    val sc = qe.sparkSession.sparkContext
    shuffleIds.foreach(shuffleId =>
      sc.shuffleDriverComponents.removeShuffle(shuffleId, false))
  }
}
