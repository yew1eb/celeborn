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

package org.apache.celeborn.trogdor.coordinator

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{ThreadUtils, Utils}
import org.apache.celeborn.trogdor.chaos.{ChaosOperationSpec, ChaosPlanSpec}
import org.apache.celeborn.trogdor.chaos.action.{Action, BashOperation, OccupyCpuOperation}
import org.apache.celeborn.trogdor.chaos.conf.ChaosConf
import org.apache.celeborn.trogdor.chaos.info.{NodeStatus, RunnerInfo}
import org.apache.celeborn.trogdor.chaos.plan.{Parser, VerificationPlan}
import org.apache.celeborn.trogdor.chaos.scheduler.SchedulerContext
import org.apache.celeborn.trogdor.rest.{TaskDone, TaskRequest, TaskState}

/**
 * Coordinator-side orchestrator for chaos testing plans.
 *
 * It parses a verification plan and compiles it into native Trogdor tasks:
 * - One [[ChaosPlanSpec]] participant task per target node to keep the agent in the plan.
 * - One [[ChaosOperationSpec]] task per operation selected by the plan actions.
 *
 * The current implementation supports the sequence trigger. Random trigger and the resource checker
 * are left as follow-ups and can be added without changing the integration shape.
 */
class ChaosOrchestrator(conf: CelebornConf, taskManager: TaskManager) extends Logging {

  private val chaosConf = new ChaosConf(conf)
  private val executor = ThreadUtils.newDaemonSingleThreadExecutor("ChaosOrchestrator")
  private val stopFlags = new ConcurrentHashMap[String, AtomicBoolean]()
  private val runningPlans = new ConcurrentHashMap[String, RunningPlan]()

  case class RunningPlan(planId: String, targetNodes: Set[String])

  def submitPlan(planId: String, planJson: String, targetNodes: Set[String]): Unit = {
    if (runningPlans.containsKey(planId)) {
      throw new IllegalStateException(s"Chaos plan $planId is already running.")
    }
    val verificationPlan = Parser.parse(planJson, chaosConf)
    if (verificationPlan.trigger.random) {
      throw new UnsupportedOperationException(
        "Random trigger is not yet supported in the Trogdor-integrated chaos engine.")
    }

    runningPlans.put(planId, RunningPlan(planId, targetNodes))
    stopFlags.put(planId, new AtomicBoolean(false))

    // Create participant tasks on every target node. These keep the agents "in" the plan and can
    // be used later to collect per-node status for the checker. The duration is configurable so
    // tests can use a short value while production runs keep participants alive for the plan run.
    val participantDurationMs = chaosConf.planParticipantDurationMs
    targetNodes.foreach { node =>
      val participantId = s"$planId-participant-$node"
      taskManager.createTask(
        participantId,
        new ChaosPlanSpec(
          0,
          participantDurationMs,
          java.util.Collections.singleton(node),
          planJson))
    }

    executor.submit(new Runnable {
      override def run(): Unit = runPlan(planId, verificationPlan, targetNodes)
    })
  }

  def stopPlan(planId: String): Unit = {
    val flag = stopFlags.get(planId)
    if (flag != null) {
      flag.set(true)
    }
    val running = runningPlans.get(planId)
    if (running != null) {
      running.targetNodes.foreach { node =>
        Utils.tryLogNonFatalError {
          taskManager.stopTask(s"$planId-participant-$node")
        }
      }
      runningPlans.remove(planId)
      stopFlags.remove(planId)
    }
  }

  def queryStatus(planId: String): String = {
    val running = runningPlans.get(planId)
    if (running == null) {
      s"Chaos plan $planId is not running."
    } else {
      s"Chaos plan $planId is running on nodes: ${running.targetNodes.mkString(", ")}."
    }
  }

  private def runPlan(planId: String, plan: VerificationPlan, targetNodes: Set[String]): Unit = {
    try {
      val repeat = plan.trigger.repeat
      val actions = plan.actions
      val flag = stopFlags.get(planId)
      val context = buildSchedulerContext(targetNodes)

      (1 to repeat).foreach { round =>
        logInfo(s"Chaos plan $planId round $round of $repeat.")
        actions.foreach { action =>
          if (flag != null && flag.get()) {
            logInfo(s"Chaos plan $planId stopped.")
            return
          }
          executeAction(planId, action, context)
          Thread.sleep(action.interval)
        }
      }
      logInfo(s"Chaos plan $planId completed.")
    } catch {
      case NonFatal(t) =>
        logError(s"Chaos plan $planId failed.", t)
    } finally {
      stopPlan(planId)
    }
  }

  private def buildSchedulerContext(targetNodes: Set[String]): SchedulerContext = {
    val runnerInfos = new ConcurrentHashMap[String, RunnerInfo]()
    targetNodes.foreach { node =>
      val status = new NodeStatus(
        Runtime.getRuntime.availableProcessors(),
        masterAlive = true,
        workerAlive = true,
        Map.empty)
      runnerInfos.put(node, new RunnerInfo(status, System.currentTimeMillis(), null))
    }
    new SchedulerContext(chaosConf, runnerInfos)
  }

  private def executeAction(planId: String, action: Action, context: SchedulerContext): Unit = {
    logInfo(s"Chaos plan $planId executing action ${action.identity()}.")
    val operations = action.generateOperations(context)
    operations.foreach { operation =>
      val opId = s"$planId-op-${System.nanoTime()}"
      val target = operation.actionTarget.identity
      val spec = operation match {
        case bash: BashOperation =>
          new ChaosOperationSpec(
            0,
            bash.interval + TimeUnit.SECONDS.toMillis(10),
            java.util.Collections.singleton(target),
            ChaosOperationSpec.OperationType.BASH,
            bash.command,
            0,
            0)
        case cpu: OccupyCpuOperation =>
          new ChaosOperationSpec(
            0,
            Math.max(cpu.interval, cpu.duration) + TimeUnit.SECONDS.toMillis(10),
            java.util.Collections.singleton(target),
            ChaosOperationSpec.OperationType.OCCUPY_CPU,
            null,
            cpu.cores,
            cpu.duration)
        case _ =>
          throw new UnsupportedOperationException(
            s"Unsupported operation type: ${operation.getClass}")
      }
      taskManager.createTask(opId, spec)
      waitForTaskDone(opId, operation.interval)
      Thread.sleep(operation.interval)
    }
  }

  private def waitForTaskDone(taskId: String, pollIntervalMs: Long): Unit = {
    val deadline = System.currentTimeMillis() + 5 * 60 * 1000
    while (System.currentTimeMillis() < deadline) {
      val state = taskManager.task(new TaskRequest(taskId))
      if (state.state() == TaskState.State.DONE) {
        if (state.isInstanceOf[TaskDone]) {
          val done = state.asInstanceOf[TaskDone]
          if (done.error() != null && done.error().nonEmpty) {
            logWarning(s"Chaos operation task $taskId finished with error: ${done.error()}")
          }
        }
        return
      }
      Thread.sleep(Math.max(100, Math.min(pollIntervalMs, 1000)))
    }
    logWarning(s"Chaos operation task $taskId did not finish within timeout, giving up wait.")
  }

  def shutdown(): Unit = {
    runningPlans.keys().asScala.foreach(stopPlan)
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.MINUTES)
  }
}
