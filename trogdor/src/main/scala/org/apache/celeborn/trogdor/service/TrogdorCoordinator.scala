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

package org.apache.celeborn.trogdor.service

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.trogdor.chaos.ChaosPlanSpec
import org.apache.celeborn.trogdor.coordinator.{ChaosOrchestrator, TaskManager}
import org.apache.celeborn.trogdor.platform.Platform
import org.apache.celeborn.trogdor.rest.{ChaosPlanResponse, CoordinatorStatusResponse, CreateTaskRequest, DestroyTaskRequest, StopTaskRequest, SubmitChaosPlanRequest, TaskRequest, TasksRequest, TasksResponse, UptimeResponse}
import org.apache.celeborn.trogdor.service.metrics.TrogdorCoordinatorSource

private[celeborn] class TrogdorCoordinator(
    override val conf: CelebornConf,
    val platform: Platform)
  extends HttpService {

  override def serviceName: String = TrogdorCoordinator.SERVICE_NAME

  override val metricsSystem: MetricsSystem = MetricsSystem.createMetricsSystem(serviceName, conf)

  private val serverStartMs = System.currentTimeMillis()
  private val taskManager =
    new TaskManager(platform, ThreadLocalRandom.current().nextLong(0, Long.MaxValue / 2))
  private val chaosOrchestrator = new ChaosOrchestrator(conf, taskManager)
  metricsSystem.registerSource(new TrogdorCoordinatorSource(conf, taskManager))
  private val stopped = new AtomicBoolean(false)

  def port(): Int = {
    val url = connectionUrl
    val idx = url.lastIndexOf(':')
    if (idx >= 0) url.substring(idx + 1).toInt else -1
  }

  def status(): CoordinatorStatusResponse = {
    new CoordinatorStatusResponse(serverStartMs)
  }

  def uptime(): UptimeResponse = {
    val now = System.currentTimeMillis()
    new UptimeResponse(serverStartMs, now)
  }

  def createTask(request: CreateTaskRequest): Unit = {
    request.spec() match {
      case planSpec: ChaosPlanSpec =>
        submitChaosPlan(
          new SubmitChaosPlanRequest(
            request.id(),
            planSpec.planJson(),
            planSpec.targetNodes()))
      case _ =>
        taskManager.createTask(request.id(), request.spec())
    }
  }

  def stopTask(request: StopTaskRequest): Unit = {
    taskManager.stopTask(request.id())
  }

  def destroyTask(request: DestroyTaskRequest): Unit = {
    taskManager.destroyTask(request.id())
  }

  def tasks(request: TasksRequest): TasksResponse = {
    taskManager.tasks(request)
  }

  def task(request: TaskRequest): org.apache.celeborn.trogdor.rest.TaskState = {
    taskManager.task(request)
  }

  def submitChaosPlan(request: SubmitChaosPlanRequest): Unit = {
    chaosOrchestrator.submitPlan(
      request.planId(),
      request.planJson(),
      request.targetNodes().asScala.toSet)
  }

  def stopChaosPlan(planId: String): Unit = {
    chaosOrchestrator.stopPlan(planId)
  }

  def showChaosPlan(planId: String): ChaosPlanResponse = {
    new ChaosPlanResponse(planId, chaosOrchestrator.queryStatus(planId))
  }

  override def getWorkerInfo: String = ""

  override def getShuffleList: String = ""

  override def getApplicationList: String = ""

  override def stop(exitKind: Int): Unit = {
    if (stopped.compareAndSet(false, true)) {
      chaosOrchestrator.shutdown()
      taskManager.beginShutdown()
      try {
        taskManager.waitForShutdown()
      } catch {
        case e: Throwable =>
          logWarning("Error waiting for TaskManager shutdown.", e)
      }
      super.stop(exitKind)
    }
  }
}

object TrogdorCoordinator {
  val SERVICE_NAME = "trogdor-coordinator"
}
