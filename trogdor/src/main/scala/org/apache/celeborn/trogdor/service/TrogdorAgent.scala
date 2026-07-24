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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.server.common.{HttpService, Service}
import org.apache.celeborn.trogdor.agent.WorkerManager
import org.apache.celeborn.trogdor.platform.Platform
import org.apache.celeborn.trogdor.rest.{AgentStatusResponse, CreateWorkerRequest, DestroyWorkerRequest, StopWorkerRequest, UptimeResponse}
import org.apache.celeborn.trogdor.service.metrics.TrogdorAgentSource

private[celeborn] class TrogdorAgent(
    override val conf: CelebornConf,
    val platform: Platform)
  extends HttpService {

  override def serviceName: String = TrogdorAgent.SERVICE_NAME

  override val metricsSystem: MetricsSystem = MetricsSystem.createMetricsSystem(serviceName, conf)

  private val serverStartMs = System.currentTimeMillis()
  private val workerManager = new WorkerManager(platform)
  metricsSystem.registerSource(new TrogdorAgentSource(conf, workerManager))
  private val stopped = new AtomicBoolean(false)

  def port(): Int = {
    // HttpServer's connector local port is available via connectionUrl.
    // Parse host:port string.
    val url = connectionUrl
    val idx = url.lastIndexOf(':')
    if (idx >= 0) url.substring(idx + 1).toInt else -1
  }

  def status(): AgentStatusResponse = {
    new AgentStatusResponse(serverStartMs, workerManager.workerStates())
  }

  def uptime(): UptimeResponse = {
    val now = System.currentTimeMillis()
    new UptimeResponse(serverStartMs, now)
  }

  def createWorker(req: CreateWorkerRequest): Unit = {
    workerManager.createWorker(req.workerId(), req.taskId(), req.spec())
  }

  def stopWorker(req: StopWorkerRequest): Unit = {
    workerManager.stopWorker(req.workerId(), false)
  }

  def destroyWorker(req: DestroyWorkerRequest): Unit = {
    workerManager.stopWorker(req.workerId(), true)
  }

  override def getWorkerInfo: String = ""

  override def getShuffleList: String = ""

  override def getApplicationList: String = ""

  override def stop(exitKind: Int): Unit = {
    if (stopped.compareAndSet(false, true)) {
      workerManager.beginShutdown()
      try {
        workerManager.waitForShutdown()
      } catch {
        case e: Throwable =>
          logWarning("Error waiting for WorkerManager shutdown.", e)
      }
      super.stop(exitKind)
    }
  }
}

object TrogdorAgent {
  val SERVICE_NAME = "trogdor-agent"
}
