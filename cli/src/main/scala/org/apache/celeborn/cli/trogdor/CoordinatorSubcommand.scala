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

package org.apache.celeborn.cli.trogdor

import java.io.File

import scala.collection.JavaConverters._

import picocli.CommandLine.{Command, Option}

import org.apache.celeborn.cli.common.BaseCommand
import org.apache.celeborn.trogdor.client.CoordinatorClient
import org.apache.celeborn.trogdor.rest.{CreateTaskRequest, DestroyTaskRequest, JsonUtil, StopTaskRequest, SubmitChaosPlanRequest, TaskRequest}

@Command(
  name = "coordinator",
  description = Array("Trogdor coordinator operations"),
  subcommands = Array(classOf[AgentSubcommand]))
class CoordinatorSubcommand extends BaseCommand {

  @Option(
    names = Array("-t", "--target"),
    description = Array("Coordinator host:port, e.g. localhost:19091"),
    required = true)
  private var target: String = _

  @Option(
    names = Array("--status"),
    description = Array("Show coordinator status"))
  private var status: Boolean = false

  @Option(
    names = Array("--uptime"),
    description = Array("Show coordinator uptime"))
  private var uptime: Boolean = false

  @Option(
    names = Array("--create-task"),
    description = Array("Create a task from a JSON spec file"))
  private var createTask: String = _

  @Option(
    names = Array("-i", "--task-id"),
    description = Array("Task id"))
  private var taskId: String = _

  @Option(
    names = Array("--show-task"),
    description = Array("Show a single task"))
  private var showTask: Boolean = false

  @Option(
    names = Array("--show-tasks"),
    description = Array("Show all tasks"))
  private var showTasks: Boolean = false

  @Option(
    names = Array("--stop-task"),
    description = Array("Stop a task"))
  private var stopTask: Boolean = false

  @Option(
    names = Array("--destroy-task"),
    description = Array("Destroy a task"))
  private var destroyTask: Boolean = false

  @Option(
    names = Array("--submit-chaos-plan"),
    description = Array("Submit a chaos plan from a JSON file"))
  private var submitChaosPlan: String = _

  @Option(
    names = Array("--show-chaos-plan"),
    description = Array("Show the status of a chaos plan"))
  private var showChaosPlan: Boolean = false

  @Option(
    names = Array("--stop-chaos-plan"),
    description = Array("Stop a chaos plan"))
  private var stopChaosPlan: Boolean = false

  override def run(): Unit = {
    val (host, port) = parseTarget(target)
    val client = new CoordinatorClient(host, port)
    try {
      if (status) logInfo(client.status().toString)
      if (uptime) logInfo(client.uptime().toString)
      if (createTask != null) {
        require(taskId != null, "--task-id is required when creating a task")
        val spec = JsonUtil.JSON_SERDE.readValue(
          new File(createTask),
          classOf[org.apache.celeborn.trogdor.task.TaskSpec])
        client.createTask(new CreateTaskRequest(taskId, spec))
        logInfo(s"Created task $taskId")
      }
      if (showTask) {
        require(taskId != null, "--task-id is required")
        logInfo(client.task(new TaskRequest(taskId)).toString)
      }
      if (showTasks) {
        logInfo(client.tasks().tasks().asScala.toMap.toString)
      }
      if (stopTask) {
        require(taskId != null, "--task-id is required")
        client.stopTask(new StopTaskRequest(taskId))
        logInfo(s"Stopped task $taskId")
      }
      if (destroyTask) {
        require(taskId != null, "--task-id is required")
        client.destroyTask(new DestroyTaskRequest(taskId))
        logInfo(s"Destroyed task $taskId")
      }
      if (submitChaosPlan != null) {
        val req = JsonUtil.JSON_SERDE.readValue(
          new File(submitChaosPlan),
          classOf[SubmitChaosPlanRequest])
        client.submitChaosPlan(req)
        logInfo(s"Submitted chaos plan ${req.planId()}")
      }
      if (showChaosPlan) {
        require(taskId != null, "--task-id is required")
        logInfo(client.showChaosPlan(taskId).toString)
      }
      if (stopChaosPlan) {
        require(taskId != null, "--task-id is required")
        client.stopChaosPlan(taskId)
        logInfo(s"Stopped chaos plan $taskId")
      }
    } finally {
      client.close()
    }
  }

  private def parseTarget(target: String): (String, Int) = {
    val idx = target.lastIndexOf(':')
    require(idx > 0, s"Invalid target: $target")
    (target.substring(0, idx), target.substring(idx + 1).toInt)
  }
}
