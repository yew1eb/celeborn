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

package org.apache.celeborn.trogdor.coordinator.http

import javax.ws.rs.{Consumes, DELETE, GET, Path, PathParam, POST, Produces, PUT}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.trogdor.rest.{ChaosPlanResponse, CoordinatorStatusResponse, CreateTaskRequest, DestroyTaskRequest, StopTaskRequest, SubmitChaosPlanRequest, TaskRequest, TasksRequest, TasksResponse, TaskState, UptimeResponse}
import org.apache.celeborn.trogdor.service.TrogdorCoordinator

@Path("/")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class CoordinatorResource extends ApiRequestContext {

  private def coordinator: TrogdorCoordinator = httpService.asInstanceOf[TrogdorCoordinator]

  @GET
  @Path("status")
  def status(): CoordinatorStatusResponse = coordinator.status()

  @GET
  @Path("uptime")
  def uptime(): UptimeResponse = coordinator.uptime()

  @POST
  @Path("tasks")
  def createTask(request: CreateTaskRequest): Unit = coordinator.createTask(request)

  @PUT
  @Path("tasks/{taskId}/stop")
  def stopTask(@PathParam("taskId") taskId: String): Unit = {
    coordinator.stopTask(new StopTaskRequest(taskId))
  }

  @DELETE
  @Path("tasks/{taskId}")
  def destroyTask(@PathParam("taskId") taskId: String): Unit = {
    coordinator.destroyTask(new DestroyTaskRequest(taskId))
  }

  @GET
  @Path("tasks")
  def tasks(): TasksResponse = coordinator.tasks(new TasksRequest(Set.empty.asJava))

  @GET
  @Path("tasks/{taskId}")
  def task(@PathParam("taskId") taskId: String): TaskState = {
    coordinator.task(new TaskRequest(taskId))
  }

  @POST
  @Path("chaos/plans")
  def submitChaosPlan(request: SubmitChaosPlanRequest): Unit = {
    coordinator.submitChaosPlan(request)
  }

  @PUT
  @Path("chaos/plans/{planId}/stop")
  def stopChaosPlan(@PathParam("planId") planId: String): Unit = {
    coordinator.stopChaosPlan(planId)
  }

  @GET
  @Path("chaos/plans/{planId}")
  def showChaosPlan(@PathParam("planId") planId: String): ChaosPlanResponse = {
    coordinator.showChaosPlan(planId)
  }
}
