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

package org.apache.celeborn.trogdor.agent.http

import javax.ws.rs.{Consumes, DELETE, GET, Path, PathParam, POST, Produces, PUT}
import javax.ws.rs.core.MediaType

import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.trogdor.rest.{AgentStatusResponse, CreateWorkerRequest, DestroyWorkerRequest, StopWorkerRequest, UptimeResponse}
import org.apache.celeborn.trogdor.service.TrogdorAgent

@Path("/")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class AgentResource extends ApiRequestContext {

  private def agent: TrogdorAgent = httpService.asInstanceOf[TrogdorAgent]

  @GET
  @Path("status")
  def status(): AgentStatusResponse = agent.status()

  @GET
  @Path("uptime")
  def uptime(): UptimeResponse = agent.uptime()

  @POST
  @Path("workers")
  def createWorker(request: CreateWorkerRequest): Unit = agent.createWorker(request)

  @PUT
  @Path("workers/{workerId}/stop")
  def stopWorker(@PathParam("workerId") workerId: Long): Unit = {
    agent.stopWorker(new StopWorkerRequest(workerId))
  }

  @DELETE
  @Path("workers/{workerId}")
  def destroyWorker(@PathParam("workerId") workerId: Long): Unit = {
    agent.destroyWorker(new DestroyWorkerRequest(workerId))
  }
}
