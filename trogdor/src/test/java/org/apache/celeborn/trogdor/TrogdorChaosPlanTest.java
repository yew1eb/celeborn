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

package org.apache.celeborn.trogdor;

import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.trogdor.platform.BasicNode;
import org.apache.celeborn.trogdor.platform.BasicPlatform;
import org.apache.celeborn.trogdor.rest.ChaosPlanResponse;
import org.apache.celeborn.trogdor.rest.CreateTaskRequest;
import org.apache.celeborn.trogdor.rest.SubmitChaosPlanRequest;
import org.apache.celeborn.trogdor.rest.TaskDone;
import org.apache.celeborn.trogdor.rest.TaskRequest;
import org.apache.celeborn.trogdor.rest.TaskState;
import org.apache.celeborn.trogdor.rest.TasksRequest;
import org.apache.celeborn.trogdor.rest.TasksResponse;
import org.apache.celeborn.trogdor.service.TrogdorAgent;
import org.apache.celeborn.trogdor.service.TrogdorCoordinator;

public class TrogdorChaosPlanTest {
  private static final String NODE_NAME = "node0";

  private int agentPort;
  private int coordinatorPort;
  private CelebornConf conf;
  private TrogdorAgent agent;
  private TrogdorCoordinator coordinator;

  @Before
  public void setUp() throws Exception {
    agentPort = findFreePort();
    coordinatorPort = findFreePort();

    conf = new CelebornConf();
    conf.set("celeborn.trogdor.agent.http.port", String.valueOf(agentPort));
    conf.set("celeborn.trogdor.coordinator.http.port", String.valueOf(coordinatorPort));
    conf.set("celeborn.trogdor.chaos.plan.participant.duration", "5s");

    Map<String, String> nodeConfig = new HashMap<>();
    nodeConfig.put("trogdor.agent.port", String.valueOf(agentPort));
    BasicNode node = new BasicNode(NODE_NAME, "localhost", nodeConfig);
    BasicPlatform platform = new BasicPlatform(Collections.singletonList(node), NODE_NAME);

    agent = new TrogdorAgent(conf, platform);
    agent.initialize();

    coordinator = new TrogdorCoordinator(conf, platform);
    coordinator.initialize();

    Thread.sleep(1000);
  }

  @After
  public void tearDown() {
    if (coordinator != null) {
      coordinator.stop(0);
    }
    if (agent != null) {
      agent.stop(0);
    }
  }

  @Test
  public void testSubmitChaosPlanViaTaskApi() throws Exception {
    String planId = "chaos-plan-1";
    String planJson =
        "{"
            + "\"actions\":[{"
            + "\"id\":\"occupy-cpu\","
            + "\"selector\":{\"type\":\"assign\",\"indices\":[0],\"interval\":\"100ms\"},"
            + "\"cores\":1,"
            + "\"duration\":\"100ms\""
            + "}],"
            + "\"trigger\":{\"policy\":\"sequence\",\"repeat\":1,\"interval\":{\"type\":\"fix\",\"value\":\"100ms\"}},"
            + "\"checker\":\"dummy\""
            + "}";

    coordinator.createTask(
        new CreateTaskRequest(
            planId,
            new org.apache.celeborn.trogdor.chaos.ChaosPlanSpec(
                0, 0, Collections.singleton(NODE_NAME), planJson)));

    // Wait for the orchestrator to schedule the operation task and for it to complete.
    boolean foundOperation = false;
    for (int i = 0; i < 60; i++) {
      TasksResponse tasks = coordinator.tasks(new TasksRequest(Collections.emptySet()));
      for (Map.Entry<String, TaskState> entry : tasks.tasks().entrySet()) {
        if (entry.getKey().startsWith(planId + "-op-")
            && entry.getValue().state() == TaskState.State.DONE) {
          foundOperation = true;
          break;
        }
      }
      if (foundOperation) {
        break;
      }
      Thread.sleep(500);
    }
    Assert.assertTrue("Expected a completed chaos operation task", foundOperation);

    // Stop the plan and verify the participant task finishes.
    coordinator.stopChaosPlan(planId);
    TaskState participant = null;
    for (int i = 0; i < 30; i++) {
      participant = coordinator.task(new TaskRequest(planId + "-participant-" + NODE_NAME));
      if (participant.state() == TaskState.State.DONE) {
        break;
      }
      Thread.sleep(500);
    }
    Assert.assertNotNull(participant);
    Assert.assertEquals(TaskState.State.DONE, participant.state());
    Assert.assertTrue(participant instanceof TaskDone);
  }

  @Test
  public void testSubmitChaosPlanViaRestEndpoint() throws Exception {
    String planId = "chaos-plan-2";
    String planJson =
        "{"
            + "\"actions\":[{"
            + "\"id\":\"occupy-cpu\","
            + "\"selector\":{\"type\":\"assign\",\"indices\":[0],\"interval\":\"100ms\"},"
            + "\"cores\":1,"
            + "\"duration\":\"100ms\""
            + "}],"
            + "\"trigger\":{\"policy\":\"sequence\",\"repeat\":1,\"interval\":{\"type\":\"fix\",\"value\":\"100ms\"}},"
            + "\"checker\":\"dummy\""
            + "}";

    coordinator.submitChaosPlan(
        new SubmitChaosPlanRequest(planId, planJson, Collections.singleton(NODE_NAME)));

    ChaosPlanResponse response = coordinator.showChaosPlan(planId);
    Assert.assertEquals(planId, response.planId());
    Assert.assertTrue(response.status().contains("is running"));

    coordinator.stopChaosPlan(planId);
    response = coordinator.showChaosPlan(planId);
    Assert.assertTrue(response.status().contains("is not running"));
  }

  private int findFreePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
