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
import org.apache.celeborn.trogdor.rest.CreateTaskRequest;
import org.apache.celeborn.trogdor.rest.TaskDone;
import org.apache.celeborn.trogdor.rest.TaskRequest;
import org.apache.celeborn.trogdor.rest.TaskState;
import org.apache.celeborn.trogdor.service.TrogdorAgent;
import org.apache.celeborn.trogdor.service.TrogdorCoordinator;
import org.apache.celeborn.trogdor.task.NoOpTaskSpec;

public class TrogdorCoordinatorAgentTest {
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

    Map<String, String> nodeConfig = new HashMap<>();
    nodeConfig.put("trogdor.agent.port", String.valueOf(agentPort));
    BasicNode node = new BasicNode(NODE_NAME, "localhost", nodeConfig);
    BasicPlatform platform = new BasicPlatform(Collections.singletonList(node), NODE_NAME);

    agent = new TrogdorAgent(conf, platform);
    agent.initialize();

    coordinator = new TrogdorCoordinator(conf, platform);
    coordinator.initialize();

    // Wait for services to start.
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
  public void testNoOpTask() throws Exception {
    String taskId = "noop-task-1";
    NoOpTaskSpec spec =
        new NoOpTaskSpec(System.currentTimeMillis(), 5000, Collections.singleton(NODE_NAME));
    coordinator.createTask(new CreateTaskRequest(taskId, spec));

    // Poll for task completion.
    TaskState state = null;
    for (int i = 0; i < 30; i++) {
      state = coordinator.task(new TaskRequest(taskId));
      if (state.state() == TaskState.State.DONE) {
        break;
      }
      Thread.sleep(500);
    }

    Assert.assertNotNull(state);
    Assert.assertEquals(TaskState.State.DONE, state.state());
    Assert.assertTrue("expected TaskDone but was " + state.getClass().getSimpleName(), state instanceof TaskDone);
    TaskDone done = (TaskDone) state;
    Assert.assertTrue(
        "NoOp task finished with non-empty error: '" + done.error() + "' (status=" + done.status() + ")",
        done.error() == null || done.error().isEmpty());
  }

  private int findFreePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
