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

package org.apache.celeborn.trogdor.coordinator;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.trogdor.client.AgentClient;
import org.apache.celeborn.trogdor.platform.Node;
import org.apache.celeborn.trogdor.rest.AgentStatusResponse;
import org.apache.celeborn.trogdor.rest.CreateWorkerRequest;
import org.apache.celeborn.trogdor.rest.StopWorkerRequest;
import org.apache.celeborn.trogdor.rest.WorkerState;

/** Manages communication with a single Trogdor agent. */
public class NodeManager {
  private static final Logger log = LoggerFactory.getLogger(NodeManager.class);
  private static final long HEARTBEAT_DELAY_MS = 1000L;

  private final Node node;
  private final TaskManager taskManager;
  private final AgentClient client;
  private final ScheduledExecutorService executor;
  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private ScheduledFuture<?> heartbeatFuture;

  public NodeManager(Node node, TaskManager taskManager) {
    this.node = node;
    this.taskManager = taskManager;
    int port = getAgentPort(node);
    this.client = new AgentClient(node.hostname(), port);
    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "TrogdorNodeManager-" + node.name());
              t.setDaemon(true);
              return t;
            });
    this.heartbeatFuture =
        executor.scheduleAtFixedRate(
            this::heartbeat, HEARTBEAT_DELAY_MS, HEARTBEAT_DELAY_MS, TimeUnit.MILLISECONDS);
  }

  private int getAgentPort(Node node) {
    String portStr = node.config().get("trogdor.agent.port");
    if (portStr == null || portStr.isEmpty()) {
      throw new IllegalArgumentException(
          "Node " + node.name() + " does not have trogdor.agent.port configured.");
    }
    return Integer.parseInt(portStr);
  }

  private void heartbeat() {
    try {
      AgentStatusResponse status = client.status();
      taskManager.updateWorkerStates(node.name(), status.workers());

      Map<Long, TaskManager.WorkerRef> desiredWorkers = taskManager.workersForNode(node.name());
      Map<Long, WorkerState> actualWorkers = status.workers();

      // Create missing workers.
      for (TaskManager.WorkerRef ref : desiredWorkers.values()) {
        if (!actualWorkers.containsKey(ref.workerId) && ref.shouldRun) {
          try {
            client.createWorker(new CreateWorkerRequest(ref.workerId, ref.taskId, ref.spec));
          } catch (Throwable e) {
            log.error("{}: error creating worker {}.", node.name(), ref.workerId, e);
          }
        }
      }

      // Stop workers that should not run anymore.
      for (Map.Entry<Long, WorkerState> entry : actualWorkers.entrySet()) {
        long workerId = entry.getKey();
        WorkerState state = entry.getValue();
        TaskManager.WorkerRef ref = desiredWorkers.get(workerId);
        if (ref == null || !ref.shouldRun) {
          if (state.state() != WorkerState.State.DONE
              && state.state() != WorkerState.State.STOPPING) {
            try {
              client.stopWorker(new StopWorkerRequest(workerId));
            } catch (Throwable e) {
              log.error("{}: error stopping worker {}.", node.name(), workerId, e);
            }
          }
        }
      }
    } catch (Throwable e) {
      log.error("{}: heartbeat failed.", node.name(), e);
    }
  }

  public void beginShutdown() {
    if (shutdown.getAndSet(true)) {
      return;
    }
    if (heartbeatFuture != null) {
      heartbeatFuture.cancel(false);
    }
    try {
      client.close();
    } catch (Throwable e) {
      log.error("{}: error closing client.", node.name(), e);
    }
  }

  public void waitForShutdown() throws Exception {
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);
  }
}
