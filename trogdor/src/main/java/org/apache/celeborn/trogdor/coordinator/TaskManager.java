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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.trogdor.platform.Platform;
import org.apache.celeborn.trogdor.rest.JsonUtil;
import org.apache.celeborn.trogdor.rest.TaskDone;
import org.apache.celeborn.trogdor.rest.TaskPending;
import org.apache.celeborn.trogdor.rest.TaskRequest;
import org.apache.celeborn.trogdor.rest.TaskRunning;
import org.apache.celeborn.trogdor.rest.TaskState;
import org.apache.celeborn.trogdor.rest.TaskStopping;
import org.apache.celeborn.trogdor.rest.TasksRequest;
import org.apache.celeborn.trogdor.rest.TasksResponse;
import org.apache.celeborn.trogdor.rest.WorkerState;
import org.apache.celeborn.trogdor.task.TaskController;
import org.apache.celeborn.trogdor.task.TaskSpec;

/** Manages the lifecycle of tasks on the coordinator. */
public class TaskManager {
  private static final Logger log = LoggerFactory.getLogger(TaskManager.class);

  private final Platform platform;
  private final Map<String, ManagedTask> tasks = new HashMap<>();
  private final Map<Long, WorkerState> workerStates = new HashMap<>();
  private final Map<String, NodeManager> nodeManagers;
  private final ScheduledExecutorService executor;
  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private final AtomicLong nextWorkerId;

  public TaskManager(Platform platform, long firstWorkerId) {
    this.platform = platform;
    this.nextWorkerId = new AtomicLong(firstWorkerId);
    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "TrogdorTaskManager");
              t.setDaemon(true);
              return t;
            });
    this.nodeManagers = new HashMap<>();
    for (org.apache.celeborn.trogdor.platform.Node node : platform.topology().nodes()) {
      this.nodeManagers.put(node.name(), new NodeManager(node, this));
    }
    log.info("Created TaskManager for agent(s) on: {}", String.join(", ", nodeManagers.keySet()));
  }

  enum InternalState {
    PENDING,
    RUNNING,
    STOPPING,
    DONE
  }

  public static class WorkerRef {
    public final long workerId;
    public final String taskId;
    public final TaskSpec spec;
    public final boolean shouldRun;

    public WorkerRef(long workerId, String taskId, TaskSpec spec, boolean shouldRun) {
      this.workerId = workerId;
      this.taskId = taskId;
      this.spec = spec;
      this.shouldRun = shouldRun;
    }
  }

  class ManagedTask {
    final String id;
    final TaskSpec spec;
    final TaskController controller;
    InternalState state = InternalState.PENDING;
    long startedMs = -1;
    long doneMs = -1;
    String error = "";
    boolean cancelled = false;
    final TreeMap<String, Long> workerIds = new TreeMap<>();

    ManagedTask(String id, TaskSpec spec, TaskController controller) {
      this.id = id;
      this.spec = spec;
      this.controller = controller;
    }

    TaskState toTaskState() {
      switch (state) {
        case PENDING:
          return new TaskPending(spec);
        case RUNNING:
          return new TaskRunning(spec, startedMs, combinedStatus());
        case STOPPING:
          return new TaskStopping(spec, startedMs, combinedStatus());
        case DONE:
          return new TaskDone(spec, startedMs, doneMs, error, cancelled, combinedStatus());
        default:
          throw new IllegalStateException("Unknown state: " + state);
      }
    }

    JsonNode combinedStatus() {
      if (workerIds.size() == 1) {
        WorkerState ws = workerStates.get(workerIds.values().iterator().next());
        return ws == null ? JsonNodeFactory.instance.nullNode() : ws.status();
      } else {
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        for (Map.Entry<String, Long> entry : workerIds.entrySet()) {
          WorkerState ws = workerStates.get(entry.getValue());
          if (ws != null && ws.status() != null) {
            node.set(entry.getKey(), ws.status());
          }
        }
        return node;
      }
    }
  }

  public synchronized void createTask(String id, TaskSpec originalSpec) {
    if (tasks.containsKey(id)) {
      throw new IllegalStateException("Task " + id + " already exists.");
    }
    long now = System.currentTimeMillis();
    TaskSpec spec = rebaseSpec(originalSpec, now);
    TaskController controller = spec.newController(id);
    ManagedTask task = new ManagedTask(id, spec, controller);
    tasks.put(id, task);
    long delayMs = Math.max(0, spec.startMs() - now);
    log.info("Created task {}, will start in {} ms.", id, delayMs);
    executor.schedule(() -> runTask(id), delayMs, TimeUnit.MILLISECONDS);
  }

  public synchronized void stopTask(String id) {
    ManagedTask task = tasks.get(id);
    if (task == null) {
      return;
    }
    if (task.state == InternalState.DONE) {
      return;
    }
    task.cancelled = true;
    if (task.state == InternalState.PENDING) {
      task.state = InternalState.DONE;
      task.doneMs = System.currentTimeMillis();
      task.error = "stopped";
      log.info("Task {} was stopped while pending.", id);
    } else {
      task.state = InternalState.STOPPING;
      log.info("Task {} is stopping.", id);
    }
  }

  public synchronized void destroyTask(String id) {
    ManagedTask task = tasks.get(id);
    if (task == null) {
      return;
    }
    stopTask(id);
    if (task.state == InternalState.DONE) {
      tasks.remove(id);
    }
  }

  public synchronized TaskState task(TaskRequest request) {
    ManagedTask task = tasks.get(request.id());
    if (task == null) {
      throw new IllegalArgumentException("Task " + request.id() + " not found.");
    }
    return task.toTaskState();
  }

  public synchronized int taskCount() {
    return tasks.size();
  }

  public synchronized int pendingTaskCount() {
    return countTasksInState(InternalState.PENDING);
  }

  public synchronized int runningTaskCount() {
    return countTasksInState(InternalState.RUNNING);
  }

  public synchronized int stoppingTaskCount() {
    return countTasksInState(InternalState.STOPPING);
  }

  public synchronized int doneTaskCount() {
    return countTasksInState(InternalState.DONE);
  }

  private int countTasksInState(InternalState state) {
    int count = 0;
    for (ManagedTask task : tasks.values()) {
      if (task.state == state) {
        count++;
      }
    }
    return count;
  }

  public synchronized TasksResponse tasks(TasksRequest request) {
    Map<String, TaskState> result = new HashMap<>();
    Set<String> ids = request.ids();
    for (Map.Entry<String, ManagedTask> entry : tasks.entrySet()) {
      if (ids.isEmpty() || ids.contains(entry.getKey())) {
        result.put(entry.getKey(), entry.getValue().toTaskState());
      }
    }
    return new TasksResponse(result);
  }

  public synchronized Map<Long, WorkerRef> workersForNode(String nodeName) {
    Map<Long, WorkerRef> result = new HashMap<>();
    for (ManagedTask task : tasks.values()) {
      if (task.state == InternalState.DONE) {
        continue;
      }
      Long workerId = task.workerIds.get(nodeName);
      if (workerId != null) {
        result.put(
            workerId,
            new WorkerRef(workerId, task.id, task.spec, task.state == InternalState.RUNNING));
      }
    }
    return result;
  }

  public synchronized void updateWorkerStates(String nodeName, Map<Long, WorkerState> states) {
    for (Map.Entry<Long, WorkerState> entry : states.entrySet()) {
      WorkerState state = entry.getValue();
      workerStates.put(entry.getKey(), state);
      if (state.state() == WorkerState.State.DONE) {
        maybeFinishTask(state.taskId());
      }
    }
  }

  private synchronized void maybeFinishTask(String taskId) {
    ManagedTask task = tasks.get(taskId);
    if (task == null || task.state == InternalState.DONE) {
      return;
    }
    boolean allDone = true;
    for (Long workerId : task.workerIds.values()) {
      WorkerState ws = workerStates.get(workerId);
      if (ws == null || ws.state() != WorkerState.State.DONE) {
        allDone = false;
        break;
      }
    }
    if (allDone) {
      task.state = InternalState.DONE;
      task.doneMs = System.currentTimeMillis();
      for (Long workerId : task.workerIds.values()) {
        WorkerState ws = workerStates.get(workerId);
        if (ws != null && ws.error() != null && !ws.error().isEmpty() && task.error.isEmpty()) {
          task.error = ws.error();
        }
      }
      log.info("Task {} finished with error='{}'.", taskId, task.error);
    }
  }

  private synchronized void runTask(String id) {
    ManagedTask task = tasks.get(id);
    if (task == null || task.state != InternalState.PENDING) {
      return;
    }
    try {
      Set<String> nodeNames = task.controller.targetNodes(platform.topology());
      TreeSet<String> validNodeNames = new TreeSet<>();
      TreeSet<String> invalidNodeNames = new TreeSet<>();
      for (String nodeName : nodeNames) {
        if (nodeManagers.containsKey(nodeName)) {
          validNodeNames.add(nodeName);
        } else {
          invalidNodeNames.add(nodeName);
        }
      }
      if (!invalidNodeNames.isEmpty()) {
        throw new IllegalArgumentException("Unknown node names: " + invalidNodeNames);
      }
      if (validNodeNames.isEmpty()) {
        throw new IllegalArgumentException("No node names specified.");
      }
      task.state = InternalState.RUNNING;
      task.startedMs = System.currentTimeMillis();
      for (String nodeName : validNodeNames) {
        long workerId = nextWorkerId.getAndIncrement();
        task.workerIds.put(nodeName, workerId);
      }
      log.info("Task {} is running on nodes: {}", id, String.join(", ", validNodeNames));

      // Schedule automatic stop based on duration.
      long delayMs = Math.max(0, task.spec.endMs() - System.currentTimeMillis());
      executor.schedule(() -> stopTask(id), delayMs, TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      log.error("Failed to run task {}.", id, e);
      task.state = InternalState.DONE;
      task.doneMs = System.currentTimeMillis();
      task.error = e.getMessage();
    }
  }

  private TaskSpec rebaseSpec(TaskSpec spec, long now) {
    if (spec.startMs() >= now) {
      return spec;
    }
    ObjectNode node = JsonUtil.objectNode();
    node.set("startMs", JsonNodeFactory.instance.numberNode(now));
    node.set("durationMs", JsonNodeFactory.instance.numberNode(spec.durationMs()));
    // Copy other fields by serializing to JSON and back.
    String json = JsonUtil.toJsonString(spec);
    // This is a simplified rebase: we just shift startMs, but for polymorphic specs,
    // we need to preserve all fields. Use Jackson treeToValue for full fidelity.
    try {
      com.fasterxml.jackson.databind.JsonNode tree = JsonUtil.JSON_SERDE.valueToTree(spec);
      ((ObjectNode) tree).set("startMs", JsonNodeFactory.instance.numberNode(now));
      return JsonUtil.JSON_SERDE.treeToValue(tree, TaskSpec.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void beginShutdown() {
    if (shutdown.getAndSet(true)) {
      return;
    }
    for (NodeManager nodeManager : nodeManagers.values()) {
      nodeManager.beginShutdown();
    }
    for (ManagedTask task : tasks.values()) {
      stopTask(task.id);
    }
  }

  public void waitForShutdown() throws Exception {
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);
    for (NodeManager nodeManager : nodeManagers.values()) {
      nodeManager.waitForShutdown();
    }
  }
}
