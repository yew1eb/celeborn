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

package org.apache.celeborn.trogdor.agent;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.trogdor.platform.Platform;
import org.apache.celeborn.trogdor.rest.WorkerState;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.task.TaskWorker;
import org.apache.celeborn.trogdor.task.WorkerStatusTracker;

/** Manages the lifecycle of TaskWorkers on the agent. */
public class WorkerManager {
  private static final Logger log = LoggerFactory.getLogger(WorkerManager.class);

  private final Platform platform;
  private final ScheduledExecutorService executor;
  private final Map<Long, ManagedWorker> workers = new HashMap<>();
  private final AtomicBoolean shutdown = new AtomicBoolean(false);

  public WorkerManager(Platform platform) {
    this.platform = platform;
    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "TrogdorWorkerManager");
              t.setDaemon(true);
              return t;
            });
  }

  enum InternalState {
    RECEIVING,
    STARTING,
    RUNNING,
    STOPPING,
    DONE
  }

  class AgentStatusTracker implements WorkerStatusTracker {
    private final ManagedWorker worker;
    private volatile JsonNode status = NullNode.getInstance();

    AgentStatusTracker(ManagedWorker worker) {
      this.worker = worker;
    }

    @Override
    public void update(JsonNode status) {
      this.status = status;
    }

    JsonNode get() {
      return status;
    }
  }

  class ManagedWorker {
    final long workerId;
    final String taskId;
    final TaskSpec spec;
    final TaskWorker taskWorker;
    final AgentStatusTracker status;
    final CompletableFuture<String> haltFuture = new CompletableFuture<>();
    final long startedMs;
    volatile InternalState state = InternalState.RECEIVING;
    volatile long doneMs = -1;
    volatile String error = "";
    volatile Future<?> stopFuture = null;
    volatile boolean mustDestroy = false;

    ManagedWorker(long workerId, String taskId, TaskSpec spec) {
      this.workerId = workerId;
      this.taskId = taskId;
      this.spec = spec;
      this.taskWorker = spec.newTaskWorker(taskId);
      this.status = new AgentStatusTracker(this);
      this.startedMs = System.currentTimeMillis();
    }

    WorkerState toWorkerState() {
      WorkerState.State external;
      switch (state) {
        case RECEIVING:
          external = WorkerState.State.RECEIVING;
          break;
        case STARTING:
          external = WorkerState.State.STARTING;
          break;
        case RUNNING:
          external = WorkerState.State.RUNNING;
          break;
        case STOPPING:
          external = WorkerState.State.STOPPING;
          break;
        case DONE:
          external = WorkerState.State.DONE;
          break;
        default:
          throw new IllegalStateException("Unknown state: " + state);
      }
      return new WorkerState(workerId, taskId, external, status.get(), error);
    }
  }

  public synchronized int workerCount() {
    return workers.size();
  }

  public synchronized int receivingWorkerCount() {
    return countWorkersInState(InternalState.RECEIVING);
  }

  public synchronized int startingWorkerCount() {
    return countWorkersInState(InternalState.STARTING);
  }

  public synchronized int runningWorkerCount() {
    return countWorkersInState(InternalState.RUNNING);
  }

  public synchronized int stoppingWorkerCount() {
    return countWorkersInState(InternalState.STOPPING);
  }

  public synchronized int doneWorkerCount() {
    return countWorkersInState(InternalState.DONE);
  }

  private int countWorkersInState(InternalState state) {
    int count = 0;
    for (ManagedWorker worker : workers.values()) {
      if (worker.state == state) {
        count++;
      }
    }
    return count;
  }

  public synchronized Map<Long, WorkerState> workerStates() {
    Map<Long, WorkerState> result = new HashMap<>();
    for (Map.Entry<Long, ManagedWorker> entry : workers.entrySet()) {
      result.put(entry.getKey(), entry.getValue().toWorkerState());
    }
    return Collections.unmodifiableMap(result);
  }

  public synchronized void createWorker(long workerId, String taskId, TaskSpec spec)
      throws Throwable {
    if (shutdown.get()) {
      throw new IllegalStateException("WorkerManager is shut down.");
    }
    if (workers.containsKey(workerId)) {
      throw new IllegalStateException("Worker " + workerId + " already exists.");
    }
    ManagedWorker worker = new ManagedWorker(workerId, taskId, spec);
    workers.put(workerId, worker);
    log.info("Created worker {} for task {}.", workerId, taskId);

    executor.submit(
        () -> {
          try {
            worker.state = InternalState.STARTING;
            worker.taskWorker.start(platform, worker.spec, worker.status, worker.haltFuture);
            synchronized (this) {
              if (worker.state == InternalState.STARTING) {
                worker.state = InternalState.RUNNING;
                long delayMs = Math.max(0, spec.endMs() - System.currentTimeMillis());
                executor.schedule(
                    () -> stopWorker(workerId, false), delayMs, TimeUnit.MILLISECONDS);
                log.info("Worker {} is now RUNNING, will stop in {} ms.", workerId, delayMs);
              }
            }
          } catch (Throwable e) {
            log.error("Failed to start worker {}.", workerId, e);
            finishWorker(worker, e.getMessage());
          }
        });

    worker.haltFuture.whenComplete(
        (error, throwable) -> {
          String err = error;
          if (throwable != null) {
            err = throwable.getMessage();
          }
          if (err == null) {
            err = "";
          }
          synchronized (this) {
            if (worker.state != InternalState.DONE) {
              final String finalErr = err;
              worker.stopFuture =
                  executor.submit(
                      () -> {
                        try {
                          worker.taskWorker.stop(platform);
                        } catch (Throwable t) {
                          log.error("Error stopping worker {}.", workerId, t);
                        } finally {
                          finishWorker(worker, finalErr);
                        }
                      });
            }
          }
        });
  }

  public synchronized void stopWorker(long workerId, boolean destroy) {
    ManagedWorker worker = workers.get(workerId);
    if (worker == null) {
      return;
    }
    if (destroy) {
      worker.mustDestroy = true;
    }
    if (worker.state == InternalState.DONE) {
      if (destroy) {
        workers.remove(workerId);
      }
      return;
    }
    if (worker.state == InternalState.STOPPING) {
      return;
    }
    worker.state = InternalState.STOPPING;
    final String finalErr = destroy ? "destroyed" : "stopped";
    worker.stopFuture =
        executor.submit(
            () -> {
              try {
                worker.taskWorker.stop(platform);
              } catch (Throwable t) {
                log.error("Error stopping worker {}.", workerId, t);
              } finally {
                finishWorker(worker, finalErr);
              }
            });
  }

  private synchronized void finishWorker(ManagedWorker worker, String error) {
    if (worker.state == InternalState.DONE) {
      return;
    }
    worker.state = InternalState.DONE;
    worker.doneMs = System.currentTimeMillis();
    worker.error = error == null ? "" : error;
    log.info("Worker {} finished with error='{}'.", worker.workerId, worker.error);
    if (worker.mustDestroy) {
      workers.remove(worker.workerId);
    }
  }

  public synchronized void beginShutdown() {
    if (shutdown.getAndSet(true)) {
      return;
    }
    for (ManagedWorker worker : workers.values()) {
      stopWorker(worker.workerId, false);
    }
  }

  public void waitForShutdown() throws Exception {
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);
  }
}
