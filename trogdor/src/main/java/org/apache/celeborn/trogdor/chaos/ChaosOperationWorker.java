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

package org.apache.celeborn.trogdor.chaos;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.celeborn.trogdor.platform.Platform;
import org.apache.celeborn.trogdor.rest.JsonUtil;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.task.TaskWorker;
import org.apache.celeborn.trogdor.task.WorkerStatusTracker;

/** Executes a single chaos operation on the agent. */
public class ChaosOperationWorker implements TaskWorker {
  private final ChaosOperationSpec.OperationType operationType;
  private final String command;
  private final int cores;
  private final long occupyCpuDurationMs;

  private volatile Thread workerThread;

  public ChaosOperationWorker(
      ChaosOperationSpec.OperationType operationType,
      String command,
      int cores,
      long occupyCpuDurationMs) {
    this.operationType = operationType;
    this.command = command;
    this.cores = cores;
    this.occupyCpuDurationMs = occupyCpuDurationMs;
  }

  @Override
  public void start(
      Platform platform,
      TaskSpec spec,
      WorkerStatusTracker status,
      CompletableFuture<String> haltFuture)
      throws Exception {
    workerThread =
        new Thread(
            () -> {
              try {
                ObjectNode node = JsonUtil.JSON_SERDE.createObjectNode();
                switch (operationType) {
                  case BASH:
                    String output = executeBash(command);
                    node.put("output", output);
                    break;
                  case OCCUPY_CPU:
                    occupyCpu(cores, occupyCpuDurationMs);
                    node.put("occupiedCores", cores);
                    node.put("durationMs", occupyCpuDurationMs);
                    break;
                  default:
                    throw new IllegalStateException("Unknown operation type: " + operationType);
                }
                status.update(node);
                haltFuture.complete("");
              } catch (Throwable t) {
                status.update(
                    new ObjectNode(JsonUtil.JSON_SERDE.getNodeFactory())
                        .put("error", t.getMessage()));
                haltFuture.complete(t.getMessage());
              }
            },
            "ChaosOperationWorker");
    workerThread.setDaemon(true);
    workerThread.start();
  }

  private String executeBash(String command) throws Exception {
    ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
    pb.redirectErrorStream(true);
    Process process = pb.start();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String output = reader.lines().collect(Collectors.joining("\n"));
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        throw new RuntimeException("Command exited with code " + exitCode + ": " + output);
      }
      return output;
    }
  }

  private void occupyCpu(int cores, long durationMs) throws InterruptedException {
    AtomicBoolean flag = new AtomicBoolean(true);
    Thread[] threads = new Thread[cores];
    for (int i = 0; i < cores; i++) {
      threads[i] =
          new Thread(
              () -> {
                java.util.Random random = new java.util.Random();
                long c = 0;
                while (flag.get()) {
                  int a = random.nextInt();
                  int b = random.nextInt();
                  c = b != 0 ? a + b * a / b : a + b * a;
                }
              },
              "ChaosCpuConsumer");
      threads[i].setDaemon(true);
      threads[i].start();
    }
    Thread.sleep(Math.min(durationMs, 5 * 60 * 1000));
    flag.set(false);
    for (Thread t : threads) {
      t.interrupt();
    }
  }

  @Override
  public void stop(Platform platform) throws Exception {
    if (workerThread != null) {
      workerThread.interrupt();
    }
  }
}
