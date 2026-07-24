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

package org.apache.celeborn.trogdor.fault;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.TextNode;

import org.apache.celeborn.trogdor.platform.Platform;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.task.TaskWorker;
import org.apache.celeborn.trogdor.task.WorkerStatusTracker;

/** Pauses and resumes a process on the agent. */
public class ProcessStopFaultWorker implements TaskWorker {
  private final String processName;
  private volatile Thread workerThread;
  private volatile boolean stopped = false;

  public ProcessStopFaultWorker(String processName) {
    this.processName = processName;
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
                List<Integer> pids = findPids(processName);
                if (pids.isEmpty()) {
                  throw new RuntimeException("No process found matching: " + processName);
                }
                status.update(new TextNode("pausing pids: " + pids));
                for (int pid : pids) {
                  sendSignal(pid, "STOP");
                }
                long duration = Math.max(0, spec.durationMs());
                if (duration <= 0) {
                  while (!stopped) {
                    Thread.sleep(100);
                  }
                } else {
                  Thread.sleep(duration);
                }
                for (int pid : pids) {
                  sendSignal(pid, "CONT");
                }
                status.update(new TextNode("resumed pids: " + pids));
                haltFuture.complete("");
              } catch (Throwable t) {
                haltFuture.complete(t.getMessage());
              }
            },
            "ProcessStopFaultWorker");
    workerThread.setDaemon(true);
    workerThread.start();
  }

  @Override
  public void stop(Platform platform) throws Exception {
    stopped = true;
    if (workerThread != null) {
      workerThread.interrupt();
    }
  }

  private List<Integer> findPids(String processName) throws Exception {
    ProcessBuilder pb = new ProcessBuilder("pgrep", "-f", processName);
    pb.redirectErrorStream(true);
    Process process = pb.start();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String output = reader.lines().collect(Collectors.joining("\n"));
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        return new ArrayList<>();
      }
      List<Integer> pids = new ArrayList<>();
      for (String line : output.split("\n")) {
        if (!line.trim().isEmpty()) {
          pids.add(Integer.parseInt(line.trim()));
        }
      }
      return pids;
    }
  }

  private void sendSignal(int pid, String signal) throws Exception {
    ProcessBuilder pb = new ProcessBuilder("kill", "-" + signal, String.valueOf(pid));
    pb.inheritIO();
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException("kill -" + signal + " " + pid + " failed with code " + exitCode);
    }
  }
}
