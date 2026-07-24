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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.TextNode;

import org.apache.celeborn.trogdor.platform.Platform;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.task.TaskWorker;
import org.apache.celeborn.trogdor.task.WorkerStatusTracker;

/** Executes an external command on the agent. */
public class ExternalCommandFaultWorker implements TaskWorker {
  private final String[] command;
  private final Map<String, String> env;
  private volatile Process process;

  public ExternalCommandFaultWorker(String[] command, Map<String, String> env) {
    this.command = command;
    this.env = env;
  }

  @Override
  public void start(
      Platform platform,
      TaskSpec spec,
      WorkerStatusTracker status,
      CompletableFuture<String> haltFuture)
      throws Exception {
    Thread workerThread =
        new Thread(
            () -> {
              try {
                ProcessBuilder pb = new ProcessBuilder(command);
                if (env != null) {
                  pb.environment().putAll(env);
                }
                pb.redirectErrorStream(true);
                process = pb.start();
                try (BufferedReader reader =
                    new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                  String output = reader.lines().collect(Collectors.joining("\n"));
                  int exitCode = process.waitFor();
                  status.update(new TextNode("exitCode=" + exitCode + ", output=" + output));
                  if (exitCode != 0) {
                    haltFuture.complete("Command exited with code " + exitCode + ": " + output);
                  } else {
                    haltFuture.complete("");
                  }
                }
              } catch (Throwable t) {
                haltFuture.complete(t.getMessage());
              }
            },
            "ExternalCommandFaultWorker");
    workerThread.setDaemon(true);
    workerThread.start();
  }

  @Override
  public void stop(Platform platform) throws Exception {
    if (process != null) {
      process.destroyForcibly();
    }
  }
}
