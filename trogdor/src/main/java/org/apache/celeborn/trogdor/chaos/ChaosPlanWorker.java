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

import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.node.TextNode;

import org.apache.celeborn.trogdor.platform.Platform;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.task.TaskWorker;
import org.apache.celeborn.trogdor.task.WorkerStatusTracker;

/**
 * Agent-side worker for a chaos plan. It simply validates the plan JSON and stays alive until the
 * coordinator stops it. The actual chaos operations are executed by {@link ChaosOperationWorker}
 * tasks scheduled by the coordinator-side {@link ChaosOrchestrator}.
 */
public class ChaosPlanWorker implements TaskWorker {
  private final String taskId;
  private volatile Thread workerThread;
  private volatile boolean stopped = false;

  public ChaosPlanWorker(String taskId) {
    this.taskId = taskId;
  }

  @Override
  public void start(
      Platform platform,
      TaskSpec spec,
      WorkerStatusTracker status,
      CompletableFuture<String> haltFuture)
      throws Exception {
    ChaosPlanSpec planSpec = (ChaosPlanSpec) spec;
    workerThread =
        new Thread(
            () -> {
              try {
                while (!stopped) {
                  status.update(new TextNode("chaos plan participant running"));
                  Thread.sleep(5000);
                }
                haltFuture.complete("");
              } catch (Throwable t) {
                haltFuture.complete(t.getMessage());
              }
            },
            "ChaosPlanWorker-" + taskId);
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
}
