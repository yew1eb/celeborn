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

import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.node.TextNode;

import org.apache.celeborn.trogdor.platform.Platform;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.task.TaskWorker;
import org.apache.celeborn.trogdor.task.WorkerStatusTracker;

/**
 * Simulates slow disk IO using device-mapper delay target. This is a simplified implementation that
 * logs the intended operation. In production tests, this should be replaced with real device-mapper
 * setup.
 */
public class DiskSlowFaultWorker implements TaskWorker {
  private final String device;
  private final long delayMs;
  private volatile Thread workerThread;
  private volatile boolean stopped = false;

  public DiskSlowFaultWorker(String device, long delayMs) {
    this.device = device;
    this.delayMs = delayMs;
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
                status.update(
                    new TextNode(
                        "Simulating disk slow on " + device + " with delay " + delayMs + " ms"));
                long duration = Math.max(0, spec.durationMs());
                if (duration <= 0) {
                  while (!stopped) {
                    Thread.sleep(100);
                  }
                } else {
                  Thread.sleep(duration);
                }
                status.update(new TextNode("Disk slow simulation finished"));
                haltFuture.complete("");
              } catch (Throwable t) {
                haltFuture.complete(t.getMessage());
              }
            },
            "DiskSlowFaultWorker");
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
