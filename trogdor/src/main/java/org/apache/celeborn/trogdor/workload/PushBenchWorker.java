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

package org.apache.celeborn.trogdor.workload;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.ShuffleClientImpl;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.trogdor.platform.Platform;
import org.apache.celeborn.trogdor.rest.JsonUtil;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.task.TaskWorker;
import org.apache.celeborn.trogdor.task.WorkerStatusTracker;

/**
 * Pushes synthetic shuffle data to Celeborn. Each agent JVM can run multiple PushBench workers;
 * therefore this worker constructs a dedicated {@link ShuffleClientImpl} (not the process
 * singleton) and a dedicated {@link LifecycleManager} per worker instance.
 */
public class PushBenchWorker implements TaskWorker {
  private final String masterHost;
  private final int masterPort;
  private final int numMappers;
  private final int numPartitions;
  private final int bytesPerPush;
  private final long totalPushes;
  private final String userIdentifier;

  private volatile LifecycleManager lifecycleManager;
  private volatile ShuffleClientImpl shuffleClient;
  private volatile Thread workerThread;
  private volatile boolean stopped = false;

  public PushBenchWorker(
      String masterHost,
      int masterPort,
      int numMappers,
      int numPartitions,
      int bytesPerPush,
      long totalPushes,
      String userIdentifier) {
    this.masterHost = masterHost;
    this.masterPort = masterPort;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
    this.bytesPerPush = bytesPerPush;
    this.totalPushes = totalPushes;
    this.userIdentifier = userIdentifier;
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
                runBench(spec, status, haltFuture);
              } catch (Throwable t) {
                haltFuture.complete(t.getMessage());
              }
            },
            "PushBenchWorker");
    workerThread.setDaemon(true);
    workerThread.start();
  }

  private void runBench(
      TaskSpec spec, WorkerStatusTracker status, CompletableFuture<String> haltFuture)
      throws Exception {
    String appUniqueId = "trogdor-push-bench-" + System.nanoTime();
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.master.endpoints", masterHost + ":" + masterPort);
    conf.set("celeborn.client.shuffle.manager.port", "0");

    lifecycleManager = new LifecycleManager(appUniqueId, conf);
    shuffleClient = new ShuffleClientImpl(appUniqueId, conf, parseUserIdentifier(userIdentifier));
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self());

    byte[] payload = new byte[bytesPerPush];
    new Random(0).nextBytes(payload);

    AtomicLong pushedBytes = new AtomicLong(0);
    AtomicLong pushedRecords = new AtomicLong(0);
    long endTimeMs =
        spec.durationMs() <= 0 ? Long.MAX_VALUE : System.currentTimeMillis() + spec.durationMs();

    long remaining = totalPushes;
    int shuffleId = 0;
    while (!stopped && remaining != 0 && System.currentTimeMillis() < endTimeMs) {
      int mapId = (int) (pushedRecords.get() % numMappers);
      int partitionId = (int) (pushedRecords.get() % numPartitions);
      try {
        int written =
            shuffleClient.pushDataWithCRC(
                shuffleId,
                mapId,
                0,
                partitionId,
                payload,
                0,
                payload.length,
                numMappers,
                numPartitions);
        pushedBytes.addAndGet(written);
        pushedRecords.incrementAndGet();
        if (remaining > 0) {
          remaining--;
        }
        if (pushedRecords.get() % 1000 == 0) {
          updateStatus(status, pushedBytes.get(), pushedRecords.get());
        }
      } catch (IOException e) {
        updateStatus(status, pushedBytes.get(), pushedRecords.get());
        throw e;
      }
    }

    updateStatus(status, pushedBytes.get(), pushedRecords.get());
    haltFuture.complete("");
  }

  private void updateStatus(WorkerStatusTracker status, long bytes, long records) {
    ObjectNode node = JsonUtil.JSON_SERDE.createObjectNode();
    node.put("pushedBytes", bytes);
    node.put("pushedRecords", records);
    status.update(node);
  }

  private UserIdentifier parseUserIdentifier(String userIdentifier) {
    String[] parts = userIdentifier.split(":");
    if (parts.length == 2) {
      return new UserIdentifier(parts[0], parts[1]);
    }
    return new UserIdentifier("default", "default");
  }

  @Override
  public void stop(Platform platform) throws Exception {
    stopped = true;
    if (workerThread != null) {
      workerThread.interrupt();
    }
    if (shuffleClient != null) {
      shuffleClient.shutdown();
    }
    if (lifecycleManager != null) {
      lifecycleManager.stop();
    }
  }
}
