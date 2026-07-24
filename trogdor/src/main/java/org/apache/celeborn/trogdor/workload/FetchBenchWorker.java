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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.ShuffleClientImpl;
import org.apache.celeborn.client.read.CelebornInputStream;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.trogdor.platform.Platform;
import org.apache.celeborn.trogdor.rest.JsonUtil;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.task.TaskWorker;
import org.apache.celeborn.trogdor.task.WorkerStatusTracker;

/**
 * Fetches synthetic shuffle data from Celeborn. The worker first writes a small seed record to
 * every partition, then repeatedly reads all partitions. Each worker instance owns a dedicated
 * {@link ShuffleClientImpl} and {@link LifecycleManager} to avoid interfering with other workers in
 * the same JVM.
 */
public class FetchBenchWorker implements TaskWorker {
  private final String masterHost;
  private final int masterPort;
  private final int numPartitions;
  private final long fetchesPerPartition;
  private final String userIdentifier;

  private volatile LifecycleManager lifecycleManager;
  private volatile ShuffleClientImpl shuffleClient;
  private volatile Thread workerThread;
  private volatile boolean stopped = false;

  public FetchBenchWorker(
      String masterHost,
      int masterPort,
      int numPartitions,
      long fetchesPerPartition,
      String userIdentifier) {
    this.masterHost = masterHost;
    this.masterPort = masterPort;
    this.numPartitions = numPartitions;
    this.fetchesPerPartition = fetchesPerPartition;
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
            "FetchBenchWorker");
    workerThread.setDaemon(true);
    workerThread.start();
  }

  private void runBench(
      TaskSpec spec, WorkerStatusTracker status, CompletableFuture<String> haltFuture)
      throws Exception {
    String appUniqueId = "trogdor-fetch-bench-" + System.nanoTime();
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.master.endpoints", masterHost + ":" + masterPort);
    conf.set("celeborn.client.shuffle.manager.port", "0");

    lifecycleManager = new LifecycleManager(appUniqueId, conf);
    shuffleClient = new ShuffleClientImpl(appUniqueId, conf, parseUserIdentifier(userIdentifier));
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self());

    int shuffleId = 0;
    int numMappers = 1;
    byte[] seed = new byte[64];
    for (int i = 0; i < seed.length; i++) {
      seed[i] = (byte) i;
    }
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      shuffleClient.pushDataWithCRC(
          shuffleId, 0, 0, partitionId, seed, 0, seed.length, numMappers, numPartitions);
    }
    shuffleClient.pushMergedData(shuffleId, 0, 0);
    shuffleClient.mapperEnd(shuffleId, 0, 0, numMappers, numPartitions);

    AtomicLong fetchedBytes = new AtomicLong(0);
    AtomicLong fetchedRecords = new AtomicLong(0);
    long endTimeMs =
        spec.durationMs() <= 0 ? Long.MAX_VALUE : System.currentTimeMillis() + spec.durationMs();

    long remaining = fetchesPerPartition * numPartitions;
    while (!stopped && remaining != 0 && System.currentTimeMillis() < endTimeMs) {
      int partitionId = (int) (fetchedRecords.get() % numPartitions);
      try (CelebornInputStream stream =
          shuffleClient.readPartition(
              shuffleId,
              partitionId,
              0,
              0,
              0,
              Integer.MAX_VALUE,
              new org.apache.celeborn.client.read.MetricsCallback() {
                @Override
                public void incBytesRead(long bytesWritten) {}

                @Override
                public void incReadTime(long time) {}
              })) {
        byte[] buffer = new byte[1024];
        int read;
        while ((read = stream.read(buffer)) != -1) {
          fetchedBytes.addAndGet(read);
        }
        fetchedRecords.incrementAndGet();
        if (remaining > 0) {
          remaining--;
        }
        if (fetchedRecords.get() % 1000 == 0) {
          updateStatus(status, fetchedBytes.get(), fetchedRecords.get());
        }
      } catch (IOException e) {
        updateStatus(status, fetchedBytes.get(), fetchedRecords.get());
        throw e;
      }
    }

    updateStatus(status, fetchedBytes.get(), fetchedRecords.get());
    haltFuture.complete("");
  }

  private void updateStatus(WorkerStatusTracker status, long bytes, long records) {
    ObjectNode node = JsonUtil.JSON_SERDE.createObjectNode();
    node.put("fetchedBytes", bytes);
    node.put("fetchedRecords", records);
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
