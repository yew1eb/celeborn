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

package org.apache.celeborn.service.deploy.master.clustermeta.ha;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.client.MasterClient;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos.ResourceRequest;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos.Type;

/**
 * Aggregates worker/app heartbeats on the raft leader and flushes them as a single
 * {@link Type#BatchHeartbeat} raft log entry per {@code batch.interval}, so N heartbeats cost one
 * replication/fsync/apply instead of N.
 */
public class HeartbeatAggregator {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatAggregator.class);

  private final HARaftServer ratisServer;

  private final LinkedBlockingQueue<ResourceProtos.WorkerHeartbeatRequest> workerQueue =
      new LinkedBlockingQueue<>();
  private final LinkedBlockingQueue<ResourceProtos.AppHeartbeatRequest> appQueue =
      new LinkedBlockingQueue<>();

  private final ScheduledExecutorService flushExecutor;

  public HeartbeatAggregator(HARaftServer ratisServer, CelebornConf conf) {
    this.ratisServer = ratisServer;
    long batchIntervalMs = conf.masterHaHeartbeatBatchIntervalMs();
    this.flushExecutor =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-heartbeat-aggregator");
    this.flushExecutor.scheduleWithFixedDelay(
        this::flushSafely, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
    LOG.info("HeartbeatAggregator started, flush interval {} ms.", batchIntervalMs);
  }

  public void offerWorkerHeartbeat(ResourceProtos.WorkerHeartbeatRequest heartbeat) {
    workerQueue.offer(heartbeat);
  }

  public void offerAppHeartbeat(ResourceProtos.AppHeartbeatRequest heartbeat) {
    appQueue.offer(heartbeat);
  }

  public void stop() {
    flushExecutor.shutdownNow();
  }

  private void flushSafely() {
    try {
      flush();
    } catch (Throwable t) {
      // Dropped batches self-heal next interval.
      LOG.error("Failed to flush aggregated heartbeats, dropping this batch.", t);
    }
  }

  private void flush() {
    List<ResourceProtos.WorkerHeartbeatRequest> drainedWorkers = new ArrayList<>();
    workerQueue.drainTo(drainedWorkers);
    List<ResourceProtos.AppHeartbeatRequest> drainedApps = new ArrayList<>();
    appQueue.drainTo(drainedApps);
    if (drainedWorkers.isEmpty() && drainedApps.isEmpty()) {
      // Empty window: no batch to submit.
      return;
    }

    if (!ratisServer.isLeader()) {
      // Standby: drained heartbeats are not committable and are renewable, so discard rather than
      // submit a request that would fail with NotLeader.
      return;
    }

    ResourceRequest batchRequest =
        ResourceRequest.newBuilder()
            .setCmdType(Type.BatchHeartbeat)
            .setRequestId(MasterClient.genRequestId())
            .setBatchHeartbeatRequest(
                ResourceProtos.BatchHeartbeatRequest.newBuilder()
                    .addAllWorkerHeartbeats(drainedWorkers)
                    .addAllAppHeartbeats(drainedApps)
                    .build())
            .build();
    ratisServer.submitRequest(batchRequest);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Flushed aggregated heartbeats, {} worker heartbeats, {} app heartbeats.",
          drainedWorkers.size(),
          drainedApps.size());
    }
  }
}
