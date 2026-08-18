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
 * Aggregates worker/app heartbeats on the raft leader and flushes them periodically as a single
 * {@link Type#BatchHeartbeat} raft log entry, so that N heartbeats cost one replication, one fsync
 * and one apply instead of N.
 *
 * <p>Concurrency model (following the repository's ReviveManager pattern): multiple producer
 * threads offer heartbeats to {@link LinkedBlockingQueue}s; a single flush thread drains them via
 * {@link LinkedBlockingQueue#drainTo} and submits the batch to raft outside any celeborn-level
 * lock. Producers never block on the aggregator.
 *
 * <p>Flush is purely time-driven: one drain+submit per {@code batch.interval}, regardless of
 * arrival rate. There is no size-based early flush -- heartbeats are periodic, tolerate the
 * interval latency (worker/app timeouts are 120s/300s), and a size threshold would either stay
 * dormant at typical scale or multiply flushes (and thus raft writes) at large scale, the opposite
 * of the goal. Flush frequency is {@code 1 / interval} at any cluster size, so the raft-write
 * reduction grows with the cluster.
 *
 * <p>Heartbeats are buffered as-is (not deduplicated). Per-key dedup would be dormant in steady
 * state because the heartbeat intervals (worker 30s, app 10s) are much larger than the default 1s
 * flush window, and even when retries make a key repeat within a window the final meta state is
 * unchanged (last apply wins). Queue depth is bounded in practice by the heartbeat arrival rate
 * times the flush interval. A stalled flush surfaces as the pre-existing
 * {@code RatisApplyCompletedIndex} going flat (the aggregator adds no metric of its own) rather
 * than backpressure, since the reply path never waits for raft. A batch that fails to submit
 * (e.g. leadership change) is dropped and logged; heartbeats are self-healing on the next interval.
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
      // Dropping a batch is safe: heartbeats are retried by workers/apps on the next interval.
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

    // Standby (non-leader) masters also run a flush thread. Drained heartbeats here are not
    // committable (this master is not the leader) and are renewable (re-sent next interval), so
    // discard them instead of submitting a request that would fail with NotLeader. This keeps the
    // queues from accumulating on standbys and avoids NotLeader exception log noise.
    if (!ratisServer.isLeader()) {
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
