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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

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
 * <p>Concurrency model (borrowing Kafka KRaft's BatchAccumulator): multiple producer threads offer
 * heartbeats under a short lock; a single flush thread swaps out the pending maps under the same
 * lock and submits the batch to raft outside the lock.
 *
 * <p>Flush semantics (borrowing TiKV's cmd-batch): at each flush tick, whatever is pending is
 * drained as-is; the size threshold only seals a batch early, the aggregator never waits to fill a
 * batch.
 *
 * <p>Heartbeats are keyed by worker/app id with last-write-wins inside one window, which both
 * deduplicates retries and naturally bounds the pending maps by the number of workers plus the
 * number of running applications. A batch that fails to submit (e.g. leadership change) is dropped
 * and logged; heartbeats are self-healing on the next interval.
 */
public class HeartbeatAggregator {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatAggregator.class);

  private final HARaftServer ratisServer;
  private final int batchSize;

  private final ReentrantLock lock = new ReentrantLock();
  private LinkedHashMap<String, ResourceProtos.WorkerHeartbeatRequest> workerHeartbeats =
      new LinkedHashMap<>();
  private LinkedHashMap<String, ResourceProtos.AppHeartbeatRequest> appHeartbeats =
      new LinkedHashMap<>();

  private final ScheduledExecutorService flushExecutor;
  private final AtomicLong flushCount = new AtomicLong(0);
  private volatile int lastBatchSize = 0;
  private volatile long lastFlushDurationMs = 0;

  public HeartbeatAggregator(HARaftServer ratisServer, CelebornConf conf) {
    this.ratisServer = ratisServer;
    this.batchSize = conf.masterHaHeartbeatBatchSize();
    long batchIntervalMs = conf.masterHaHeartbeatBatchIntervalMs();
    this.flushExecutor =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-heartbeat-aggregator");
    this.flushExecutor.scheduleWithFixedDelay(
        this::flushSafely, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
    LOG.info(
        "HeartbeatAggregator started, flush interval {} ms, batch size {}.",
        batchIntervalMs,
        batchSize);
  }

  public void offerWorkerHeartbeat(ResourceProtos.WorkerHeartbeatRequest heartbeat) {
    lock.lock();
    try {
      workerHeartbeats.put(workerKey(heartbeat), heartbeat);
      flushEarlyIfNecessary();
    } finally {
      lock.unlock();
    }
  }

  public void offerAppHeartbeat(ResourceProtos.AppHeartbeatRequest heartbeat) {
    lock.lock();
    try {
      appHeartbeats.put(heartbeat.getAppId(), heartbeat);
      flushEarlyIfNecessary();
    } finally {
      lock.unlock();
    }
  }

  public int pendingCount() {
    lock.lock();
    try {
      return workerHeartbeats.size() + appHeartbeats.size();
    } finally {
      lock.unlock();
    }
  }

  public long flushCount() {
    return flushCount.get();
  }

  public int lastBatchSize() {
    return lastBatchSize;
  }

  public long lastFlushDurationMs() {
    return lastFlushDurationMs;
  }

  public void stop() {
    flushExecutor.shutdownNow();
  }

  private void flushEarlyIfNecessary() {
    // Must hold lock. The size threshold only seals a batch early; flushing still happens on the
    // single flush thread to keep submission serialized.
    if (workerHeartbeats.size() + appHeartbeats.size() >= batchSize) {
      flushExecutor.execute(this::flushSafely);
    }
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
    Map<String, ResourceProtos.WorkerHeartbeatRequest> drainedWorkers;
    Map<String, ResourceProtos.AppHeartbeatRequest> drainedApps;
    lock.lock();
    try {
      if (workerHeartbeats.isEmpty() && appHeartbeats.isEmpty()) {
        return;
      }
      drainedWorkers = workerHeartbeats;
      workerHeartbeats = new LinkedHashMap<>();
      drainedApps = appHeartbeats;
      appHeartbeats = new LinkedHashMap<>();
    } finally {
      lock.unlock();
    }

    ResourceRequest batchRequest =
        ResourceRequest.newBuilder()
            .setCmdType(Type.BatchHeartbeat)
            .setRequestId(MasterClient.genRequestId())
            .setBatchHeartbeatRequest(
                ResourceProtos.BatchHeartbeatRequest.newBuilder()
                    .addAllWorkerHeartbeats(drainedWorkers.values())
                    .addAllAppHeartbeats(drainedApps.values())
                    .build())
            .build();
    lastBatchSize = drainedWorkers.size() + drainedApps.size();
    long startNs = System.nanoTime();
    ratisServer.submitRequest(batchRequest);
    // Includes raft replication, majority fsync and the local apply, i.e. the full latency
    // of one batched raft write.
    lastFlushDurationMs = (System.nanoTime() - startNs) / 1000000;
    flushCount.incrementAndGet();
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Flushed aggregated heartbeats, {} worker heartbeats, {} app heartbeats.",
          drainedWorkers.size(),
          drainedApps.size());
    }
  }

  private static String workerKey(ResourceProtos.WorkerHeartbeatRequest heartbeat) {
    return heartbeat.getHost()
        + ":"
        + heartbeat.getRpcPort()
        + ":"
        + heartbeat.getPushPort()
        + ":"
        + heartbeat.getFetchPort()
        + ":"
        + heartbeat.getReplicatePort();
  }
}
