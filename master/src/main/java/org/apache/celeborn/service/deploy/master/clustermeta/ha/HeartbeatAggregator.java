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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * heartbeats under a short lock; a single flush thread swaps out the pending buffers under the same
 * lock and submits the batch to raft outside the lock.
 *
 * <p>Flush semantics (borrowing TiKV's cmd-batch): at each flush tick, whatever is pending is
 * drained as-is; the size threshold only seals a batch early, the aggregator never waits to fill a
 * batch.
 *
 * <p>Heartbeats are buffered as-is (not deduplicated). Per-key dedup would be dormant in steady
 * state because the heartbeat intervals (worker 30s, app 10s) are much larger than the default 1s
 * flush window, and even when retries make a key repeat within a window the final meta state is
 * unchanged (last apply wins). The pending count is bounded in practice by the heartbeat arrival
 * rate times the flush interval and is monitored by {@code HeartbeatBatchPendingCount}; a stalled
 * flush is surfaced via that gauge rather than backpressure, since the reply path never waits for
 * raft. A batch that fails to submit (e.g. leadership change) is dropped and logged; heartbeats are
 * self-healing on the next interval.
 */
public class HeartbeatAggregator {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatAggregator.class);

  /** Minimum flush interval, guards against a misconfigured 0/negative value busy-looping. */
  private static final long MIN_BATCH_INTERVAL_MS = 100L;

  private final HARaftServer ratisServer;
  private final int batchSize;

  private final ReentrantLock lock = new ReentrantLock();
  private ArrayList<ResourceProtos.WorkerHeartbeatRequest> workerHeartbeats = new ArrayList<>();
  private ArrayList<ResourceProtos.AppHeartbeatRequest> appHeartbeats = new ArrayList<>();

  /**
   * Guards the early-flush path so a burst that crosses the size threshold does not queue a flush
   * task per offer. CAS-set when scheduling, cleared at the start of {@link #flush()}; this caps
   * pending early-flush tasks at one while still allowing the next burst to re-arm promptly.
   */
  private final AtomicBoolean flushScheduled = new AtomicBoolean(false);

  private final ScheduledExecutorService flushExecutor;
  private final AtomicLong flushCount = new AtomicLong(0);
  private volatile int lastBatchSize = 0;
  private volatile long lastFlushDurationMs = 0;

  public HeartbeatAggregator(HARaftServer ratisServer, CelebornConf conf) {
    this.ratisServer = ratisServer;
    this.batchSize = conf.masterHaHeartbeatBatchSize();
    long batchIntervalMs = conf.masterHaHeartbeatBatchIntervalMs();
    if (batchIntervalMs < MIN_BATCH_INTERVAL_MS) {
      LOG.warn(
          "celeborn.master.ha.heartbeat.batch.interval {} ms is below the minimum {} ms; "
              + "clamping to the minimum to avoid busy-looping the flush thread.",
          batchIntervalMs,
          MIN_BATCH_INTERVAL_MS);
      batchIntervalMs = MIN_BATCH_INTERVAL_MS;
    }
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
      workerHeartbeats.add(heartbeat);
      flushEarlyIfNecessary();
    } finally {
      lock.unlock();
    }
  }

  public void offerAppHeartbeat(ResourceProtos.AppHeartbeatRequest heartbeat) {
    lock.lock();
    try {
      appHeartbeats.add(heartbeat);
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
    // single flush thread to keep submission serialized. The CAS guard caps pending early-flush
    // tasks at one: without it a burst crossing the threshold would queue a task per offer.
    if (workerHeartbeats.size() + appHeartbeats.size() >= batchSize
        && flushScheduled.compareAndSet(false, true)) {
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
    List<ResourceProtos.WorkerHeartbeatRequest> drainedWorkers;
    List<ResourceProtos.AppHeartbeatRequest> drainedApps;
    lock.lock();
    try {
      // Clear the early-flush re-arm guard first so the next burst that crosses the threshold can
      // schedule again, even when this invocation turns out to be a no-op (empty window).
      flushScheduled.set(false);
      if (workerHeartbeats.isEmpty() && appHeartbeats.isEmpty()) {
        return;
      }
      drainedWorkers = workerHeartbeats;
      workerHeartbeats = new ArrayList<>();
      drainedApps = appHeartbeats;
      appHeartbeats = new ArrayList<>();
    } finally {
      lock.unlock();
    }

    // Standby (non-leader) masters also run a flush thread. Pending heartbeats drained here are
    // not committable (this master is not the leader) and are renewable (re-sent next interval),
    // so discard them instead of submitting a request that would fail with NotLeader. This keeps
    // the pending gauge from growing on standbys and avoids NotLeader exception log noise.
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
}
