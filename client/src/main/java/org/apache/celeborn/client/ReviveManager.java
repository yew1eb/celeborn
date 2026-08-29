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

package org.apache.celeborn.client;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.ReviveRequest;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.util.ThreadUtils;

class ReviveManager {
  private static final Logger logger = LoggerFactory.getLogger(ReviveManager.class);

  private static final long WAIT_POLL_MS = 50;

  LinkedBlockingQueue<ReviveRequest> requestQueue = new LinkedBlockingQueue<>();
  private final int batchSize;
  private final long reviveWaitTimeMs;
  private final boolean adaptivePartitionWriteParallelismEnabled;
  ShuffleClientImpl shuffleClient;
  private final ScheduledExecutorService batchReviveRequestScheduler =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(
          "celeborn-client-lifecycle-manager-batch-revive-scheduler");

  public ReviveManager(ShuffleClientImpl shuffleClient, CelebornConf conf) {
    this.shuffleClient = shuffleClient;
    this.batchSize = conf.clientPushReviveBatchSize();
    this.reviveWaitTimeMs =
        conf.clientRpcRequestPartitionLocationAskTimeout().duration().toMillis();
    this.adaptivePartitionWriteParallelismEnabled =
        conf.clientShuffleAdaptivePartitionWriteParallelismEnabled();

    long interval = conf.clientPushReviveInterval();
    batchReviveRequestScheduler.scheduleWithFixedDelay(
        () -> {
          Map<Integer, Set<ReviveRequest>> shuffleMap = new HashMap<>();
          do {
            ArrayList<ReviveRequest> batchRequests = new ArrayList<>();
            requestQueue.drainTo(batchRequests, batchSize);
            for (ReviveRequest req : batchRequests) {
              Set<ReviveRequest> set =
                  shuffleMap.computeIfAbsent(req.shuffleId, id -> new HashSet<>());
              set.add(req);
            }
            for (Map.Entry<Integer, Set<ReviveRequest>> shuffleEntry : shuffleMap.entrySet()) {
              // Call reviveBatch for requests in the same (appId, shuffleId)
              int shuffleId = shuffleEntry.getKey();
              Set<ReviveRequest> requests = shuffleEntry.getValue();
              Set<Integer> mapIds = new HashSet<>();
              ArrayList<ReviveRequest> filteredRequests = new ArrayList<>();
              Map<Integer, ReviveRequest> requestsToSend = new HashMap<>();

              Map<Integer, PartitionLocationGroup> partitionMap =
                  shuffleClient.reducePartitionMap.get(shuffleId);
              // Insert request that is not MapperEnded and with the max epoch
              // into requestsToSend
              Iterator<ReviveRequest> iter = requests.iterator();
              // Adaptive parallelism only: one representative request per partition, used as the
              // mapId/attemptId donor when rebuilding retire reports below.
              Map<Integer, ReviveRequest> representativeRequests = new HashMap<>();
              while (iter.hasNext()) {
                ReviveRequest req = iter.next();
                if (shuffleClient.mapperEnded(shuffleId, req.mapId)
                    || (adaptivePartitionWriteParallelismEnabled
                        ? shuffleClient.hasWritableLocation(
                            partitionMap, req.partitionId, req.mapId)
                        : shuffleClient.newerPartitionLocationExists(
                            partitionMap, req.partitionId, req.epoch, false))) {
                  req.reviveStatus = StatusCode.SUCCESS.getValue();
                  if (adaptivePartitionWriteParallelismEnabled) {
                    mapIds.add(req.mapId);
                    representativeRequests.putIfAbsent(req.partitionId, req);
                  }
                } else {
                  filteredRequests.add(req);
                  mapIds.add(req.mapId);
                  if (adaptivePartitionWriteParallelismEnabled) {
                    representativeRequests.putIfAbsent(req.partitionId, req);
                  }
                  ReviveRequest current = requestsToSend.get(req.partitionId);
                  if (current == null || current.epoch < req.epoch) {
                    requestsToSend.put(req.partitionId, req);
                  }
                }
              }

              ArrayList<ReviveRequest> allToSend = new ArrayList<>(requestsToSend.values());
              if (adaptivePartitionWriteParallelismEnabled) {
                // Every retire report must reach the LifecycleManager (its active-set bookkeeping
                // depends on it), even when already satisfied locally. Reports are rebuilt from
                // the groups' outstanding sets at send time instead of being collected from the
                // queue: the queue version let a stuck scheduler pile every distinct retired
                // epoch of the backlog (thousands for one hot partition) into a single Revive
                // message, while the group view only holds retired epochs the LM has not
                // digested yet (still in the active list) — bounded by the active set size,
                // dropping stale reports, and automatically re-sending ones lost to an RPC
                // timeout (they stay outstanding until the LM's full-set reply evicts them).
                for (Map.Entry<Integer, ReviveRequest> entry : representativeRequests.entrySet()) {
                  int partitionId = entry.getKey();
                  ReviveRequest representative = entry.getValue();
                  PartitionLocationGroup group =
                      partitionMap == null ? null : partitionMap.get(partitionId);
                  if (group == null) {
                    continue;
                  }
                  // An epoch covered by a waiting (non-satisfied) request is reported by it.
                  ReviveRequest waiting = requestsToSend.get(partitionId);
                  int coveredEpoch = waiting == null ? -1 : waiting.epoch;
                  for (PartitionLocationGroup.OutstandingRetire retire :
                      group.outstandingRetires()) {
                    if (retire.location.getEpoch() != coveredEpoch) {
                      allToSend.add(
                          new ReviveRequest(
                              shuffleId,
                              representative.mapId,
                              representative.attemptId,
                              partitionId,
                              retire.location.getEpoch(),
                              retire.location,
                              retire.cause));
                    }
                  }
                }
              }
              if (!allToSend.isEmpty()) {
                // Call reviveBatch. Return null means Exception caught or
                // SHUFFLE_NOT_REGISTERED
                Map<Integer, Integer> results =
                    shuffleClient.reviveBatch(shuffleId, mapIds, allToSend);
                if (results == null) {
                  for (ReviveRequest req : filteredRequests) {
                    req.reviveStatus = StatusCode.REVIVE_FAILED.getValue();
                  }
                } else {
                  for (ReviveRequest req : filteredRequests) {
                    if (shuffleClient.mapperEnded(shuffleId, req.mapId)) {
                      req.reviveStatus = StatusCode.SUCCESS.getValue();
                    } else {
                      req.reviveStatus = results.get(req.partitionId);
                    }
                  }
                }
              }
            }
            // break the loop if remaining requests is less than half of
            // `celeborn.client.push.revive.batchSize`
          } while (requestQueue.size() > batchSize / 2);
        },
        interval,
        interval,
        TimeUnit.MILLISECONDS);
  }

  public void addRequest(ReviveRequest request) {
    shuffleClient.excludeWorkerByCause(request.cause, request.loc);
    // This sync is necessary to ensure the add action is atomic
    try {
      requestQueue.put(request);
    } catch (InterruptedException e) {
      logger.error("Exception when put into requests!", e);
    }
  }

  /**
   * Blocking revive for the all-locations-unwritable case of adaptive parallelism. Used only at the
   * pushOrMergeData entry, where the data thread must synchronously obtain a writable location (the
   * retry path re-enqueues instead, in the same shape as the merged path). Each round enqueues a
   * max-epoch request into the standard batched revive and waits until the partition is writable
   * again: writability is the only completion predicate — the wait wakes as soon as ANY source
   * makes the partition writable (this request's batch response, another mapper's revive response
   * merged into the group, or the scheduler's local satisfy), while {@code reviveStatus} only
   * signals failure/timeout of this round. The batch scheduler attaches the partition's outstanding
   * retire reports at send time (see {@link PartitionLocationGroup#outstandingRetires()}) and keeps
   * at most one in-flight request per partition. Bounded by {@code maxAttempts} rounds; each round
   * waits at most one requestPartition ask timeout.
   *
   * @return a writable location for {@code mapId}, or null when the mapper has ended or the
   *     attempts are exhausted (the LM's active-set bookkeeping keeps lagging this executor's
   *     retires, or the LM is genuinely unresponsive).
   */
  PartitionLocation reviveUntilWritable(
      int shuffleId, int mapId, int attemptId, int partitionId, int maxAttempts) {
    PartitionLocationGroup group = shuffleClient.locationGroup(shuffleId, partitionId);
    if (group == null) {
      return null;
    }
    PartitionLocation loc = group.currentFor(mapId);
    if (loc != null) {
      return loc;
    }
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      if (shuffleClient.mapperEnded(shuffleId, mapId)) {
        return null;
      }
      ReviveRequest req =
          new ReviveRequest(
              shuffleId,
              mapId,
              attemptId,
              partitionId,
              group.maxEpoch(),
              group.latest(),
              StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_PRIMARY);
      // addRequest also excludes the worker by cause. A timed-out round leaves the request
      // queued; the scheduler dedups per partition before sending, so a re-enqueue is harmless.
      addRequest(req);
      long dueTime = System.currentTimeMillis() + reviveWaitTimeMs;
      while (req.reviveStatus == StatusCode.REVIVE_INITIALIZED.getValue()
          && group.currentFor(mapId) == null
          && System.currentTimeMillis() < dueTime) {
        try {
          Thread.sleep(WAIT_POLL_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      }
      loc = group.currentFor(mapId);
      if (loc != null) {
        logger.debug(
            "Blocking revive for shuffle {} partition {} succeeded at attempt {}/{}, writing to epoch {}@{}.",
            shuffleId,
            partitionId,
            attempt,
            maxAttempts,
            loc.getEpoch(),
            loc.hostAndPushPort());
        return loc;
      }
      logger.debug(
          "Blocking revive for shuffle {} partition {}: attempt {}/{} left nothing writable, {}.",
          shuffleId,
          partitionId,
          attempt,
          maxAttempts,
          req.reviveStatus == StatusCode.REVIVE_INITIALIZED.getValue()
              ? "timed out waiting for the batch revive"
              : "revive status " + StatusCode.fromValue(req.reviveStatus));
    }
    return group.currentFor(mapId);
  }

  public void close() {
    ThreadUtils.shutdown(batchReviveRequestScheduler);
  }
}
