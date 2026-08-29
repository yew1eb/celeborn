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
import org.apache.celeborn.common.protocol.ReviveRequest;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.util.ThreadUtils;

class ReviveManager {
  private static final Logger logger = LoggerFactory.getLogger(ReviveManager.class);

  LinkedBlockingQueue<ReviveRequest> requestQueue = new LinkedBlockingQueue<>();
  private final int batchSize;
  private final boolean adaptivePartitionWriteParallelismEnabled;
  ShuffleClientImpl shuffleClient;
  private final ScheduledExecutorService batchReviveRequestScheduler =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(
          "celeborn-client-lifecycle-manager-batch-revive-scheduler");

  public ReviveManager(ShuffleClientImpl shuffleClient, CelebornConf conf) {
    this.shuffleClient = shuffleClient;
    this.batchSize = conf.clientPushReviveBatchSize();
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

  public void close() {
    ThreadUtils.shutdown(batchReviveRequestScheduler);
  }
}
