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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.ReviveRequest;
import org.apache.celeborn.common.protocol.message.StatusCode;

public class ReviveManagerSuiteJ {

  private static final int SHUFFLE_ID = 0;
  private static final int PARTITION_ID = 0;
  private static final int MAP_ID = 7;
  private static final int ATTEMPT_ID = 0;
  private static final Map<Integer, Integer> SUCCESS =
      Collections.singletonMap(PARTITION_ID, (int) StatusCode.SUCCESS.getValue());

  private PartitionLocation loc(int epoch, String host) {
    return new PartitionLocation(
        PARTITION_ID, epoch, host, 1234, 1235, 1236, 1237, PartitionLocation.Mode.PRIMARY);
  }

  private ShuffleClientImpl mockClient(PartitionLocationGroup group) {
    ShuffleClientImpl client = mock(ShuffleClientImpl.class);
    when(client.locationGroup(SHUFFLE_ID, PARTITION_ID)).thenReturn(group);
    return client;
  }

  private ReviveManager newManager(ShuffleClientImpl client) {
    CelebornConf conf = new CelebornConf();
    conf.set(
        CelebornConf.CLIENT_SHUFFLE_ADAPTIVE_PARTITION_WRITE_PARALLELISM_ENABLED().key(), "true");
    return new ReviveManager(client, conf);
  }

  @Test
  public void testReviveUntilWritableFastPathSkipsRpc() {
    PartitionLocation writable = loc(0, "w1");
    ShuffleClientImpl client = mockClient(new PartitionLocationGroup(writable));
    ReviveManager manager = newManager(client);
    try {
      assertSame(
          writable, manager.reviveUntilWritable(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID));
      verify(client, never()).reviveBatch(anyInt(), any(), any());
    } finally {
      manager.close();
    }
  }

  @Test
  public void testReviveUntilWritableRetriesAndCarriesRetireReports() {
    // Both active locations hard-retired: nothing writable for any mapId.
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2")), true);
    group.retire(0, StatusCode.HARD_SPLIT);
    group.retire(1, StatusCode.HARD_SPLIT);
    ShuffleClientImpl client = mockClient(group);

    ArgumentCaptor<Set<Integer>> mapIdsCaptor = ArgumentCaptor.forClass(Set.class);
    ArgumentCaptor<List<ReviveRequest>> requestsCaptor = ArgumentCaptor.forClass(List.class);
    PartitionLocation revived = loc(2, "w3");
    when(client.reviveBatch(eq(SHUFFLE_ID), mapIdsCaptor.capture(), requestsCaptor.capture()))
        // First round: SUCCESS but the reply brings nothing writable (LM's active set lags).
        .thenAnswer(invocation -> new java.util.HashMap<>(SUCCESS))
        // Second round: the LM digests the reports and allocates a replacement.
        .thenAnswer(
            invocation -> {
              group.mergeActiveLocations(Collections.singletonList(revived), false);
              return new java.util.HashMap<>(SUCCESS);
            });

    ReviveManager manager = newManager(client);
    try {
      assertSame(
          revived, manager.reviveUntilWritable(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID));
      verify(client, times(2)).reviveBatch(eq(SHUFFLE_ID), any(), any());

      // Every round carries the primary request (max epoch) plus every outstanding retire
      // report; without them the LM's gap-based allocation sees gap == 0 and re-replies the
      // retired epochs.
      for (List<ReviveRequest> requests : requestsCaptor.getAllValues()) {
        Set<Integer> epochs = new HashSet<>();
        for (ReviveRequest req : requests) {
          epochs.add(req.epoch);
        }
        assertEquals(new HashSet<>(Arrays.asList(0, 1)), epochs);
        assertEquals(
            StatusCode.HARD_SPLIT,
            requests.stream().filter(r -> r.epoch == 0).findFirst().get().cause);
      }
      for (Set<Integer> mapIds : mapIdsCaptor.getAllValues()) {
        assertEquals(Collections.singleton(MAP_ID), mapIds);
      }
    } finally {
      manager.close();
    }
  }

  @Test
  public void testReviveUntilWritableAttemptsExhausted() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.retire(0, StatusCode.HARD_SPLIT);
    ShuffleClientImpl client = mockClient(group);
    // SUCCESS replies whose locations are all retired locally: retry until the attempt budget
    // (3) is exhausted, then give up instead of failing immediately.
    when(client.reviveBatch(anyInt(), any(), any()))
        .thenAnswer(invocation -> new java.util.HashMap<>(SUCCESS));

    ReviveManager manager = newManager(client);
    try {
      assertNull(manager.reviveUntilWritable(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID));
      verify(client, times(3)).reviveBatch(anyInt(), any(), any());
    } finally {
      manager.close();
    }
  }

  @Test
  public void testReviveUntilWritableRpcFailureRetries() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.retire(0, StatusCode.HARD_SPLIT);
    ShuffleClientImpl client = mockClient(group);
    when(client.reviveBatch(anyInt(), any(), any())).thenReturn(null);

    ReviveManager manager = newManager(client);
    try {
      assertNull(manager.reviveUntilWritable(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID));
      verify(client, times(3)).reviveBatch(anyInt(), any(), any());
    } finally {
      manager.close();
    }
  }

  @Test
  public void testReviveUntilWritableSingleFlight() throws Exception {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.retire(0, StatusCode.HARD_SPLIT);
    ShuffleClientImpl client = mockClient(group);

    CountDownLatch rpcEntered = new CountDownLatch(1);
    CountDownLatch rpcRelease = new CountDownLatch(1);
    AtomicInteger rpcCalls = new AtomicInteger();
    PartitionLocation revived = loc(1, "w2");
    when(client.reviveBatch(anyInt(), any(), any()))
        .thenAnswer(
            invocation -> {
              rpcCalls.incrementAndGet();
              rpcEntered.countDown();
              assertTrue(rpcRelease.await(10, TimeUnit.SECONDS));
              group.mergeActiveLocations(Collections.singletonList(revived), false);
              return new java.util.HashMap<>(SUCCESS);
            });

    ReviveManager manager = newManager(client);
    try {
      List<FutureTask<PartitionLocation>> tasks = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        FutureTask<PartitionLocation> task =
            new FutureTask<>(
                () -> manager.reviveUntilWritable(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID));
        tasks.add(task);
      }
      new Thread(tasks.get(0)).start();
      // Wait until the first thread holds the single-flight lock inside the RPC.
      assertTrue(rpcEntered.await(10, TimeUnit.SECONDS));
      new Thread(tasks.get(1)).start();
      // Give the second thread time to block on the lock, then release the RPC.
      Thread.sleep(200);
      rpcRelease.countDown();

      // Both threads get the revived location, but only one RPC was ever in flight: the
      // second thread re-checks writability after the lock wait and skips its own RPC.
      for (FutureTask<PartitionLocation> task : tasks) {
        assertSame(revived, task.get(10, TimeUnit.SECONDS));
      }
      assertEquals(1, rpcCalls.get());
    } finally {
      manager.close();
    }
  }

  @Test
  public void testBatchRetireReportsRebuiltFromOutstandingRetires() throws Exception {
    // Group: epochs 0 and 1 hard-retired, epoch 2 writable — every queued request below is
    // locally satisfied, so the batch should carry retire reports only, rebuilt from the
    // group's outstanding set: deduped, and free of stale epochs the group no longer holds.
    ShuffleClientImpl client =
        spy(
            new ShuffleClientImpl(
                "app-revive-manager-test", new CelebornConf(), new UserIdentifier("mock", "mock")));
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2"), loc(2, "w3")), true);
    group.retire(0, StatusCode.HARD_SPLIT);
    group.retire(1, StatusCode.HARD_SPLIT);
    ConcurrentHashMap<Integer, PartitionLocationGroup> partitionMap = new ConcurrentHashMap<>();
    partitionMap.put(PARTITION_ID, group);
    client.reducePartitionMap.put(SHUFFLE_ID, partitionMap);

    CountDownLatch sent = new CountDownLatch(1);
    AtomicReference<List<ReviveRequest>> sentRequests = new AtomicReference<>();
    doAnswer(
            invocation -> {
              sentRequests.set(new ArrayList<>(invocation.getArgument(2)));
              sent.countDown();
              return new java.util.HashMap<>(SUCCESS);
            })
        .when(client)
        .reviveBatch(anyInt(), any(), any());

    ReviveManager manager = newManager(client);
    try {
      // Duplicate reports of epoch 0 from three mappers, one report of epoch 1, and one report
      // of epoch 99 which the group has never seen (digested and evicted long ago).
      for (int mapId : Arrays.asList(1, 2, 3)) {
        manager.addRequest(
            new ReviveRequest(
                SHUFFLE_ID, mapId, 0, PARTITION_ID, 0, loc(0, "w1"), StatusCode.HARD_SPLIT));
      }
      manager.addRequest(
          new ReviveRequest(
              SHUFFLE_ID, 4, 0, PARTITION_ID, 1, loc(1, "w2"), StatusCode.HARD_SPLIT));
      manager.addRequest(
          new ReviveRequest(
              SHUFFLE_ID, 5, 0, PARTITION_ID, 99, loc(99, "wX"), StatusCode.HARD_SPLIT));

      assertTrue(sent.await(10, TimeUnit.SECONDS));
      Set<Integer> epochs =
          sentRequests.get().stream().map(r -> r.epoch).collect(Collectors.toSet());
      // Exactly the outstanding set {0, 1}: queue duplicates deduped, stale epoch 99 dropped.
      assertEquals(new HashSet<>(Arrays.asList(0, 1)), epochs);
      for (ReviveRequest req : sentRequests.get()) {
        assertEquals(StatusCode.HARD_SPLIT, req.cause);
      }
    } finally {
      manager.close();
    }
  }
}
