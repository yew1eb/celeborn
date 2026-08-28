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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
  private static final int MAX_ATTEMPTS = 2;
  private static final Map<Integer, Integer> SUCCESS =
      Collections.singletonMap(PARTITION_ID, (int) StatusCode.SUCCESS.getValue());

  private PartitionLocation loc(int epoch, String host) {
    return new PartitionLocation(
        PARTITION_ID, epoch, host, 1234, 1235, 1236, 1237, PartitionLocation.Mode.PRIMARY);
  }

  // A real ShuffleClientImpl (spied) so the batch scheduler can read real fields
  // (reducePartitionMap, mapperEndMap); a plain mock has null fields and the scheduler
  // thread would die on an NPE, silently cancelling all future ticks.
  private ShuffleClientImpl spyClient(PartitionLocationGroup group) {
    ShuffleClientImpl client =
        spy(
            new ShuffleClientImpl(
                "app-revive-manager-test", new CelebornConf(), new UserIdentifier("mock", "mock")));
    ConcurrentHashMap<Integer, PartitionLocationGroup> partitionMap = new ConcurrentHashMap<>();
    if (group != null) {
      partitionMap.put(PARTITION_ID, group);
    }
    client.reducePartitionMap.put(SHUFFLE_ID, partitionMap);
    return client;
  }

  private ReviveManager newManager(ShuffleClientImpl client) {
    CelebornConf conf = new CelebornConf();
    conf.set(
        CelebornConf.CLIENT_SHUFFLE_ADAPTIVE_PARTITION_WRITE_PARALLELISM_ENABLED().key(), "true");
    return new ReviveManager(client, conf);
  }

  @Test
  public void testReviveUntilWritableFastPathSkipsQueue() {
    PartitionLocation writable = loc(0, "w1");
    ShuffleClientImpl client = spyClient(new PartitionLocationGroup(writable));
    ReviveManager manager = newManager(client);
    try {
      assertSame(
          writable,
          manager.reviveUntilWritable(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID, MAX_ATTEMPTS));
      assertTrue(manager.requestQueue.isEmpty());
      verify(client, never()).reviveBatch(anyInt(), any(), any());
    } finally {
      manager.close();
    }
  }

  @Test
  public void testReviveUntilWritableSatisfiedByBatchRevive() throws Exception {
    // Both active locations hard-retired: nothing writable for any mapId. The blocking revive
    // enqueues; the batch scheduler sends (with the retire reports attached) and the reply
    // brings a new writable location.
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2")), true);
    group.retire(0, StatusCode.HARD_SPLIT);
    group.retire(1, StatusCode.HARD_SPLIT);
    ShuffleClientImpl client = spyClient(group);

    CountDownLatch sent = new CountDownLatch(1);
    AtomicReference<List<ReviveRequest>> sentRequests = new AtomicReference<>();
    PartitionLocation revived = loc(2, "w3");
    doAnswer(
            invocation -> {
              sentRequests.set(new ArrayList<>(invocation.getArgument(2)));
              group.mergeActiveLocations(Collections.singletonList(revived), false);
              sent.countDown();
              return new java.util.HashMap<>(SUCCESS);
            })
        .when(client)
        .reviveBatch(anyInt(), any(), any());

    ReviveManager manager = newManager(client);
    try {
      assertSame(
          revived,
          manager.reviveUntilWritable(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID, MAX_ATTEMPTS));
      assertTrue(sent.await(10, TimeUnit.SECONDS));
      // The send carried the max-epoch primary request plus the outstanding retire report.
      Set<Integer> epochs =
          sentRequests.get().stream().map(r -> r.epoch).collect(Collectors.toSet());
      assertEquals(new HashSet<>(Arrays.asList(0, 1)), epochs);
    } finally {
      manager.close();
    }
  }

  @Test
  public void testReviveUntilWritableAttemptsExhausted() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.retire(0, StatusCode.HARD_SPLIT);
    ShuffleClientImpl client = spyClient(group);
    // SUCCESS replies whose locations are all retired locally: retry until the attempt budget
    // is exhausted, then give up instead of failing immediately.
    doAnswer(invocation -> new java.util.HashMap<>(SUCCESS))
        .when(client)
        .reviveBatch(anyInt(), any(), any());

    ReviveManager manager = newManager(client);
    try {
      assertNull(
          manager.reviveUntilWritable(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID, MAX_ATTEMPTS));
      verify(client, times(MAX_ATTEMPTS)).reviveBatch(anyInt(), any(), any());
    } finally {
      manager.close();
    }
  }

  @Test
  public void testReviveUntilWritableRpcFailureRetries() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.retire(0, StatusCode.HARD_SPLIT);
    ShuffleClientImpl client = spyClient(group);
    doReturn(null).when(client).reviveBatch(anyInt(), any(), any());

    ReviveManager manager = newManager(client);
    try {
      assertNull(
          manager.reviveUntilWritable(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID, MAX_ATTEMPTS));
      verify(client, times(MAX_ATTEMPTS)).reviveBatch(anyInt(), any(), any());
    } finally {
      manager.close();
    }
  }

  @Test
  public void testReviveUntilWritableMapperEndedSkipsQueue() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.retire(0, StatusCode.HARD_SPLIT);
    ShuffleClientImpl client = spyClient(group);
    client
        .mapperEndMap
        .computeIfAbsent(SHUFFLE_ID, id -> ConcurrentHashMap.newKeySet())
        .add(MAP_ID);

    ReviveManager manager = newManager(client);
    try {
      assertNull(
          manager.reviveUntilWritable(SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID, MAX_ATTEMPTS));
      assertTrue(manager.requestQueue.isEmpty());
      verify(client, never()).reviveBatch(anyInt(), any(), any());
    } finally {
      manager.close();
    }
  }

  @Test
  public void testReviveUntilWritableConcurrentWaitersShareOneBatch() throws Exception {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.retire(0, StatusCode.HARD_SPLIT);
    ShuffleClientImpl client = spyClient(group);

    CountDownLatch rpcEntered = new CountDownLatch(1);
    CountDownLatch rpcRelease = new CountDownLatch(1);
    AtomicInteger rpcCalls = new AtomicInteger();
    PartitionLocation revived = loc(1, "w2");
    doAnswer(
            invocation -> {
              rpcCalls.incrementAndGet();
              rpcEntered.countDown();
              assertTrue(rpcRelease.await(10, TimeUnit.SECONDS));
              group.mergeActiveLocations(Collections.singletonList(revived), false);
              return new java.util.HashMap<>(SUCCESS);
            })
        .when(client)
        .reviveBatch(anyInt(), any(), any());

    ReviveManager manager = newManager(client);
    try {
      List<FutureTask<PartitionLocation>> tasks = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        FutureTask<PartitionLocation> task =
            new FutureTask<>(
                () ->
                    manager.reviveUntilWritable(
                        SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID, MAX_ATTEMPTS));
        tasks.add(task);
      }
      new Thread(tasks.get(0)).start();
      new Thread(tasks.get(1)).start();
      // Wait until the batch RPC is in flight, then release it. Both waiters either had their
      // requests deduped into this one batch, or wake on the writability merge itself — either
      // way no second RPC is sent.
      assertTrue(rpcEntered.await(10, TimeUnit.SECONDS));
      rpcRelease.countDown();

      for (FutureTask<PartitionLocation> task : tasks) {
        assertSame(revived, task.get(10, TimeUnit.SECONDS));
      }
      assertEquals(1, rpcCalls.get());
    } finally {
      manager.close();
    }
  }

  @Test
  public void testReviveUntilWritableWakesOnForeignMerge() throws Exception {
    // The batch RPC is stuck (LM unresponsive), but another thread's revive response merges a
    // writable location into the group: the wait must wake on writability alone, not on its own
    // request's status.
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.retire(0, StatusCode.HARD_SPLIT);
    ShuffleClientImpl client = spyClient(group);

    CountDownLatch rpcEntered = new CountDownLatch(1);
    CountDownLatch rpcRelease = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              rpcEntered.countDown();
              assertTrue(rpcRelease.await(10, TimeUnit.SECONDS));
              return new java.util.HashMap<>(SUCCESS);
            })
        .when(client)
        .reviveBatch(anyInt(), any(), any());

    ReviveManager manager = newManager(client);
    try {
      FutureTask<PartitionLocation> task =
          new FutureTask<>(
              () ->
                  manager.reviveUntilWritable(
                      SHUFFLE_ID, MAP_ID, ATTEMPT_ID, PARTITION_ID, MAX_ATTEMPTS));
      new Thread(task).start();
      // The request was sent and the RPC is still blocked.
      assertTrue(rpcEntered.await(10, TimeUnit.SECONDS));
      PartitionLocation revived = loc(1, "w2");
      group.mergeActiveLocations(Collections.singletonList(revived), false);
      // Returns without the RPC ever completing.
      assertSame(revived, task.get(10, TimeUnit.SECONDS));
      rpcRelease.countDown();
    } finally {
      manager.close();
    }
  }

  @Test
  public void testBatchRetireReportsRebuiltFromOutstandingRetires() throws Exception {
    // Group: epochs 0 and 1 hard-retired, epoch 2 writable — every queued request below is
    // locally satisfied, so the batch should carry retire reports only, rebuilt from the
    // group's outstanding set: deduped, and free of stale epochs the group no longer holds.
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2"), loc(2, "w3")), true);
    group.retire(0, StatusCode.HARD_SPLIT);
    group.retire(1, StatusCode.HARD_SPLIT);
    ShuffleClientImpl client = spyClient(group);

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
