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

package org.apache.celeborn.client.write;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.protocol.PartitionLocation;

/** Unit tests for {@link WriteLocationTracker}. */
public class WriteLocationTrackerSuiteJ {

  private static PartitionLocation loc(int partitionId, int epoch) {
    return new PartitionLocation(
        partitionId, epoch, "host" + epoch, 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY);
  }

  @Test
  public void testDisabledSelectForMapIdFallsBack() {
    WriteLocationTracker tracker = new WriteLocationTracker(false);
    PartitionLocation fallback = loc(1, 5);
    Assert.assertEquals(
        fallback, tracker.selectForMapId(1, 1, 7, fallback));
    Assert.assertFalse(tracker.hasNewer(1, 1, 0));
  }

  @Test
  public void testSeedOnRegisterDoesNotCreateSiblingEntries() {
    // Sparse: seedOnRegister only fills singleMap; siblingsMap must stay empty even for many
    // partitions (memory bounded for large partition counts).
    WriteLocationTracker tracker = new WriteLocationTracker(true);
    PartitionLocation[] locs = new PartitionLocation[100];
    for (int i = 0; i < 100; i++) {
      locs[i] = loc(i, 0);
    }
    tracker.seedOnRegister(1, locs);
    Assert.assertEquals(0, tracker.siblingsEntryCount(1));
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(loc(i, 0), tracker.getSingle(1, i));
    }
  }

  @Test
  public void testUpdateOnReviveSparsePutAndDegrade() {
    WriteLocationTracker tracker = new WriteLocationTracker(true);
    // Single location revive: no sibling entry (degrade / stay sparse).
    tracker.updateOnRevive(1, 1, Arrays.asList(loc(1, 1)));
    Assert.assertEquals(0, tracker.siblingsEntryCount(1));
    Assert.assertEquals(loc(1, 1), tracker.getSingle(1, 1));

    // Multi-location revive: sibling entry created.
    List<PartitionLocation> multi = Arrays.asList(loc(1, 1), loc(1, 2), loc(1, 3));
    tracker.updateOnRevive(1, 1, multi);
    Assert.assertEquals(1, tracker.siblingsEntryCount(1));
    Assert.assertEquals(loc(1, 1), tracker.getSingle(1, 1));

    // Subsequent single-location revive degrades back to single-value (sparse).
    tracker.updateOnRevive(1, 1, Arrays.asList(loc(1, 4)));
    Assert.assertEquals(0, tracker.siblingsEntryCount(1));
    Assert.assertEquals(loc(1, 4), tracker.getSingle(1, 1));
  }

  @Test
  public void testSelectForMapIdRoutesByMapIdStably() {
    WriteLocationTracker tracker = new WriteLocationTracker(true);
    List<PartitionLocation> multi = Arrays.asList(loc(1, 1), loc(1, 2), loc(1, 3));
    tracker.updateOnRevive(1, 1, multi);
    // Same mapId always routes to the same sibling (deterministic, batch order preserved).
    PartitionLocation a = tracker.selectForMapId(1, 1, 5, loc(1, 99));
    PartitionLocation b = tracker.selectForMapId(1, 1, 5, loc(1, 99));
    Assert.assertEquals(a, b);
    Assert.assertTrue(multi.contains(a));
    // No sibling set: falls back.
    Assert.assertEquals(loc(1, 99), tracker.selectForMapId(1, 2, 5, loc(1, 99)));
  }

  @Test
  public void testHasNewerChecksSiblingSetWhenEnabled() {
    WriteLocationTracker tracker = new WriteLocationTracker(true);
    tracker.updateOnRevive(1, 1, Arrays.asList(loc(1, 1), loc(1, 5)));
    Assert.assertTrue(tracker.hasNewer(1, 1, 4)); // sibling epoch 5 > 4
    Assert.assertFalse(tracker.hasNewer(1, 1, 5));
    // No sibling set: fall back to single-value (epoch 1).
    tracker.updateOnRevive(1, 2, Arrays.asList(loc(2, 3)));
    Assert.assertTrue(tracker.hasNewer(1, 2, 2)); // single epoch 3 > 2
    Assert.assertFalse(tracker.hasNewer(1, 2, 3));
  }

  @Test
  public void testExcludeSiblingDegradesWhenListEmpties() {
    WriteLocationTracker tracker = new WriteLocationTracker(true);
    tracker.updateOnRevive(1, 1, Arrays.asList(loc(1, 1), loc(1, 2)));
    Assert.assertEquals(1, tracker.siblingsEntryCount(1));
    tracker.excludeSibling(1, 1, loc(1, 1)); // remove one
    Assert.assertEquals(1, tracker.siblingsEntryCount(1)); // still 1 sibling left
    tracker.excludeSibling(1, 1, loc(1, 2)); // remove the last -> degrade
    Assert.assertEquals(0, tracker.siblingsEntryCount(1));
  }

  @Test
  public void testCleanupClearsBoth() {
    WriteLocationTracker tracker = new WriteLocationTracker(true);
    tracker.seedOnRegister(1, new PartitionLocation[] {loc(1, 0), loc(2, 0)});
    tracker.updateOnRevive(1, 1, Arrays.asList(loc(1, 1), loc(1, 2)));
    tracker.cleanup(1);
    Assert.assertNull(tracker.getSingle(1, 1));
    Assert.assertEquals(0, tracker.siblingsEntryCount(1));
  }

  @Test
  public void testConcurrentSelectAndExclude() throws Exception {
    // selectForMapId (read) and excludeSibling (write) concurrently must not throw.
    WriteLocationTracker tracker = new WriteLocationTracker(true);
    List<PartitionLocation> multi = new ArrayList<>();
    for (int e = 1; e <= 8; e++) {
      multi.add(loc(1, e));
    }
    tracker.updateOnRevive(1, 1, multi);
    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> err = new AtomicReference<>(null);
    for (int i = 0; i < threads; i++) {
      final int mapId = i;
      pool.submit(
          () -> {
            try {
              latch.await();
              for (int j = 0; j < 1000; j++) {
                tracker.selectForMapId(1, 1, mapId, loc(1, 0));
                if (j % 100 == 0) {
                  tracker.excludeSibling(1, 1, loc(1, 1 + (j % 8)));
                }
              }
            } catch (Throwable t) {
              err.compareAndSet(null, t);
            }
          });
    }
    latch.countDown();
    pool.shutdown();
    Assert.assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
    Assert.assertNull("concurrent read/write threw", err.get());
  }

  @Test
  public void testGetSingleMapIsConsistentView() {
    // getSingleMap returns the same backing map across calls (used by getPartitionLocation/DataPushQueue).
    WriteLocationTracker tracker = new WriteLocationTracker(true);
    ConcurrentHashMap<Integer, PartitionLocation> m1 = tracker.getSingleMap(1);
    ConcurrentHashMap<Integer, PartitionLocation> m2 = tracker.getSingleMap(1);
    Assert.assertSame(m1, m2);
    Assert.assertTrue(m1.isEmpty());
    tracker.seedOnRegister(1, new PartitionLocation[] {loc(1, 0)});
    Assert.assertEquals(loc(1, 0), m1.get(1));
  }
}
