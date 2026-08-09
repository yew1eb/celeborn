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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.message.StatusCode;

public class LocationGroupSuiteJ {

  private PartitionLocation loc(int epoch, String host) {
    return new PartitionLocation(
        0, epoch, host, 1234, 1235, 1236, 1237, PartitionLocation.Mode.PRIMARY);
  }

  @Test
  public void testFastPathSingleLocation() {
    PartitionLocation loc = loc(0, "w1");
    LocationGroup group = new LocationGroup(loc);
    assertFalse(group.isInflated());
    assertSame(loc, group.currentFor(0));
    assertSame(loc, group.currentFor(7));
    assertSame(loc, group.latest());
    assertEquals(0, group.maxEpoch());
    assertTrue(group.hasUsable());
    assertSame(loc, group.anotherActiveFor(0, 3));
    assertNull(group.anotherActiveFor(0, 0));

    // Single-location revive update keeps thin-wrapper mode.
    PartitionLocation loc1 = loc(1, "w2");
    group.mergeAll(Collections.singletonList(loc1));
    assertFalse(group.isInflated());
    assertSame(loc1, group.currentFor(0));
    assertEquals(1, group.maxEpoch());
  }

  @Test
  public void testRetireSwitchesCurrent() {
    LocationGroup group = new LocationGroup(loc(0, "w1"));
    assertTrue(group.retire(0, StatusCode.SOFT_SPLIT));
    assertFalse(group.retire(0, StatusCode.SOFT_SPLIT));
    assertTrue(group.isInflated());
    // Soft-retired location keeps draining when it is the only one.
    assertEquals(0, group.currentFor(0).getEpoch());
    assertTrue(group.hasUsable());

    group.mergeAll(Arrays.asList(loc(0, "w1"), loc(1, "w2"), loc(2, "w3")));
    assertEquals(2, group.maxEpoch());
    assertEquals(3, group.activeCount());
    // mapId % K dispatches to non-retired locations, skipping retired epoch 0.
    assertEquals(1, group.currentFor(0).getEpoch());
    assertEquals(1, group.currentFor(1).getEpoch());
    assertEquals(2, group.currentFor(2).getEpoch());
    assertEquals(1, group.currentFor(3).getEpoch());
    assertEquals(2, group.latest().getEpoch());

    group.retire(1, StatusCode.HARD_SPLIT);
    // Epoch 1 is hard-retired: everyone falls to epoch 2.
    assertEquals(2, group.currentFor(0).getEpoch());
    assertEquals(2, group.currentFor(1).getEpoch());
    assertTrue(group.hasUsable());
  }

  @Test
  public void testMergeAllConvergesOutOfOrderEpochs() {
    LocationGroup group = new LocationGroup(loc(5, "w1"));
    // Full active set delivered out of order, including epochs not known locally.
    group.mergeAll(Arrays.asList(loc(3, "w3"), loc(7, "w7"), loc(1, "w1")));
    assertEquals(4, group.activeCount());
    assertEquals(7, group.maxEpoch());
    assertEquals(7, group.latest().getEpoch());

    // Locally retired epochs are never re-activated by the full set.
    group.retire(3, StatusCode.HARD_SPLIT);
    group.mergeAll(Arrays.asList(loc(3, "w3"), loc(8, "w8")));
    assertEquals(8, group.maxEpoch());
    for (int mapId = 0; mapId < 16; mapId++) {
      assertNotEquals(3, group.currentFor(mapId).getEpoch());
    }

    // mergeAll dedupes epochs already active.
    group.mergeAll(Arrays.asList(loc(7, "w7"), loc(8, "w8")));
    assertEquals(5, group.activeCount());
  }

  @Test
  public void testAllUnusable() {
    LocationGroup group = new LocationGroup(loc(0, "w1"));
    group.mergeAll(Arrays.asList(loc(0, "w1"), loc(1, "w2")));
    group.retire(0, StatusCode.HARD_SPLIT);
    assertTrue(group.hasUsable());
    group.retire(1, StatusCode.HARD_SPLIT);
    assertFalse(group.hasUsable());
    assertNull(group.currentFor(0));
    assertNull(group.anotherActiveFor(0, 1));
  }

  @Test
  public void testAnotherActiveFor() {
    LocationGroup group = new LocationGroup(loc(0, "w1"));
    group.mergeAll(Arrays.asList(loc(0, "w1"), loc(1, "w2")));
    assertEquals(1, group.anotherActiveFor(0, 0).getEpoch());
    assertEquals(0, group.anotherActiveFor(0, 1).getEpoch());
    // No other usable location when the other one is hard-retired.
    group.retire(1, StatusCode.HARD_SPLIT);
    assertNull(group.anotherActiveFor(0, 0));
    // Soft-retired other location is still usable for re-push.
    LocationGroup group2 = new LocationGroup(loc(0, "w1"));
    group2.mergeAll(Arrays.asList(loc(0, "w1"), loc(1, "w2")));
    group2.retire(1, StatusCode.SOFT_SPLIT);
    assertEquals(1, group2.anotherActiveFor(0, 0).getEpoch());
  }
}
