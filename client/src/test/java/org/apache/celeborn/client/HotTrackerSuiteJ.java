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

import org.junit.Test;

public class HotTrackerSuiteJ {

  private static final long WINDOW_MS = 60000;
  private static final int MAX_LOCATIONS = 4;

  private LocationGroup.HotTracker newTracker() {
    return new LocationGroup.HotTracker(WINDOW_MS, MAX_LOCATIONS);
  }

  @Test
  public void testBoostWhenFillsFasterThanWindow() {
    LocationGroup.HotTracker tracker = newTracker();
    tracker.onEpochLearned(0, 0);
    // fillTime 30s < 60s window => hot, desired +1
    assertEquals(2, tracker.onSoftSplit(0, 30000));
  }

  @Test
  public void testNoBoostWhenFillsSlowerThanWindow() {
    LocationGroup.HotTracker tracker = newTracker();
    tracker.onEpochLearned(0, 0);
    // Normal file rolling: fillTime 90s > 60s window => not hot
    assertEquals(1, tracker.onSoftSplit(0, 90000));
  }

  @Test
  public void testNoBoostWhenEpochLearnTimeUnknown() {
    LocationGroup.HotTracker tracker = newTracker();
    assertEquals(1, tracker.onSoftSplit(0, 1000));
  }

  @Test
  public void testSameEpochSplitReportedOnlyOnce() {
    LocationGroup.HotTracker tracker = newTracker();
    tracker.onEpochLearned(0, 0);
    assertEquals(2, tracker.onSoftSplit(0, 30000));
    // Repeated SOFT_SPLIT notifications of the same epoch (before the client finishes
    // switching) must not boost again.
    assertEquals(2, tracker.onSoftSplit(0, 31000));
    assertEquals(2, tracker.onSoftSplit(0, 32000));
  }

  @Test
  public void testDebounceOneBoostPerWindow() {
    LocationGroup.HotTracker tracker = newTracker();
    tracker.onEpochLearned(0, 0);
    // Boost at t=30s.
    assertEquals(2, tracker.onSoftSplit(0, 30000));
    // Epoch 1 learned at t=30s and filled at t=50s: hot, but within the debounce window.
    tracker.onEpochLearned(1, 30000);
    assertEquals(2, tracker.onSoftSplit(1, 50000));
    // Epoch 2 learned at t=50s and filled at t=95s: hot and past the debounce window.
    tracker.onEpochLearned(2, 50000);
    assertEquals(3, tracker.onSoftSplit(2, 95000));
  }

  @Test
  public void testMaxLocationsCap() {
    LocationGroup.HotTracker tracker = newTracker();
    for (int epoch = 0; epoch < 10; epoch++) {
      long learnTime = epoch * WINDOW_MS;
      tracker.onEpochLearned(epoch, learnTime);
      tracker.onSoftSplit(epoch, learnTime + WINDOW_MS / 2);
    }
    // 10 hot epochs but capped at MAX_LOCATIONS.
    assertEquals(MAX_LOCATIONS, tracker.desired());
  }

  @Test
  public void testOutOfOrderEpochFill() {
    LocationGroup.HotTracker tracker = newTracker();
    // With K > 1, epoch 10 (written by one mapper subset) can fill up before epoch 5
    // (written by another subset). Each epoch is measured against its own learn time.
    tracker.onEpochLearned(5, 0);
    tracker.onEpochLearned(10, 0);
    // Epoch 10 fills first: fillTime 30s < window => boost.
    assertEquals(2, tracker.onSoftSplit(10, 30000));
    // Epoch 5 fills later at t=100s: fillTime 100s > window => no boost, unaffected by
    // the earlier out-of-order event of epoch 10.
    assertEquals(2, tracker.onSoftSplit(5, 100000));
  }

  @Test
  public void testRetireCleansMeasureState() {
    LocationGroup.HotTracker tracker = newTracker();
    tracker.onEpochLearned(0, 0);
    assertEquals(2, tracker.onSoftSplit(0, 30000));
    tracker.onRetire(0);
    // After retire, a late repeated SOFT_SPLIT of the same epoch has an unknown start
    // point and never boosts again.
    assertEquals(2, tracker.onSoftSplit(0, 40000));
    tracker.onRetire(1);
    assertEquals(2, tracker.onSoftSplit(1, 45000));
  }
}
