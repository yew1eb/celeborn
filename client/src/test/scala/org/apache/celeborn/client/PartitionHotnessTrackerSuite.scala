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

package org.apache.celeborn.client

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.PartitionLocation.Mode
import org.apache.celeborn.common.protocol.message.StatusCode

class PartitionHotnessTrackerSuite extends CelebornFunSuite {

  private val shuffleId = 1
  private val partitionId = 0

  private def makeLoc(partitionId: Int, epoch: Int, host: String): PartitionLocation =
    new PartitionLocation(partitionId, epoch, host, 9000, 9100, 9200, 9300, Mode.PRIMARY)

  private def makeTracker(
      workerAvailable: PartitionLocation => Boolean,
      maxLocations: Int = -1,
      numMappersOf: Int => Int = _ => 1000): PartitionHotnessTracker = {
    val conf = new CelebornConf()
    if (maxLocations > 0) {
      // Pin the cap so capping tests are independent of the product default.
      conf.set(
        CelebornConf.CLIENT_SHUFFLE_ADAPTIVE_PARTITION_WRITE_PARALLELISM_MAX_LOCATIONS.key,
        maxLocations.toString)
    }
    // With the product default maxLocations=-1 the cap resolves to the shuffle's mapper count;
    // give the fake shuffle enough mappers that only an explicitly pinned maxLocations caps it.
    new PartitionHotnessTracker(conf, (_, _) => None, workerAvailable, numMappersOf)
  }

  test("HARD_SPLIT with an unavailable worker only retires, never boosts") {
    val tracker = makeTracker(_ => false)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // A HARD_SPLIT of a known-unavailable worker is not measured: desired stays 1.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.HARD_SPLIT), 10000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 1)
  }

  test("push failure cause only retires, never boosts") {
    val tracker = makeTracker(_ => true)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY),
      10000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 1)
  }

  test("HARD_SPLIT and SOFT_SPLIT are measured equivalently") {
    Seq(StatusCode.SOFT_SPLIT, StatusCode.HARD_SPLIT).foreach { cause =>
      val tracker = makeTracker(_ => true)
      val loc0 = makeLoc(partitionId, 0, "host1")
      tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

      // Fast fill of epoch 0 (45s, target 2): both causes boost desired to 2.
      tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(cause), 45000L)
      assert(tracker.desiredLocationCount(shuffleId, partitionId) == 2)

      // Slow fill of epoch 1 (70s > window): neither cause boosts.
      val loc1 = makeLoc(partitionId, 1, "host1")
      tracker.registerAllocation(shuffleId, partitionId, Set(1), 70000L)
      tracker.onEpochRetired(shuffleId, partitionId, 1, loc1, Some(cause), 140000L)
      assert(tracker.desiredLocationCount(shuffleId, partitionId) == 2)
    }
  }

  test("fillTime proportionally steps desired, capped at maxLocationsPerPartition") {
    val tracker = makeTracker(_ => true, 4)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // fillTime 30s under K=1 -> target ceil(1*60/30) = 2. Epoch 0 is soft-retained in the
    // active set after the report.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 30000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 2)

    // fillTime 25s measured under K=2 ({epoch 0 soft-retained, epoch 1}):
    // target ceil(2*60/25) = 5, capped at the configured max 4.
    tracker.registerAllocation(shuffleId, partitionId, Set(1), 30000L)
    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      1,
      makeLoc(partitionId, 1, "host1"),
      Some(StatusCode.SOFT_SPLIT),
      55000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 4)

    // fillTime 10s under K=3 -> target 18, still the cap: a very hot partition reaches the
    // cap after its first fast split report, without any debounce window.
    tracker.registerAllocation(shuffleId, partitionId, Set(2), 55000L)
    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      2,
      makeLoc(partitionId, 2, "host1"),
      Some(StatusCode.SOFT_SPLIT),
      65000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 4)
  }

  test("fillTime measured under K active locations scales the target by K") {
    // The measured fillTime is per-location; without the K factor the target is
    // underestimated K-fold once K > 1 and desired freezes at the value judged
    // once under K ~ 1.
    val tracker = makeTracker(_ => true, maxLocations = 64)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // First fill under K = 1: fillTime 30s -> target ceil(60/30) = 2.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 30000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 2)

    // Epochs 0 (soft-retained) and 1 active. Epoch 1 fills in 10s under K = 2:
    // target ceil(2 * 60 / 10) = 12 — NOT the K-blind ceil(60/10) = 6.
    tracker.registerAllocation(shuffleId, partitionId, Set(1), 30000L)
    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      1,
      makeLoc(partitionId, 1, "host1"),
      Some(StatusCode.SOFT_SPLIT),
      40000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 12)
  }

  test("zero fillTime is floored at 1ms, desired capped at the mapper count") {
    // With the default maxLocations = -1 the cap resolves to the shuffle's mapper count:
    // a regular fill below the cap takes its target unchanged; a zero fillTime (report in
    // the same millisecond as the allocation) is floored at 1ms instead of computing
    // ceil(window/0) = Infinity, so desired is capped at the mapper count.
    val tracker = makeTracker(_ => true, maxLocations = -1, numMappersOf = _ => 128)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // Regular fill: fillTime 10s -> target 6 < 128, desired takes the target.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 10000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 6)

    // Zero fillTime under K=2: target ceil(2*60000/1) = 120000, capped at 128 mappers.
    tracker.registerAllocation(shuffleId, partitionId, Set(1), 10000L)
    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      1,
      makeLoc(partitionId, 1, "host1"),
      Some(StatusCode.SOFT_SPLIT),
      10000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 128)
  }

  test("positive maxLocations takes precedence over the mapper count") {
    val tracker = makeTracker(_ => true, maxLocations = 4, numMappersOf = _ => 128)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // fillTime 10s -> target 6, capped at the configured 4 despite 128 mappers.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 10000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 4)
  }

  test("desired never decreases on slower subsequent fills") {
    val tracker = makeTracker(_ => true, 4)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // Very fast fill: jump straight to the cap.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 10000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 4)

    // After the fan-out the next location fills slower (30s, target 3): desired stays 4.
    tracker.registerAllocation(shuffleId, partitionId, Set(1), 10000L)
    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      1,
      makeLoc(partitionId, 1, "host1"),
      Some(StatusCode.SOFT_SPLIT),
      40000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 4)
  }

  test("SOFT_SPLIT of an available worker retains the epoch in the active set") {
    val tracker = makeTracker(_ => true)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // Soft split: the file stays writable until it hard-splits, so epoch 0 is retained.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 30000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId) == Set(0))

    // A later hard split of the same epoch removes it from the writable set.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.HARD_SPLIT), 40000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId).isEmpty)
  }

  test("SOFT_SPLIT of an unavailable worker removes the epoch from the active set") {
    val tracker = makeTracker(_ => false)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 30000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId).isEmpty)
  }

  test("push failure cause removes the epoch from the active set") {
    val tracker = makeTracker(_ => true)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // Seed the epoch as active via a soft split, then a push failure removes it.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 30000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId) == Set(0))
    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY),
      40000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId).isEmpty)
  }

  test("late SOFT_SPLIT report does not resurrect a hard-retired epoch") {
    val tracker = makeTracker(_ => true)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // Epoch 0 soft-splits and stays writable, then hard-splits and is removed.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 30000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId) == Set(0))
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.HARD_SPLIT), 40000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId).isEmpty)

    // A SOFT_SPLIT report sent before the hard split arrives late: it must not resurrect
    // the dead epoch into the writable set.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 41000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId).isEmpty)
  }
}
