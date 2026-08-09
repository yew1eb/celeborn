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
      workerAvailable: PartitionLocation => Boolean): PartitionHotnessTracker =
    new PartitionHotnessTracker(new CelebornConf(), (_, _) => None, workerAvailable)

  test("HARD_SPLIT with an available worker boosts desired") {
    val tracker = makeTracker(_ => true)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // Epoch 0 hard-splits 10s after allocation (< 60s window) on a healthy worker:
    // the threshold crossing is measured like a fast fill and boosts desired to 2.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.HARD_SPLIT), 10000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 2)
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

      // Fast fill of epoch 0: both causes boost desired to 2.
      tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(cause), 10000L)
      assert(tracker.desiredLocationCount(shuffleId, partitionId) == 2)

      // Slow fill of epoch 1 (70s > window): neither cause boosts.
      val loc1 = makeLoc(partitionId, 1, "host1")
      tracker.recordAllocTime(shuffleId, partitionId, 1, 70000L)
      tracker.onEpochRetired(shuffleId, partitionId, 1, loc1, Some(cause), 140000L)
      assert(tracker.desiredLocationCount(shuffleId, partitionId) == 2)
    }
  }
}
