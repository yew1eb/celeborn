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

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.PartitionLocation.Mode
import org.apache.celeborn.common.protocol.message.ControlMessages.WorkerResource
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.JavaUtils

class ChangePartitionManagerParallelWriteSuite extends CelebornFunSuite {

  private val APP = "app-parallel-write-test"

  private case class CapturedReply(
      status: StatusCode,
      loc: Option[PartitionLocation],
      additionals: util.List[PartitionLocation])

  private class CapturingContext extends RequestLocationCallContext {
    val replies = new ConcurrentHashMap[Int, CapturedReply]()
    override def reply(
        partitionId: Int,
        status: StatusCode,
        partitionLocationOpt: Option[PartitionLocation],
        available: Boolean,
        additionalLocations: util.List[PartitionLocation]): Unit = {
      replies.put(partitionId, CapturedReply(status, partitionLocationOpt, additionalLocations))
    }
  }

  private var lifecycleManager: LifecycleManager = _

  override protected def afterEach(): Unit = {
    if (lifecycleManager != null) {
      lifecycleManager.stop()
      lifecycleManager = null
    }
    super.afterEach()
  }

  private def makeConf(): CelebornConf = {
    val conf = new CelebornConf()
    conf.set(CelebornConf.CLIENT_SHUFFLE_PARALLEL_WRITE_ENABLED.key, "true")
    conf.set(CelebornConf.CLIENT_BATCH_HANDLE_CHANGE_PARTITION_ENABLED.key, "false")
    conf.set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")
    conf
  }

  private def makeWorker(id: Int): WorkerInfo =
    new WorkerInfo(s"host$id", 9000 + id, 9100 + id, 9200 + id, 9300 + id)

  private def makeLoc(partitionId: Int, epoch: Int, host: String): PartitionLocation =
    new PartitionLocation(partitionId, epoch, host, 9000, 9100, 9200, 9300, Mode.PRIMARY)

  /**
   * Set up a LifecycleManager holding the given workers (slot reservation stubbed out)
   * and an initial epoch-0 location of the partition on the first worker.
   */
  private def prepareLifecycleManager(
      conf: CelebornConf,
      shuffleId: Int,
      partitionId: Int,
      workers: Seq[WorkerInfo]): PartitionLocation = {
    lifecycleManager = new LifecycleManager(APP, conf) {
      override def reserveSlotsWithRetry(
          shuffleId: Int,
          candidates: util.HashSet[WorkerInfo],
          slots: WorkerResource,
          updateEpoch: Boolean,
          isSegmentGranularityVisible: Boolean): Boolean = true
    }
    val allocatedWorkers = JavaUtils.newConcurrentHashMap[String, ShufflePartitionLocationInfo]()
    workers.foreach(w => allocatedWorkers.put(w.toUniqueId, new ShufflePartitionLocationInfo(w)))
    val loc0 = makeLoc(partitionId, 0, workers.head.host)
    allocatedWorkers.get(workers.head.toUniqueId).addPrimaryPartitions(
      util.Collections.singletonList(loc0))
    lifecycleManager.shuffleAllocatedWorkers.put(shuffleId, allocatedWorkers)
    lifecycleManager.updateLatestPartitionLocations(shuffleId, util.Arrays.asList(loc0))
    lifecycleManager.commitManager.registerShuffle(
      shuffleId,
      1,
      isSegmentGranularityVisible = false,
      1)
    loc0
  }

  private def primaryLocs(shuffleId: Int, partitionId: Int): List[PartitionLocation] =
    lifecycleManager
      .workerSnapshots(shuffleId)
      .asScala
      .values
      .flatMap(_.getPrimaryPartitions(Some(partitionId)).asScala)
      .toList

  /** A ChangePartitionManager whose clock is driven by the returned time holder. */
  private class FakeClockManager(
      conf: CelebornConf,
      lifecycleManager: LifecycleManager,
      var now: Long)
    extends ChangePartitionManager(conf, lifecycleManager) {
    nowMs = () => now
    def advance(ms: Long): Unit = {
      now += ms
    }
  }

  test("hot partition (fillTime < window) boosts desired and allocates gap locations") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 100000L)

    // Epoch 0 fills in 10s (< 60s window): the first SOFT_SPLIT report boosts desired to 2.
    changePartitionManager.advance(10000)
    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)

    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)
    // desired=2 with the retiring epoch 0: 2 fresh locations allocated on different workers.
    val newLocs = primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
    assert(newLocs.map(_.getEpoch).toSet == Set(1, 2))
    assert(newLocs.map(_.getHost).distinct.size == 2)

    // The reply carries the full active set: max epoch as the primary reply, the rest
    // as additional locations.
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 2)
    assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(1))
  }

  test("partition filled slower than the window is not boosted") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 2).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 100000L)

    // Normal file rolling: fillTime 70s > 60s window, desired stays 1 (plain replace).
    changePartitionManager.advance(70000)
    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)

    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 1)
    val newLocs = primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
    assert(newLocs.map(_.getEpoch).toSet == Set(1))
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 1)
    assert(reply.additionals.isEmpty)
  }

  test("epoch with unknown allocTime (legacy data) never boosts") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 2).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    // No recordInitialAllocTime: the alloc time of epoch 0 is unknown.
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)

    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)

    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 1)
    assert(primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
      .map(_.getEpoch).toSet == Set(1))
  }

  test("HARD_SPLIT only retires, never boosts") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 2).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 100000L)

    // Epoch 0 hard-splits 10s after allocation: even a fast fill does not boost.
    changePartitionManager.advance(10000)
    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.HARD_SPLIT),
      isSegmentGranularityVisible = false)

    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 1)
    assert(primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
      .map(_.getEpoch).toSet == Set(1))
  }

  test("repeated SOFT_SPLIT reports of the same epoch boost only once") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 100000L)

    changePartitionManager.advance(10000)
    changePartitionManager.handleRequestPartitionLocation(
      new CapturingContext,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)
    val locCountAfterBoost = primaryLocs(shuffleId, partitionId).size

    // Another executor reports SOFT_SPLIT of the same epoch 0: no second boost, no new
    // allocation, but it still gets the current active set.
    changePartitionManager.advance(5000)
    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)

    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)
    assert(primaryLocs(shuffleId, partitionId).size == locCountAfterBoost)
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 2)
    assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(1))
  }

  test("debounce: at most one boost per window") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 0L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // t=10s: epoch 0 fills fast, boost desired to 2 (epochs 1, 2 allocated at t=10s).
    changePartitionManager.advance(10000)
    changePartitionManager.handleRequestPartitionLocation(
      new CapturingContext,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)

    // t=40s: epoch 2 (allocated at t=10s) fills in 30s, hot, but within the debounce
    // window of the last boost: desired stays 2 (epoch 3 allocated as plain replace).
    changePartitionManager.advance(30000)
    val loc2 = primaryLocs(shuffleId, partitionId).find(_.getEpoch == 2).get
    changePartitionManager.handleRequestPartitionLocation(
      new CapturingContext,
      shuffleId,
      partitionId,
      2,
      loc2,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)

    // t=75s: epoch 3 (allocated at t=40s) fills in 35s, hot and past the debounce window:
    // desired boosts to 3.
    changePartitionManager.advance(35000)
    val loc3 = primaryLocs(shuffleId, partitionId).find(_.getEpoch == 3).get
    changePartitionManager.handleRequestPartitionLocation(
      new CapturingContext,
      shuffleId,
      partitionId,
      3,
      loc3,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 3)
  }

  test("desired is capped at maxLocationsPerPartition") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 2).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new ChangePartitionManager(conf, lifecycleManager)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // One hot epoch per window: desired climbs 1 -> 2 -> 3 -> 4, then is capped.
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      0,
      Some(StatusCode.SOFT_SPLIT),
      10000L)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)
    changePartitionManager.recordAllocTime(shuffleId, partitionId, 1, 70000L)
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      1,
      Some(StatusCode.SOFT_SPLIT),
      80000L)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 3)
    changePartitionManager.recordAllocTime(shuffleId, partitionId, 2, 140000L)
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      2,
      Some(StatusCode.SOFT_SPLIT),
      150000L)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 4)
    changePartitionManager.recordAllocTime(shuffleId, partitionId, 3, 210000L)
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      3,
      Some(StatusCode.SOFT_SPLIT),
      220000L)
    // The configured upper bound is 4.
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 4)
  }

  test("out-of-order epoch fills are measured independently") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 2).map(makeWorker)
    prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new ChangePartitionManager(conf, lifecycleManager)

    // With K > 1, epoch 10 (written by one mapper subset) can fill up before epoch 5
    // (written by another subset). Each epoch is measured against its own alloc time.
    changePartitionManager.recordAllocTime(shuffleId, partitionId, 5, 50000L)
    changePartitionManager.recordAllocTime(shuffleId, partitionId, 10, 0L)
    // Epoch 10 fills first: fillTime 30s < window, boost.
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      10,
      Some(StatusCode.SOFT_SPLIT),
      30000L)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)
    // Epoch 5 fills later at t=95s: measured against its own allocTime, fillTime 45s <
    // window and past the debounce window, boost, unaffected by the out-of-order event.
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      5,
      Some(StatusCode.SOFT_SPLIT),
      95000L)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 3)
  }

  test("request with active set already satisfied allocates 0 but still replies full set") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 100000L)

    // Boost to 2 active locations first: active epochs {1, 2}.
    changePartitionManager.advance(10000)
    changePartitionManager.handleRequestPartitionLocation(
      new CapturingContext,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)
    val locCountAfterBoost = primaryLocs(shuffleId, partitionId).size
    val loc1 = primaryLocs(shuffleId, partitionId).find(_.getEpoch == 1).get

    // A lagging executor retires epoch 1 slowly (fillTime 90s > window, no boost): the
    // surviving epoch 2 already satisfies the writer, nothing is allocated, but the reply
    // still carries the current active set.
    changePartitionManager.advance(90000)
    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      1,
      loc1,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)

    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)
    assert(primaryLocs(shuffleId, partitionId).size == locCountAfterBoost)
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 2)
    assert(reply.additionals.isEmpty)
  }

  test("concurrent revives of one partition allocate once and are replied the full set") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new ChangePartitionManager(conf, lifecycleManager)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 100000L)

    // The first SOFT_SPLIT report of epoch 0 boosts desired to 2; reports of the same
    // epoch queued from other executors are deduped.
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      0,
      Some(StatusCode.SOFT_SPLIT),
      110000L)
    val context1 = new CapturingContext
    val context2 = new CapturingContext
    val requestSet = new util.HashSet[ChangePartitionRequest]()
    val request1 = ChangePartitionRequest(
      context1,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT))
    val request2 = ChangePartitionRequest(
      context2,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT))
    requestSet.add(request1)
    requestSet.add(request2)
    changePartitionManager.changePartitionRequests
      .computeIfAbsent(shuffleId, changePartitionManager.rpcContextRegisterFunc)
      .put(partitionId, requestSet)

    changePartitionManager.handleRequestPartitions(shuffleId, Array(request2), false)

    // desired=2, allocated exactly once (idempotent for concurrent revives).
    val newLocs = primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
    assert(newLocs.map(_.getEpoch).toSet == Set(1, 2))
    assert(newLocs.map(_.getHost).distinct.size == 2)

    // Both requests are replied with the same full active set.
    Seq(context1, context2).foreach { context =>
      val reply = context.replies.get(partitionId)
      assert(reply != null && reply.status == StatusCode.SUCCESS)
      assert(reply.loc.isDefined && reply.loc.get.getEpoch == 2)
      assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(1))
    }
  }
}
