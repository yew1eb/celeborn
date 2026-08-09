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
    loc0
  }

  private def primaryLocs(shuffleId: Int, partitionId: Int): List[PartitionLocation] =
    lifecycleManager
      .workerSnapshots(shuffleId)
      .asScala
      .values
      .flatMap(_.getPrimaryPartitions(Some(partitionId)).asScala)
      .toList

  test("desired=3 allocates gap locations on different workers and replies full active set") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new ChangePartitionManager(conf, lifecycleManager)
    val context = new CapturingContext

    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false,
      desiredLocationCount = 3)

    // The retiring epoch-0 location is replaced and boosted: 3 fresh locations allocated.
    val newLocs = primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
    assert(newLocs.map(_.getEpoch).toSet == Set(1, 2, 3))
    // Newly allocated locations are on mutually different workers.
    assert(newLocs.map(_.getHost).distinct.size == 3)

    // The reply carries the full active set: max epoch as the primary reply, the rest
    // as additional locations.
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 3)
    assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(1, 2))
  }

  test("request with active set already satisfied allocates 0 but still replies full set") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new ChangePartitionManager(conf, lifecycleManager)

    // Boost to 3 active locations first: active epochs {1, 2, 3}.
    changePartitionManager.handleRequestPartitionLocation(
      new CapturingContext,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false,
      desiredLocationCount = 3)
    val locCountAfterBoost = primaryLocs(shuffleId, partitionId).size
    val loc1 = primaryLocs(shuffleId, partitionId).find(_.getEpoch == 1).get

    // A lagging executor retires epoch 1 with desired=2: active {2, 3} already satisfies
    // desired, nothing is allocated, but the reply still carries the full active set.
    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      1,
      loc1,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false,
      desiredLocationCount = 2)

    assert(primaryLocs(shuffleId, partitionId).size == locCountAfterBoost)
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 3)
    assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(2))
  }

  test("concurrent revives of one partition take max desired and allocate once") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 4).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new ChangePartitionManager(conf, lifecycleManager)

    // Two executors revive the same partition concurrently with desired 2 and 4.
    val context1 = new CapturingContext
    val context2 = new CapturingContext
    val requestSet = new util.HashSet[ChangePartitionRequest]()
    val request1 = ChangePartitionRequest(
      context1,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      2)
    val request2 = ChangePartitionRequest(
      context2,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      4)
    requestSet.add(request1)
    requestSet.add(request2)
    changePartitionManager.changePartitionRequests
      .computeIfAbsent(shuffleId, changePartitionManager.rpcContextRegisterFunc)
      .put(partitionId, requestSet)

    changePartitionManager.handleRequestPartitions(shuffleId, Array(request2), false)

    // desired = max(2, 4) = 4, allocated exactly once (idempotent for concurrent revives).
    val newLocs = primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
    assert(newLocs.map(_.getEpoch).toSet == Set(1, 2, 3, 4))
    assert(newLocs.map(_.getHost).distinct.size == 4)

    // Both requests are replied with the same full active set.
    Seq(context1, context2).foreach { context =>
      val reply = context.replies.get(partitionId)
      assert(reply != null && reply.status == StatusCode.SUCCESS)
      assert(reply.loc.isDefined && reply.loc.get.getEpoch == 4)
      assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(1, 2, 3))
    }
  }

  test("desired is truncated to maxLocationsPerPartition") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 5).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new ChangePartitionManager(conf, lifecycleManager)
    val context = new CapturingContext

    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false,
      desiredLocationCount = 10)

    // desired is truncated to the configured upper bound 4.
    val newLocs = primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
    assert(newLocs.map(_.getEpoch).toSet == Set(1, 2, 3, 4))
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 4)
    assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(1, 2, 3))
  }
}
