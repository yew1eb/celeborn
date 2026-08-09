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
import java.util.{function, Set => JSet}
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.celeborn.client.LifecycleManager.ShuffleFailedWorkers
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.ControlMessages.WorkerResource
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils}

case class ChangePartitionRequest(
    context: RequestLocationCallContext,
    shuffleId: Int,
    partitionId: Int,
    epoch: Int,
    oldPartition: PartitionLocation,
    causes: Option[StatusCode])

class ChangePartitionManager(
    conf: CelebornConf,
    lifecycleManager: LifecycleManager) extends Logging {

  private val pushReplicateEnabled = conf.clientPushReplicateEnabled
  private val dynamicWriteParallelismEnabled = conf.dynamicWriteParallelismEnabled
  private val dynamicWriteParallelismMax = conf.dynamicWriteParallelismMax
  private val reviveWindowMs = conf.dynamicWriteParallelismReviveWindowMs
  private val reviveThresholdRatio = conf.dynamicWriteParallelismReviveThresholdRatio
  private val cooldownMs = conf.dynamicWriteParallelismCooldownMs

  // Dynamic write parallelism state, per (shuffleId, partitionId).
  // reviveTimestamps: wall-clock timestamps of real allocations (post three-layer dedup), used to
  //                  compute revive frequency as the upgrade signal. Capped per partition.
  // parallelismState: target parallelism P + last upgrade time (for cooldown).
  private val partitionReviveTimestamps =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[Integer, util.ArrayDeque[java.lang.Long]]]()
  private val partitionParallelismState =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[Integer, ParallelismState]]()

  // shuffleId -> (partitionId -> set of ChangePartition)
  val changePartitionRequests
      : ConcurrentHashMap[Int, ConcurrentHashMap[Integer, JSet[ChangePartitionRequest]]] =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[Integer, JSet[ChangePartitionRequest]]]()

  // shuffleId -> locks
  private val locks = JavaUtils.newConcurrentHashMap[Int, Array[AnyRef]]()
  private val lockBucketSize = conf.batchHandleChangePartitionBuckets

  // shuffleId -> set of partition id
  private val inBatchPartitions =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap.KeySetView[Int, java.lang.Boolean]]()

  private val batchHandleChangePartitionEnabled = conf.batchHandleChangePartitionEnabled
  private val batchHandleChangePartitionExecutors = ThreadUtils.newDaemonCachedThreadPool(
    "celeborn-client-lifecycle-manager-change-partition-executor",
    conf.batchHandleChangePartitionNumThreads)
  private val batchHandleChangePartitionRequestInterval =
    conf.batchHandleChangePartitionRequestInterval
  private val batchHandleChangePartitionSchedulerThread: Option[ScheduledExecutorService] =
    if (batchHandleChangePartitionEnabled) {
      Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "celeborn-client-lifecycle-manager-change-partition-scheduler"))
    } else {
      None
    }

  private var batchHandleChangePartition: Option[ScheduledFuture[_]] = _

  private val testRetryRevive = conf.testRetryRevive

  private val dynamicResourceEnabled = conf.clientShuffleDynamicResourceEnabled
  private val dynamicResourceUnavailableFactor = conf.clientShuffleDynamicResourceFactor

  def start(): Unit = {
    batchHandleChangePartition = batchHandleChangePartitionSchedulerThread.map {
      // noinspection ConvertExpressionToSAM
      _.scheduleWithFixedDelay(
        new Runnable {
          override def run(): Unit = {
            try {
              changePartitionRequests.asScala.foreach { case (shuffleId, requests) =>
                batchHandleChangePartitionExecutors.submit {
                  new Runnable {
                    override def run(): Unit = {
                      val distinctPartitions = {
                        val requestSet = inBatchPartitions.get(shuffleId)
                        val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
                        requests.asScala.map { case (partitionId, request) =>
                          locksForShuffle(partitionId % locksForShuffle.length).synchronized {
                            if (!requestSet.contains(partitionId) && requests.containsKey(
                                partitionId)) {
                              requestSet.add(partitionId)
                              Some(request.asScala.toArray.maxBy(_.epoch))
                            } else {
                              None
                            }
                          }
                        }.filter(_.isDefined).map(_.get).toArray
                      }
                      if (distinctPartitions.nonEmpty) {
                        handleRequestPartitions(
                          shuffleId,
                          distinctPartitions,
                          lifecycleManager.commitManager.isSegmentGranularityVisible(shuffleId))
                      }
                    }
                  }
                }
              }
            } catch {
              case e: InterruptedException =>
                logError("Partition split scheduler thread is shutting down, detail: ", e)
                throw e
            }
          }
        },
        0,
        batchHandleChangePartitionRequestInterval,
        TimeUnit.MILLISECONDS)
    }
  }

  def stop(): Unit = {
    batchHandleChangePartition.foreach(_.cancel(true))
    batchHandleChangePartitionSchedulerThread.foreach(ThreadUtils.shutdown(_))
  }

  val rpcContextRegisterFunc
      : function.Function[Int, ConcurrentHashMap[Integer, JSet[ChangePartitionRequest]]] =
    new util.function.Function[
      Int,
      ConcurrentHashMap[Integer, util.Set[ChangePartitionRequest]]]() {
      override def apply(s: Int): ConcurrentHashMap[Integer, util.Set[ChangePartitionRequest]] =
        JavaUtils.newConcurrentHashMap()
    }

  private val inBatchShuffleIdRegisterFunc =
    new util.function.Function[Int, ConcurrentHashMap.KeySetView[Int, java.lang.Boolean]]() {
      override def apply(s: Int): ConcurrentHashMap.KeySetView[Int, java.lang.Boolean] =
        ConcurrentHashMap.newKeySet[Int]()
    }

  private val locksRegisterFunc = new util.function.Function[Int, Array[AnyRef]] {
    override def apply(t: Int): Array[AnyRef] = {
      Array.fill(lockBucketSize)(new AnyRef())
    }
  }

  def handleRequestPartitionLocation(
      context: RequestLocationCallContext,
      shuffleId: Int,
      partitionId: Int,
      oldEpoch: Int,
      oldPartition: PartitionLocation,
      cause: Option[StatusCode] = None,
      isSegmentGranularityVisible: Boolean): Unit = {

    val changePartition = ChangePartitionRequest(
      context,
      shuffleId,
      partitionId,
      oldEpoch,
      oldPartition,
      cause)
    // check if there exists request for the partition, if do just register
    val requests = changePartitionRequests.computeIfAbsent(shuffleId, rpcContextRegisterFunc)
    inBatchPartitions.computeIfAbsent(shuffleId, inBatchShuffleIdRegisterFunc)

    lifecycleManager.commitManager.registerCommitPartitionRequest(
      shuffleId,
      oldPartition,
      cause)

    val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
    locksForShuffle(partitionId % locksForShuffle.length).synchronized {
      if (requests.containsKey(partitionId)) {
        logDebug(s"[handleRequestPartitionLocation] For shuffle: $shuffleId, request for same " +
          s"partition: $partitionId-$oldEpoch exists, register context.")
        requests.get(partitionId).add(changePartition)
        return
      } else {
        getLatestPartition(shuffleId, partitionId, oldEpoch).foreach { latestLoc =>
          context.reply(
            partitionId,
            StatusCode.SUCCESS,
            // Reply the active sibling set (1:N when enabled; [latestLoc] when disabled).
            Option(lifecycleManager.getActiveSiblings(shuffleId, partitionId)),
            lifecycleManager.workerStatusTracker.workerAvailableByLocation(oldPartition))
          logDebug(s"[handleRequestPartitionLocation]: For shuffle: $shuffleId," +
            s" old partition: $partitionId-$oldEpoch, new partition: $latestLoc found, return it")
          return
        }
        val set = new util.HashSet[ChangePartitionRequest]()
        set.add(changePartition)
        requests.put(partitionId, set)
      }
    }
    if (!batchHandleChangePartitionEnabled) {
      handleRequestPartitions(shuffleId, Array(changePartition), isSegmentGranularityVisible)
    }
  }

  private def getLatestPartition(
      shuffleId: Int,
      partitionId: Int,
      epoch: Int): Option[PartitionLocation] = {
    val map = lifecycleManager.latestPartitionLocation.get(shuffleId)
    if (map != null) {
      val loc = map.get(partitionId)
      if (loc != null && loc.getEpoch > epoch) {
        return Some(loc)
      }
    }
    None
  }

  def handleRequestPartitions(
      shuffleId: Int,
      changePartitions: Array[ChangePartitionRequest],
      isSegmentGranularityVisible: Boolean): Unit = {
    val requestsMap = changePartitionRequests.get(shuffleId)

    val changes = changePartitions.map { change =>
      s"${change.shuffleId}-${change.partitionId}-${change.epoch}"
    }.mkString("[", ",", "]")
    logWarning(s"Batch handle change partition for $changes")

    // Exclude all failed workers
    if (changePartitions.exists(_.causes.isDefined) && !testRetryRevive) {
      changePartitions.filter(_.causes.isDefined).foreach { changePartition =>
        lifecycleManager.workerStatusTracker.excludeWorkerFromPartition(
          shuffleId,
          changePartition.oldPartition,
          changePartition.causes.get)
      }
    }

    // remove together to reduce lock time
    def replySuccess(locations: Array[PartitionLocation]): Unit = {
      val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
      locations.map { location =>
        locksForShuffle(location.getId % locksForShuffle.length).synchronized {
          if (batchHandleChangePartitionEnabled) {
            inBatchPartitions.get(shuffleId).remove(location.getId)
          }
          // Here one partition id can be remove more than once,
          // so need to filter null result before reply.
          location -> Option(requestsMap.remove(location.getId))
        }
      }.foreach { case (newLocation, requests) =>
        requests.map(_.asScala.toList.foreach(req =>
          req.context.reply(
            req.partitionId,
            StatusCode.SUCCESS,
            // Reply the full active sibling set for this partition (1:N). When dynamic write
            // parallelism is disabled, getActiveSiblings returns just [newLocation], preserving
            // the legacy single-location behavior.
            Option(lifecycleManager.getActiveSiblings(shuffleId, req.partitionId)),
            lifecycleManager.workerStatusTracker.workerAvailableByLocation(req.oldPartition))))
      }
    }

    // remove together to reduce lock time
    def replyFailure(status: StatusCode): Unit = {
      changePartitions.map { changePartition =>
        val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
        locksForShuffle(changePartition.partitionId % locksForShuffle.length).synchronized {
          if (batchHandleChangePartitionEnabled) {
            inBatchPartitions.get(shuffleId).remove(changePartition.partitionId)
          }
          Option(requestsMap.remove(changePartition.partitionId))
        }
      }.foreach { requests =>
        requests.map(_.asScala.toList.foreach(req =>
          req.context.reply(
            req.partitionId,
            status,
            None: Option[util.List[PartitionLocation]],
            lifecycleManager.workerStatusTracker.workerAvailableByLocation(req.oldPartition))))
      }
    }

    val candidates = new util.HashSet[WorkerInfo]()
    val newlyRequestedLocations = new WorkerResource()

    val snapshotCandidates =
      lifecycleManager
        .workerSnapshots(shuffleId)
        .asScala
        .values
        .map(_.workerInfo)
        .filter(lifecycleManager.workerStatusTracker.workerAvailable)
        .toSet
        .asJava
    candidates.addAll(snapshotCandidates)

    if (dynamicResourceEnabled) {
      val shuffleAllocatedWorkers = lifecycleManager.workerSnapshots(shuffleId).size()
      val unavailableWorkerRatio = 1 - (snapshotCandidates.size * 1.0 / shuffleAllocatedWorkers)
      if (candidates.size < 1 || (pushReplicateEnabled && candidates.size < 2)
        || (unavailableWorkerRatio >= dynamicResourceUnavailableFactor)) {

        // get new available workers for the request partition ids
        val partitionIds = new util.ArrayList[Integer](
          changePartitions.map(_.partitionId).map(Integer.valueOf).toList.asJava)
        // The partition id value is not important here because we're just trying to get the workers to use
        val requestSlotsRes =
          lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, partitionIds)

        requestSlotsRes.status match {
          case StatusCode.REQUEST_FAILED =>
            logInfo(s"ChangePartition requestSlots RPC request failed for $shuffleId!")
          case StatusCode.SLOT_NOT_AVAILABLE =>
            logInfo(s"ChangePartition requestSlots for $shuffleId failed, have no available slots.")
          case StatusCode.SUCCESS =>
            logDebug(
              s"ChangePartition requestSlots request for workers Success! shuffleId: $shuffleId availableWorkers Info: ${requestSlotsRes.workerResource.keySet()}")
          case StatusCode.WORKER_EXCLUDED =>
            logInfo(s"ChangePartition requestSlots request for workers for $shuffleId failed due to all workers be excluded!")
          case _ => // won't happen
            throw new UnsupportedOperationException()
        }

        if (requestSlotsRes.status.equals(StatusCode.SUCCESS)) {
          requestSlotsRes.workerResource.keySet().asScala.foreach { workerInfo: WorkerInfo =>
            newlyRequestedLocations.computeIfAbsent(workerInfo, lifecycleManager.newLocationFunc)
          }

          // SetupEndpoint for new Workers
          val workersRequireEndpoints = new util.HashSet[WorkerInfo](
            requestSlotsRes.workerResource.keySet()
              .asScala
              .filter(lifecycleManager.workerStatusTracker.workerAvailable)
              .asJava)

          val connectFailedWorkers = new ShuffleFailedWorkers()
          lifecycleManager.setupEndpoints(
            workersRequireEndpoints,
            shuffleId,
            connectFailedWorkers)
          workersRequireEndpoints.removeAll(connectFailedWorkers.asScala.keys.toList.asJava)
          candidates.addAll(workersRequireEndpoints)

          // Update worker status
          lifecycleManager.workerStatusTracker.recordWorkerFailure(connectFailedWorkers)
          lifecycleManager.workerStatusTracker.removeFromExcludedWorkers(candidates)
        }
      }
    }

    if (candidates.size < 1 || (pushReplicateEnabled && candidates.size < 2)) {
      logError("[Update partition] failed for not enough candidates for revive.")
      replyFailure(StatusCode.SLOT_NOT_AVAILABLE)
      return
    }

    // PartitionSplit all contains oldPartition
    // Dynamic write parallelism: record a real allocation (post three-layer dedup) per partition
    // and decide how many sibling locations to allocate this round. appendCount=1 means just
    // replenish the replaced location (no upgrade); >1 means upgrade P->2P.
    val nowMs = System.currentTimeMillis()
    val appendCounts = changePartitions.map { cp =>
      val count = recordRealAllocationAndGetAppendCount(shuffleId, cp.partitionId, nowMs)
      (cp, count)
    }

    val newlyAllocatedLocations =
      reallocateChangePartitionRequestSlotsFromCandidates(appendCounts, candidates.asScala.toList)

    if (!lifecycleManager.reserveSlotsWithRetry(
        shuffleId,
        candidates,
        newlyAllocatedLocations,
        isSegmentGranularityVisible = isSegmentGranularityVisible)) {
      logError(s"[Update partition] failed for $shuffleId.")
      replyFailure(StatusCode.RESERVE_SLOTS_FAILED)
      return
    }

    // newlyRequestedLocations is empty if dynamicResourceEnabled is false
    newlyRequestedLocations.putAll(newlyAllocatedLocations)

    val newPrimaryLocations = newlyRequestedLocations.asScala.flatMap {
      case (workInfo, (primaryLocations, replicaLocations)) =>
        // Add all re-allocated slots to worker snapshots.
        val partitionLocationInfo = lifecycleManager.workerSnapshots(shuffleId).computeIfAbsent(
          workInfo.toUniqueId,
          new util.function.Function[String, ShufflePartitionLocationInfo] {
            override def apply(workerId: String): ShufflePartitionLocationInfo = {
              new ShufflePartitionLocationInfo(workInfo)
            }
          })
        partitionLocationInfo.addPrimaryPartitions(primaryLocations)
        partitionLocationInfo.addReplicaPartitions(replicaLocations)
        lifecycleManager.updateLatestPartitionLocations(shuffleId, primaryLocations)

        // partition location can be null when call reserveSlotsWithRetry().
        val locations = (primaryLocations.asScala ++ replicaLocations.asScala.map(_.getPeer))
          .distinct.filter(_ != null)
        // TODO: should record the new partition locations and acknowledge the new partitionLocations to downstream task,
        //  in scenario the downstream task start early before the upstream task.
        locations
    }

    if (newPrimaryLocations.nonEmpty) {
      val changes = newPrimaryLocations.map { partition =>
        s"(partition ${partition.getId} epoch from ${partition.getEpoch - 1} to ${partition.getEpoch})"
      }.mkString("[", ", ", "]")
      logInfo(s"[Update partition] success for " +
        s"shuffle $shuffleId, succeed partitions: " +
        s"$changes.")
    }
    replySuccess(newPrimaryLocations.toArray)
  }

  private def reallocateChangePartitionRequestSlotsFromCandidates(
      appendCounts: Array[(ChangePartitionRequest, Int)],
      candidates: List[WorkerInfo]): WorkerResource = {
    val slots = new WorkerResource()
    appendCounts.foreach { case (partition, count) =>
      // Allocate `count` sibling locations for this partition (1 when feature disabled or no
      // upgrade). Each gets a distinct epoch via allocateFromCandidates' internal epoch increment.
      (0 until count).foreach { _ =>
        lifecycleManager.allocateFromCandidates(
          partition.partitionId,
          partition.epoch,
          candidates,
          slots)
      }
    }
    slots
  }

  def removeExpiredShuffle(shuffleId: Int): Unit = {
    changePartitionRequests.remove(shuffleId)
    inBatchPartitions.remove(shuffleId)
    locks.remove(shuffleId)
    lifecycleManager.activeSiblingsMap.remove(shuffleId)
    partitionReviveTimestamps.remove(shuffleId)
    partitionParallelismState.remove(shuffleId)
  }

  // ---- Dynamic write parallelism (1:N) helpers ----

  /** Per-partition dynamic parallelism state. */
  case class ParallelismState(var targetParallelism: Int, var lastUpgradeTimeMs: Long)

  private val MAX_REVIVE_SAMPLES = 64

  /**
   * Called after a real allocation (the third layer of the existing dedup in
   * handleRequestPartitionLocation) for a SOFT_SPLIT/HARD_SPLIT revive. Records the timestamp,
   * evaluates revive frequency, and decides whether to upgrade target parallelism (P -> 2P).
   *
   * Counting here (not at the revive entry) is critical: multiple mappers reporting the same full
   * location are collapsed to one real allocation by the three-layer dedup, so the frequency is
   * not inflated by the mapper count and reflects the true location-replacement rate.
   *
   * Returns the count of NEW sibling locations to allocate (>= 1 to replenish the replaced one;
   * > 1 when an upgrade is triggered). Caller is responsible for actual allocation + reply.
   */
  def recordRealAllocationAndGetAppendCount(
      shuffleId: Int,
      partitionId: Int,
      nowMs: Long): Int = {
    if (!dynamicWriteParallelismEnabled) return 1

    val state = getOrCreateState(shuffleId, partitionId)
    val timestamps = getOrCreateTimestamps(shuffleId, partitionId)

    // 1. Record this real allocation, prune window, cap samples.
    timestamps.synchronized {
      timestamps.addLast(nowMs)
      val cutoff = nowMs - reviveWindowMs
      while (!timestamps.isEmpty && timestamps.peekFirst() < cutoff) timestamps.pollFirst()
      while (timestamps.size() > MAX_REVIVE_SAMPLES) timestamps.pollFirst()
    }

    val activeCount = lifecycleManager.getActiveSiblings(shuffleId, partitionId).size()
    val currentP = math.max(1, activeCount)

    // 2. Cooldown: no upgrade right after a previous one.
    if (nowMs - state.lastUpgradeTimeMs < cooldownMs) {
      return 1
    }

    // 3. Upgrade decision: windowed real-allocation count >= K(P) = ratio * P.
    val inWindow = timestamps.synchronized {
      val cutoff = nowMs - reviveWindowMs
      timestamps.descendingIterator().asScala.takeWhile(_ >= cutoff).size
    }
    val threshold = math.ceil(reviveThresholdRatio * currentP).toInt
    if (inWindow >= threshold && currentP < dynamicWriteParallelismMax) {
      val newP = math.min(currentP * 2, dynamicWriteParallelismMax)
      state.targetParallelism = newP
      state.lastUpgradeTimeMs = nowMs
      // Clear window so this upgrade isn't double-counted by subsequent records.
      timestamps.synchronized { timestamps.clear() }
      // Allocate enough new siblings to reach newP (newP - currentP new ones; the current real
      // allocation replenishes 1, so net new = newP - currentP).
      return math.max(1, newP - currentP)
    }

    // No upgrade: just replenish the one replaced sibling.
    1
  }

  private def getOrCreateState(shuffleId: Int, partitionId: Int): ParallelismState = {
    val map = partitionParallelismState.computeIfAbsent(
      shuffleId,
      (_: Int) => JavaUtils.newConcurrentHashMap[Integer, ParallelismState]())
    map.computeIfAbsent(partitionId, (_: Integer) => ParallelismState(1, 0L))
  }

  private def getOrCreateTimestamps(
      shuffleId: Int,
      partitionId: Int): util.ArrayDeque[java.lang.Long] = {
    val map = partitionReviveTimestamps.computeIfAbsent(
      shuffleId,
      (_: Int) => JavaUtils.newConcurrentHashMap[Integer, util.ArrayDeque[java.lang.Long]]())
    map.computeIfAbsent(partitionId, (_: Integer) => new util.ArrayDeque[java.lang.Long]())
  }
}
