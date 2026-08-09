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

import scala.collection.JavaConverters._

import org.junit.Assert

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.PartitionLocation

/**
 * Unit tests for the dynamic write parallelism (1:N) decision logic in
 * [[ChangePartitionManager.recordRealAllocationAndGetAppendCount]]. Verifies:
 *  - feature disabled returns appendCount=1 (legacy behavior)
 *  - first real allocation upgrades P 1 -> 2
 *  - upgrade is P -> 2P (doubling), capped by max
 *  - cooldown suppresses immediate re-upgrade
 *  - K(P) = ratio x P threshold before upgrade
 */
class ChangePartitionManagerDynamicParallelismSuite extends CelebornFunSuite {

  private val appId = "test-dynamic-parallelism"

  private def newConf(enabled: Boolean, max: Int = 8, ratio: Double = 1.0): CelebornConf = {
    val conf = new CelebornConf()
    conf.set(CelebornConf.CLIENT_DYNAMIC_WRITE_PARALLELISM_ENABLED.key, String.valueOf(enabled))
    conf.set(CelebornConf.CLIENT_DYNAMIC_WRITE_PARALLELISM_MAX.key, String.valueOf(max))
    conf.set(
      CelebornConf.CLIENT_DYNAMIC_WRITE_PARALLELISM_REVIVE_THRESHOLD_RATIO.key,
      String.valueOf(ratio))
    conf.set(CelebornConf.CLIENT_DYNAMIC_WRITE_PARALLELISM_COOLDOWN_MS.key, "0")
    conf
  }

  private def newLoc(partitionId: Int, epoch: Int): PartitionLocation =
    new PartitionLocation(
      partitionId,
      epoch,
      "host",
      0,
      0,
      0,
      0,
      PartitionLocation.Mode.PRIMARY)

  private def seedActiveSiblings(
      lm: LifecycleManager,
      shuffleId: Int,
      partitionId: Int,
      epochs: Int*): Unit = {
    val locs = new util.ArrayList[PartitionLocation]()
    epochs.foreach(e => locs.add(newLoc(partitionId, e)))
    lm.updateLatestPartitionLocations(shuffleId, locs)
  }

  test("disabled: appendCount is always 1 (legacy)") {
    val lm = new LifecycleManager(appId, newConf(enabled = false))
    val cpm = new ChangePartitionManager(lm.conf, lm)
    val n = cpm.recordRealAllocationAndGetAppendCount(1, 1, 0L)
    Assert.assertEquals(1, n)
    lm.stop()
  }

  test("enabled: first real allocation upgrades P 1 -> 2") {
    val lm = new LifecycleManager(appId, newConf(enabled = true))
    val cpm = new ChangePartitionManager(lm.conf, lm)
    // seed one active sibling so getActiveSiblings().size == 1 (currentP=1)
    seedActiveSiblings(lm, 1, 1, 0)
    val n = cpm.recordRealAllocationAndGetAppendCount(1, 1, 1000L)
    // newP = min(1*2, 8) = 2; append = newP - currentP = 2 - 1 = 1
    Assert.assertEquals(1, n)
    lm.stop()
  }

  test("enabled: K(P)=ratio*P threshold; below threshold no upgrade") {
    // ratio=1.0, currentP=2 -> K=2. One revive in window -> below threshold -> no upgrade -> 1.
    val lm = new LifecycleManager(appId, newConf(enabled = true, max = 8, ratio = 1.0))
    val cpm = new ChangePartitionManager(lm.conf, lm)
    seedActiveSiblings(lm, 1, 1, 0, 1) // 2 active siblings => currentP=2
    val first = cpm.recordRealAllocationAndGetAppendCount(1, 1, 1000L) // inWindow=1 < K(2)=2
    Assert.assertEquals(1, first)
    val second = cpm.recordRealAllocationAndGetAppendCount(1, 1, 2000L) // inWindow=2 >= K(2)=2
    Assert.assertEquals(2, second) // upgrade 2 -> 4, append = 4 - 2 = 2
    lm.stop()
  }

  test("enabled: upgrade doubles P and is capped by max") {
    val lm = new LifecycleManager(appId, newConf(enabled = true, max = 4, ratio = 1.0))
    val cpm = new ChangePartitionManager(lm.conf, lm)
    // P=1 -> 2 (one revive reaches K(1)=1)
    seedActiveSiblings(lm, 1, 1, 0)
    Assert.assertEquals(1, cpm.recordRealAllocationAndGetAppendCount(1, 1, 1000L))
    // Now currentP=1 (active set still has 1 sibling; Driver would allocate the 2nd on the
    // revived allocation). Simulate P=2 by seeding a 2nd sibling, then drive K(2)=2 to upgrade
    // to 4 (capped). Window was cleared on prior upgrade, so need 2 fresh revives.
    seedActiveSiblings(lm, 1, 1, 0, 1) // currentP=2
    Assert.assertEquals(1, cpm.recordRealAllocationAndGetAppendCount(1, 1, 2000L)) // inWindow=1 < 2
    Assert.assertEquals(2, cpm.recordRealAllocationAndGetAppendCount(1, 1, 3000L)) // inWindow=2 >= 2 -> P=4, append=4-2=2
    // At max=4: further revives should not upgrade (capped). Seed 4 siblings, K(4)=4 reached.
    seedActiveSiblings(lm, 1, 1, 0, 1, 2, 3) // currentP=4
    Assert.assertEquals(1, cpm.recordRealAllocationAndGetAppendCount(1, 1, 4000L))
    Assert.assertEquals(1, cpm.recordRealAllocationAndGetAppendCount(1, 1, 5000L))
    Assert.assertEquals(1, cpm.recordRealAllocationAndGetAppendCount(1, 1, 6000L))
    Assert.assertEquals(1, cpm.recordRealAllocationAndGetAppendCount(1, 1, 7000L)) // inWindow=4>=K(4) but capped -> 1
    lm.stop()
  }

  test("enabled: cooldown suppresses immediate re-upgrade") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.CLIENT_DYNAMIC_WRITE_PARALLELISM_ENABLED.key, "true")
    conf.set(CelebornConf.CLIENT_DYNAMIC_WRITE_PARALLELISM_MAX.key, "8")
    conf.set(CelebornConf.CLIENT_DYNAMIC_WRITE_PARALLELISM_REVIVE_THRESHOLD_RATIO.key, "1.0")
    conf.set(CelebornConf.CLIENT_DYNAMIC_WRITE_PARALLELISM_COOLDOWN_MS.key, "10000")
    val lm = new LifecycleManager(appId, conf)
    val cpm = new ChangePartitionManager(conf, lm)
    seedActiveSiblings(lm, 1, 1, 0)
    // P=1 -> 2 (upgrade at t=1000, cooldown until 11000)
    Assert.assertEquals(1, cpm.recordRealAllocationAndGetAppendCount(1, 1, 1000L))
    // Immediately after, still in cooldown -> no upgrade even if window has entries
    seedActiveSiblings(lm, 1, 1, 0, 1) // currentP=2, K(2)=2
    Assert.assertEquals(1, cpm.recordRealAllocationAndGetAppendCount(1, 1, 2000L)) // inWindow=2 but cooldown
    Assert.assertEquals(1, cpm.recordRealAllocationAndGetAppendCount(1, 1, 3000L))
    lm.stop()
  }

  test("enabled: getActiveSiblings falls back to latest when feature disabled") {
    val lm = new LifecycleManager(appId, newConf(enabled = false))
    seedActiveSiblings(lm, 1, 1, 5)
    val siblings = lm.getActiveSiblings(1, 1)
    // Disabled: returns [latestPartitionLocation], activeSiblingsMap is not populated.
    Assert.assertEquals(1, siblings.size())
    Assert.assertEquals(5, siblings.get(0).getEpoch)
    lm.stop()
  }
}
