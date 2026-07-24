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

package org.apache.celeborn.trogdor.chaos.conf

import java.io.File

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.util.Utils

/**
 * Thin wrapper around [[CelebornConf]] for chaos testing settings.
 * Kept to minimize changes when migrating from the original verifier module.
 */
class ChaosConf(val celebornConf: CelebornConf) {

  def runnerTestMode: Boolean = {
    celebornConf.get("celeborn.trogdor.chaos.runner.test.mode", "false").toBoolean
  }

  def planActionBadInflightFile: String = {
    celebornConf.get(
      "celeborn.trogdor.chaos.plan.action.block.bad.inflight.location",
      "/root/badblock/inflight")
  }

  def planActionDefaultInterval: String = {
    celebornConf.get("celeborn.trogdor.chaos.plan.action.default.interval", "5s")
  }

  def planActionOccupyCpuMaxDurationMs: Long = {
    Utils.timeStringAsMs(
      celebornConf.get("celeborn.trogdor.chaos.plan.action.occupycpu.maxduration", "120s"))
  }

  def planActionSelectorDefaultInterval: String = {
    celebornConf.get("celeborn.trogdor.chaos.plan.action.selector.default.interval", "5s")
  }

  def planParticipantDurationMs: Long = {
    Utils.timeStringAsMs(
      celebornConf.get("celeborn.trogdor.chaos.plan.participant.duration", "5m"))
  }

  def startMasterScript: String = {
    val default =
      ChaosConf.defaultScriptsLocation.map(_ + File.separator + "start-master.sh").orNull
    celebornConf.get("celeborn.trogdor.chaos.scripts.master.start.script", default)
  }

  def stopMasterScript: String = {
    val default =
      ChaosConf.defaultScriptsLocation.map(_ + File.separator + "stop-master.sh").orNull
    celebornConf.get("celeborn.trogdor.chaos.scripts.master.stop.script", default)
  }

  def startWorkerScript: String = {
    val default =
      ChaosConf.defaultScriptsLocation.map(_ + File.separator + "start-worker.sh").orNull
    celebornConf.get("celeborn.trogdor.chaos.scripts.worker.start.script", default)
  }

  def stopWorkerScript: String = {
    val default =
      ChaosConf.defaultScriptsLocation.map(_ + File.separator + "stop-worker.sh").orNull
    celebornConf.get("celeborn.trogdor.chaos.scripts.worker.stop.script", default)
  }
}

object ChaosConf {

  private val defaultScriptsLocation: Option[String] =
    sys.env.get("CELEBORN_HOME").map(home => s"$home${File.separator}sbin").filter { dir =>
      new File(dir).isDirectory
    }
}
