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

package org.apache.celeborn.trogdor.service

import java.io.File

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{ShutdownHookManager, Utils}
import org.apache.celeborn.trogdor.platform.{BasicPlatform, PlatformConfig}

object TrogdorCoordinatorApp extends Logging {
  def main(args: Array[String]): Unit = {
    val (confPath, nodeName) = parseArgs(args)
    val conf = new CelebornConf()
    if (confPath != null && !confPath.isEmpty) {
      loadConfig(confPath).foreach { case (k, v) => conf.set(k, v) }
    }
    val platform = PlatformConfig.parse(nodeName, confPath)
    val coordinator = new TrogdorCoordinator(conf, platform)
    try {
      coordinator.initialize()
      logInfo(s"TrogdorCoordinator started on ${coordinator.connectionUrl}")
      ShutdownHookManager.get().addShutdownHook(
        new Runnable {
          override def run(): Unit = {
            logInfo("Shutting down TrogdorCoordinator.")
            coordinator.stop(0)
          }
        },
        0)
    } catch {
      case e: Throwable =>
        logError("Failed to start TrogdorCoordinator.", e)
        coordinator.stop(1)
        System.exit(1)
    }
  }

  private def parseArgs(args: Array[String]): (String, String) = {
    var confPath: String = null
    var nodeName: String = null
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "-c" | "--config" =>
          i += 1
          confPath = args(i)
        case "-n" | "--node-name" =>
          i += 1
          nodeName = args(i)
        case _ =>
      }
      i += 1
    }
    if (nodeName == null || nodeName.isEmpty) {
      throw new IllegalArgumentException("--node-name must be specified.")
    }
    (confPath, nodeName)
  }

  private def loadConfig(confPath: String): Map[String, String] = {
    Utils.getPropertiesFromFile(confPath)
  }
}
