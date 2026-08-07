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

package org.apache.spark.shuffle.celeborn.ui

import java.util.{Collections, LinkedHashMap => JLinkedHashMap}

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.celeborn.events.CelebornBuildInfoEvent

/**
 * Driver-side plugin. On init it registers the Celeborn listener to the status queue,
 *  attaches the Celeborn UI tab, and posts a build-info event. The LifecycleManager is
 *  created lazily later in [[org.apache.spark.shuffle.celeborn.SparkShuffleManager.registerShuffle]],
 *  so build info only carries SparkConf-derivable data; shuffle topology and fallback
 *  stats arrive later via assignment/fallback events.
 */
class CelebornDriverPlugin extends DriverPlugin with Logging {

  override def init(
      sc: SparkContext,
      ctx: PluginContext): java.util.Map[String, String] = {
    if (CelebornUIUtils.isUIEnabled(sc.conf)) {
      CelebornListener.register(sc)
      CelebornUIUtils.attachUI(sc)
      sc.listenerBus.post(CelebornBuildInfoEvent(buildInfoMap(sc)))
    } else {
      logInfo("Celeborn Spark UI extension is disabled, skipping.")
    }
    Collections.emptyMap[String, String]
  }

  override def shutdown(): Unit = {}

  private def buildInfoMap(sc: SparkContext): Map[String, String] = {
    val info = new JLinkedHashMap[String, String]()
    info.put("Spark Version", sc.version)
    info.put("Application Id", sc.applicationId)
    info.put("Celeborn UI Enabled", "true")
    info.asScala.toMap
  }
}
