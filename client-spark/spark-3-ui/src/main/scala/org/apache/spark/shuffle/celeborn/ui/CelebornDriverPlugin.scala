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

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.celeborn.events.CelebornBuildInfoEvent

/**
 * Driver-side plugin. [[init]] registers the Celeborn listener to the status queue and posts a
 *  build-info event — these don't touch the Jetty server, so they are safe to run during plugin
 *  init (before SparkContext.attachAllHandlers). The UI tab is attached later in
 *  [[registerMetrics]], which Spark invokes AFTER attachAllHandlers, so the tab's page
 *  servlets are added once the server is fully started and Spark's own handlers are in place
 *  (mirrors Gluten's attachUI timing — see SubstraitBackend.registerMetrics). Attaching during
 *  init would race attachAllHandlers and throw IllegalStateException: STARTED on some Spark
 *  builds.
 */
class CelebornDriverPlugin extends DriverPlugin with Logging {
  private var _sc: Option[SparkContext] = None

  override def init(
      sc: SparkContext,
      ctx: PluginContext): java.util.Map[String, String] = {
    _sc = Some(sc)
    if (CelebornUIUtils.isUIEnabled(sc.conf)) {
      CelebornListener.register(sc)
    } else {
      logInfo("Celeborn Spark UI extension is disabled, skipping.")
    }
    Collections.emptyMap[String, String]
  }

  override def registerMetrics(appId: String, ctx: PluginContext): Unit = {
    _sc.foreach { sc =>
      if (CelebornUIUtils.isUIEnabled(sc.conf)) {
        // registerMetrics runs after SparkContext is fully initialized, so applicationId is
        // available here (it's null during init). Post the build-info event now and attach UI.
        sc.listenerBus.post(CelebornBuildInfoEvent(buildInfoMap(sc, appId)))
        CelebornUIUtils.attachUI(sc)
      }
    }
  }

  override def shutdown(): Unit = {}

  private def buildInfoMap(sc: SparkContext, appId: String): Map[String, String] = {
    val info = new JLinkedHashMap[String, String]()
    info.put("Spark Version", sc.version)
    // Prefer the appId passed to registerMetrics (reliable); fall back to sc.applicationId.
    info.put("Application Id", Option(appId).filter(_.nonEmpty).getOrElse(sc.applicationId))
    info.put("Celeborn UI Enabled", "true")
    // Derive key Celeborn runtime config from CelebornConf (rebuild from sc.conf via SparkUtils).
    // Celeborn has no ProjectConstants version class, so runtime config stands in for version —
    // it is more useful for diagnosing shuffle behavior anyway.
    try {
      val celebornConf = org.apache.spark.shuffle.celeborn.SparkUtils.fromSparkConf(sc.conf)
      info.put("Celeborn Compression Codec", celebornConf.shuffleCompressionCodec.toString)
      info.put("Celeborn Shuffle Writer Mode", celebornConf.shuffleWriterMode.toString)
      info.put("Celeborn Push Replicate Enabled", celebornConf.clientPushReplicateEnabled.toString)
      info.put("Celeborn Partition Split Mode", celebornConf.shufflePartitionSplitMode.toString)
      info.put(
        "Celeborn Partition Split Threshold",
        org.apache.spark.util.Utils.bytesToString(celebornConf.shufflePartitionSplitThreshold))
      info.put("Celeborn Fallback Policy", celebornConf.sparkShuffleFallbackPolicy.toString)
    } catch {
      case e: Throwable =>
        logWarning("Failed to derive Celeborn conf for build info", e)
    }
    info.asScala.toMap
  }
}
