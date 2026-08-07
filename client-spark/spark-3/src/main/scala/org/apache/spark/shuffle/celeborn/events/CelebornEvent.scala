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

package org.apache.spark.shuffle.celeborn.events

import org.apache.spark.scheduler.SparkListenerEvent

/**
 * Root for Celeborn UI events posted to the Spark listener bus. Defined in the
 *  spark-3 module (rather than spark-3-ui) so that [[org.apache.spark.shuffle.celeborn.SparkShuffleManager]]
 *  and [[org.apache.spark.shuffle.celeborn.CelebornShuffleFallbackPolicyRunner]] can post them
 *  without inverting the module dependency. Subclasses are case classes so they serialize into
 *  the Spark event log and can be replayed by the HistoryServer plugin.
 */
sealed trait CelebornEvent extends SparkListenerEvent

/** Posted once from the driver plugin at initialization with build/version info. */
case class CelebornBuildInfoEvent(info: Map[String, String]) extends CelebornEvent

/**
 * Posted from SparkShuffleManager.registerShuffle after a successful Celeborn slot assignment,
 *  carrying the shuffle -> worker topology.
 */
case class CelebornShuffleAssignmentEvent(
    appShuffleId: Int,
    celebornShuffleId: Int,
    workers: Seq[String],
    numPartitions: Int,
    timestamp: Long) extends CelebornEvent

/**
 * Posted from CelebornShuffleFallbackPolicyRunner when a shuffle falls back to
 *  SortShuffleManager, carrying a snapshot of per-policy fallback counts.
 */
case class CelebornFallbackEvent(
    fallbackCounts: Map[String, Long],
    timestamp: Long) extends CelebornEvent
