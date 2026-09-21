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

package org.apache.celeborn.tests.spark

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.shuffle.celeborn.SparkShuffleManager
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClient}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode

class CelebornStageEndShuffleCleanupSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    // Always stop the session so that a failed test never leaks its SparkSession
    // (getOrCreate would otherwise reuse it in the next test).
    stopActiveSparkSessions()
  }

  private def lifecycleManager(): LifecycleManager = {
    SparkEnv.get.shuffleManager.asInstanceOf[SparkShuffleManager].getLifecycleManager
  }

  private def awaitShufflesUnregistered(lifecycleManager: LifecycleManager): Unit = {
    val deadline = System.currentTimeMillis() + 60000
    while (!lifecycleManager.registeredShuffle.isEmpty &&
      System.currentTimeMillis() < deadline) {
      Thread.sleep(500)
    }
    assert(
      lifecycleManager.registeredShuffle.isEmpty,
      "Shuffles should be unregistered after all their reader stages completed.")
  }

  private def stageEndShuffleCleanupConf: SparkConf = {
    val sparkConf = updateSparkConf(
      new SparkConf().setAppName("celeborn-test").setMaster("local[2]"),
      ShuffleMode.HASH)
    sparkConf.set(
      s"spark.${CelebornConf.CLIENT_SPARK_SHUFFLE_CLEANUP_ENABLED.key}",
      "true")
    sparkConf.set(
      s"spark.${CelebornConf.CLIENT_SPARK_SHUFFLE_CLEANUP_STAGE_LEVEL_ENABLED.key}",
      "true")
    // Delete immediately once the last reader stage completes.
    sparkConf.set(
      s"spark.${CelebornConf.CLIENT_SPARK_SHUFFLE_CLEANUP_STAGE_LEVEL_DELAYED_MINUTES.key}",
      "0")
    // Speed up the delayed unregister inside LifecycleManager.
    sparkConf.set(s"spark.${CelebornConf.SHUFFLE_EXPIRED_CHECK_INTERVAL.key}", "1s")
    sparkConf
  }

  test("shuffles are unregistered on stage completion for RDD jobs") {
    val spark = SparkSession.builder().config(stageEndShuffleCleanupConf).getOrCreate()

    val rdd = spark.sparkContext.parallelize(0 until 1000, 4).repartition(4)
    assert(rdd.count() == 1000)

    awaitShufflesUnregistered(lifecycleManager())
    spark.stop()
  }

  test("shuffles with multiple reader stages are unregistered after the last reader completes") {
    val spark = SparkSession.builder().config(stageEndShuffleCleanupConf).getOrCreate()

    val rdd1 = spark.sparkContext.parallelize(0 until 1000, 4).repartition(2)
    val rdd2 = rdd1.repartition(4)
    val rdd3 = rdd1.repartition(4)
    assert(rdd2.union(rdd3).count() == 2000)

    awaitShufflesUnregistered(lifecycleManager())
    spark.stop()
  }

  test("SQL queries coexist with stage-level cleanup") {
    val spark = SparkSession.builder().config(stageEndShuffleCleanupConf).getOrCreate()

    spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
    spark.sql("SELECT id % 10 AS k, COUNT(1) AS cnt FROM ta GROUP BY id % 10").collect()

    awaitShufflesUnregistered(lifecycleManager())
    spark.stop()
  }

  test("shuffles are kept when stage-level cleanup is disabled") {
    val sparkConf = updateSparkConf(
      new SparkConf().setAppName("celeborn-test").setMaster("local[2]"),
      ShuffleMode.HASH)
    sparkConf.set(
      s"spark.${CelebornConf.CLIENT_SPARK_SHUFFLE_CLEANUP_ENABLED.key}",
      "true")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val rdd = spark.sparkContext.parallelize(0 until 1000, 4).repartition(4)
    assert(rdd.count() == 1000)

    assert(
      !lifecycleManager().registeredShuffle.isEmpty,
      "Shuffles should still be registered when stage-level cleanup is disabled.")
    spark.stop()
  }
}
