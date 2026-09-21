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

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.shuffle.celeborn.SparkShuffleManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClient}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode

class CelebornQueryEndShuffleCleanupSuite extends AnyFunSuite
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
      "Shuffles should be unregistered after the SQL query completes.")
  }

  private def queryEndShuffleCleanupConf(aqeEnabled: Boolean): SparkConf = {
    val sparkConf = updateSparkConf(
      new SparkConf().setAppName("celeborn-test").setMaster("local[2]"),
      ShuffleMode.HASH)
    sparkConf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, aqeEnabled.toString)
    sparkConf.set(
      s"spark.${CelebornConf.CLIENT_SPARK_SQL_QUERY_END_SHUFFLE_CLEANUP_ENABLED.key}",
      "true")
    // Speed up the delayed unregister inside LifecycleManager.
    sparkConf.set(s"spark.${CelebornConf.SHUFFLE_EXPIRED_CHECK_INTERVAL.key}", "1s")
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    sparkConf
  }

  Seq(true, false).foreach { aqeEnabled =>
    test(s"CELEBORN-2465: shuffles are unregistered on SQL query completion, " +
      s"aqeEnabled: $aqeEnabled") {
      val spark = SparkSession.builder().config(queryEndShuffleCleanupConf(aqeEnabled))
        .getOrCreate()

      spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
      spark.sql("SELECT id % 10 AS k, COUNT(1) AS cnt FROM ta GROUP BY id % 10").collect()
      awaitShufflesUnregistered(lifecycleManager())

      // A subsequent query must not be affected by the cleanup of the previous query.
      val result = spark.sql(
        """
          |SELECT k, SUM(cnt) FROM (
          |  SELECT id % 10 AS k, COUNT(1) AS cnt FROM ta GROUP BY id % 10
          |) t GROUP BY k
          |""".stripMargin).collect()
      assert(result.length == 10)
      awaitShufflesUnregistered(lifecycleManager())

      spark.stop()
    }
  }

  test("CELEBORN-2465: CTAS shuffles are unregistered on SQL query completion, " +
    "aqeEnabled: true") {
    val spark = SparkSession.builder().config(queryEndShuffleCleanupConf(true)).getOrCreate()

    try {
      // For CTAS / INSERT, the root of executedPlan is a DataWritingCommandExec wrapping an
      // AdaptiveSparkPlanExec, so the exchanges are not reachable without unwrapping the
      // AdaptiveSparkPlanExec nested in the tree.
      import org.apache.spark.sql.functions._
      spark.range(0, 1000, 1, 4)
        .withColumn("k", expr("id % 10"))
        .groupBy("k")
        .count()
        .write
        .mode("overwrite")
        .saveAsTable("celeborn_cleanup_ctas_test")
      awaitShufflesUnregistered(lifecycleManager())
    } finally {
      spark.sql("DROP TABLE IF EXISTS celeborn_cleanup_ctas_test")
    }

    spark.stop()
  }

  test("CELEBORN-2465: all shuffles of a multi-join query are unregistered, aqeEnabled: true") {
    val spark = SparkSession.builder().config(queryEndShuffleCleanupConf(true)).getOrCreate()

    spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
    spark.range(0, 1000, 1, 4).createOrReplaceTempView("tb")
    // Four shuffle joins in a row, so the final plan holds multiple shuffle query stages, and
    // exchange reuse makes some of them ReusedExchangeExec.
    val result = spark.sql(
      """
        |SELECT COUNT(*) FROM (
        |  SELECT a1.id AS k FROM ta a1
        |  JOIN tb b1 ON a1.id = b1.id
        |  JOIN ta a2 ON a1.id = a2.id
        |  JOIN tb b2 ON a1.id = b2.id
        |  JOIN ta a3 ON a1.id = a3.id
        |)
        |""".stripMargin).collect()
    assert(result.head.getLong(0) == 1000)
    awaitShufflesUnregistered(lifecycleManager())

    spark.stop()
  }

  test("CELEBORN-2465: shuffles are not unregistered on SQL query completion by default") {
    val sparkConf = updateSparkConf(
      new SparkConf().setAppName("celeborn-test").setMaster("local[2]"),
      ShuffleMode.HASH)
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
    spark.sql("SELECT id % 10 AS k, COUNT(1) AS cnt FROM ta GROUP BY id % 10").collect()

    assert(
      !lifecycleManager().registeredShuffle.isEmpty,
      "Shuffles should still be registered when query end shuffle cleanup is disabled.")

    spark.stop()
  }

  test("CELEBORN-2465: failed query only cleans up actually registered shuffles") {
    val spark = SparkSession.builder().config(queryEndShuffleCleanupConf(false)).getOrCreate()

    spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
    spark.udf.register(
      "fail_udf",
      (v: Long) => {
        throw new RuntimeException("intentional failure for test")
        v
      })

    // The query fails in the reduce stage after the shuffle has been written.
    assertThrows[SparkException] {
      spark.sql(
        """
          |SELECT k, fail_udf(cnt) FROM (
          |  SELECT id % 10 AS k, COUNT(1) AS cnt FROM ta GROUP BY id % 10
          |) t
          |""".stripMargin).collect()
    }

    // The listener must not break error propagation, and must clean up the materialized
    // shuffle of the failed query without touching non-registered shuffle ids.
    awaitShufflesUnregistered(lifecycleManager())

    spark.stop()
  }
}
