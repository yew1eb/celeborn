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

package org.apache.spark.sql.celeborn

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite

class CelebornStageShuffleDependencyTrackerSuite extends AnyFunSuite {

  private def awaitUntil(
      condition: => Boolean,
      timeoutMs: Long = 10000,
      hint: String = ""): Unit = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (!condition && System.currentTimeMillis() < deadline) {
      Thread.sleep(50)
    }
    assert(condition, hint)
  }

  private def awaitNever(
      condition: => Boolean,
      checkMs: Long = 500,
      hint: String = ""): Unit = {
    val deadline = System.currentTimeMillis() + checkMs
    while (System.currentTimeMillis() < deadline) {
      assert(!condition, hint)
      Thread.sleep(50)
    }
  }

  test("cleanup is not triggered before the deletion delay elapses") {
    val cleanedShuffleId = new AtomicInteger(-1)
    // 10 minutes delay, far beyond this test's lifetime.
    val tracker = new CelebornStageShuffleDependencyTracker(
      10 * 60L * 1000L,
      shuffleId => cleanedShuffleId.set(shuffleId))

    val shuffleId = 1
    val writerStageId = 100
    val readerStageId = 200

    tracker.linkWriter(shuffleId, writerStageId)
    tracker.linkReader(shuffleId, readerStageId)
    tracker.removeStage(readerStageId)

    awaitNever(
      cleanedShuffleId.get() == shuffleId,
      hint = "Cleanup should not be triggered before the delay elapses")
    assert(tracker.getActiveShuffleCount == 1)
  }

  test("cleanup is triggered once the only reader stage completes") {
    val cleanedShuffleId = new AtomicInteger(-1)
    val tracker = new CelebornStageShuffleDependencyTracker(
      -1,
      shuffleId => cleanedShuffleId.set(shuffleId))

    val shuffleId = 1
    val writerStageId = 100
    val readerStageId = 200

    tracker.linkWriter(shuffleId, writerStageId)
    tracker.linkReader(shuffleId, readerStageId)
    tracker.removeStage(readerStageId)

    awaitUntil(
      cleanedShuffleId.get() == shuffleId,
      hint = "Cleanup should be triggered once the only reader stage completes")
    awaitUntil(
      tracker.getCleanedShuffleCount == 1,
      hint = "Cleaned shuffle count should be 1")
  }

  test("cleanup is triggered exactly once after the last reader stage completes") {
    val cleanupCount = new AtomicInteger(0)
    val tracker = new CelebornStageShuffleDependencyTracker(
      -1,
      _ => cleanupCount.incrementAndGet())

    val shuffleId = 2
    val writerStageId = 101
    val reader1 = 201
    val reader2 = 202

    tracker.linkWriter(shuffleId, writerStageId)
    tracker.linkReader(shuffleId, reader1)
    tracker.linkReader(shuffleId, reader2)

    tracker.removeStage(reader1)
    // Should not cleanup yet since reader2 is still running.
    awaitNever(
      cleanupCount.get() > 0,
      hint = "Cleanup should not be triggered while a reader stage is still running")

    tracker.removeStage(reader2)
    awaitUntil(
      cleanupCount.get() == 1,
      hint = "Cleanup should be triggered exactly once after the last reader completes")
    awaitNever(
      cleanupCount.get() > 1,
      hint = "Cleanup should not be triggered more than once")
  }

  test("removing an unknown stage does nothing") {
    val cleanupCount = new AtomicInteger(0)
    val tracker = new CelebornStageShuffleDependencyTracker(
      -1,
      _ => cleanupCount.incrementAndGet())

    tracker.removeStage(999)

    Thread.sleep(500)
    assert(cleanupCount.get() == 0)
  }

  test("multiple shuffles are cleaned up independently") {
    val cleanedSet = ConcurrentHashMap.newKeySet[Int]()
    val tracker = new CelebornStageShuffleDependencyTracker(
      -1,
      shuffleId => cleanedSet.add(shuffleId))

    tracker.linkWriter(1, 10)
    tracker.linkWriter(2, 20)
    tracker.linkReader(1, 101)
    tracker.linkReader(2, 201)

    tracker.removeStage(101)
    tracker.removeStage(201)

    awaitUntil(
      cleanedSet.contains(1) && cleanedSet.contains(2),
      hint = "Both shuffles should be cleaned up independently")
  }
}
