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

package org.apache.celeborn.client.read;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

/** Verifies the read-side timing accumulators surfaced in the Celeborn UI read metrics. */
public class ReadStreamStatsSuiteJ {

  @Test
  public void testDeserializeAndCopyTimeAccumulate() {
    ReadStreamStats stats = new ReadStreamStats();
    // Deserialization happens per record via TimedDeserializationStream; copies happen per
    // CelebornInputStream.read call. Both accumulate in nanos and report in millis.
    stats.addDeserializeTime(TimeUnit.MILLISECONDS.toNanos(2));
    stats.addDeserializeTime(TimeUnit.MILLISECONDS.toNanos(3));
    stats.addCopyTime(TimeUnit.MILLISECONDS.toNanos(1));
    stats.addCopyTime(TimeUnit.MILLISECONDS.toNanos(4));
    Assert.assertEquals(5L, stats.getDeserializeTimeMs());
    Assert.assertEquals(5L, stats.getCopyTimeMs());
  }

  @Test
  public void testEmptyStatsReportZero() {
    ReadStreamStats stats = new ReadStreamStats();
    Assert.assertEquals(0L, stats.getDeserializeTimeMs());
    Assert.assertEquals(0L, stats.getCopyTimeMs());
    Assert.assertEquals(0L, stats.getChunkWaitTimeMs());
    Assert.assertEquals(0L, stats.getMaxChunkRttMs());
    Assert.assertEquals(0L, stats.getRetryCount());
    Assert.assertEquals(0L, stats.getExcludeCount());
  }

  @Test
  public void testRetryAndExcludeCountersAccumulate() {
    ReadStreamStats stats = new ReadStreamStats();
    stats.incRetryCount();
    stats.incRetryCount();
    stats.incExcludeCount();
    stats.incPeerSwitchCount();
    stats.addRetryWaitTime(50L);
    stats.addRetryWaitTime(25L);
    Assert.assertEquals(2L, stats.getRetryCount());
    Assert.assertEquals(1L, stats.getExcludeCount());
    Assert.assertEquals(1L, stats.getPeerSwitchCount());
    Assert.assertEquals(75L, stats.getRetryWaitTimeMs());
  }

  @Test
  public void testChunkRttTracksMax() {
    ReadStreamStats stats = new ReadStreamStats();
    stats.recordChunkRtt(TimeUnit.MILLISECONDS.toNanos(5));
    stats.recordChunkRtt(TimeUnit.MILLISECONDS.toNanos(20));
    stats.recordChunkRtt(TimeUnit.MILLISECONDS.toNanos(9));
    Assert.assertEquals(20L, stats.getMaxChunkRttMs());
  }
}
