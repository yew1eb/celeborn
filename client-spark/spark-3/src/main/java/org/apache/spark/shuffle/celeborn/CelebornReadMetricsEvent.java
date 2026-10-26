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

package org.apache.spark.shuffle.celeborn;

import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * Spark listener event carrying per-stream read-path metrics reported through
 * ReportShuffleReadMetrics, aggregated by the Celeborn UI extension into the "Shuffle Read Times"
 * section. Persisted to the event log and replayed by the History Server: keep the no-arg
 * constructor and bean getters/setters for Jackson, and add new metrics as fields so old event logs
 * and old readers stay compatible.
 */
public class CelebornReadMetricsEvent implements SparkListenerEvent {

  private int shuffleId;

  private long chunkWaitTimeMs;
  private long decompressTimeMs;
  private long deserializeTimeMs;
  private long copyTimeMs;
  private long retryCount;
  private long retryWaitTimeMs;
  private long peerSwitchCount;
  private long excludeCount;
  private long maxChunkRttMs;

  public CelebornReadMetricsEvent() {}

  public CelebornReadMetricsEvent(
      int shuffleId,
      long chunkWaitTimeMs,
      long decompressTimeMs,
      long deserializeTimeMs,
      long copyTimeMs,
      long retryCount,
      long retryWaitTimeMs,
      long peerSwitchCount,
      long excludeCount,
      long maxChunkRttMs) {
    this.shuffleId = shuffleId;
    this.chunkWaitTimeMs = chunkWaitTimeMs;
    this.decompressTimeMs = decompressTimeMs;
    this.deserializeTimeMs = deserializeTimeMs;
    this.copyTimeMs = copyTimeMs;
    this.retryCount = retryCount;
    this.retryWaitTimeMs = retryWaitTimeMs;
    this.peerSwitchCount = peerSwitchCount;
    this.excludeCount = excludeCount;
    this.maxChunkRttMs = maxChunkRttMs;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public void setShuffleId(int shuffleId) {
    this.shuffleId = shuffleId;
  }

  public long getChunkWaitTimeMs() {
    return chunkWaitTimeMs;
  }

  public void setChunkWaitTimeMs(long chunkWaitTimeMs) {
    this.chunkWaitTimeMs = chunkWaitTimeMs;
  }

  public long getDecompressTimeMs() {
    return decompressTimeMs;
  }

  public void setDecompressTimeMs(long decompressTimeMs) {
    this.decompressTimeMs = decompressTimeMs;
  }

  public long getDeserializeTimeMs() {
    return deserializeTimeMs;
  }

  public void setDeserializeTimeMs(long deserializeTimeMs) {
    this.deserializeTimeMs = deserializeTimeMs;
  }

  public long getCopyTimeMs() {
    return copyTimeMs;
  }

  public void setCopyTimeMs(long copyTimeMs) {
    this.copyTimeMs = copyTimeMs;
  }

  public long getRetryCount() {
    return retryCount;
  }

  public void setRetryCount(long retryCount) {
    this.retryCount = retryCount;
  }

  public long getRetryWaitTimeMs() {
    return retryWaitTimeMs;
  }

  public void setRetryWaitTimeMs(long retryWaitTimeMs) {
    this.retryWaitTimeMs = retryWaitTimeMs;
  }

  public long getPeerSwitchCount() {
    return peerSwitchCount;
  }

  public void setPeerSwitchCount(long peerSwitchCount) {
    this.peerSwitchCount = peerSwitchCount;
  }

  public long getExcludeCount() {
    return excludeCount;
  }

  public void setExcludeCount(long excludeCount) {
    this.excludeCount = excludeCount;
  }

  public long getMaxChunkRttMs() {
    return maxChunkRttMs;
  }

  public void setMaxChunkRttMs(long maxChunkRttMs) {
    this.maxChunkRttMs = maxChunkRttMs;
  }

  @Override
  public boolean logEvent() {
    return true;
  }
}
