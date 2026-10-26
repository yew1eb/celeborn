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
 * Spark listener event carrying per-mapper metrics reported through MapperEnd, aggregated by the
 * Celeborn UI extension. Persisted to the event log and replayed by the History Server: keep the
 * no-arg constructor and bean getters/setters for Jackson, and add new metrics as fields so old
 * event logs and old readers stay compatible.
 */
public class CelebornMapperMetricsEvent implements SparkListenerEvent {

  private int shuffleId;

  // Bytes pushed before compression/encryption; 0 means not reported.
  private long uncompressedBytesWritten;

  // Bytes pushed after compression/encryption; 0 means not reported.
  private long compressedBytesWritten;

  // Write-path timing breakdown (ms); 0 means not reported (older clients).
  private long serializeTimeMs;
  private long copyTimeMs;
  private long compressTimeMs;
  private long queueWaitTimeMs;
  private long queueStallTimeMs;
  private long inflightWaitTimeMs;
  private long drainWaitTimeMs;
  // Max round-trip time of a single push, in ms.
  private long maxPushRttMs;

  public CelebornMapperMetricsEvent() {}

  public CelebornMapperMetricsEvent(
      int shuffleId, long uncompressedBytesWritten, long compressedBytesWritten) {
    this.shuffleId = shuffleId;
    this.uncompressedBytesWritten = uncompressedBytesWritten;
    this.compressedBytesWritten = compressedBytesWritten;
  }

  public CelebornMapperMetricsEvent(
      int shuffleId,
      long uncompressedBytesWritten,
      long compressedBytesWritten,
      long serializeTimeMs,
      long copyTimeMs,
      long compressTimeMs,
      long queueWaitTimeMs,
      long queueStallTimeMs,
      long inflightWaitTimeMs,
      long drainWaitTimeMs,
      long maxPushRttMs) {
    this.shuffleId = shuffleId;
    this.uncompressedBytesWritten = uncompressedBytesWritten;
    this.compressedBytesWritten = compressedBytesWritten;
    this.serializeTimeMs = serializeTimeMs;
    this.copyTimeMs = copyTimeMs;
    this.compressTimeMs = compressTimeMs;
    this.queueWaitTimeMs = queueWaitTimeMs;
    this.queueStallTimeMs = queueStallTimeMs;
    this.inflightWaitTimeMs = inflightWaitTimeMs;
    this.drainWaitTimeMs = drainWaitTimeMs;
    this.maxPushRttMs = maxPushRttMs;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public void setShuffleId(int shuffleId) {
    this.shuffleId = shuffleId;
  }

  public long getUncompressedBytesWritten() {
    return uncompressedBytesWritten;
  }

  public void setUncompressedBytesWritten(long uncompressedBytesWritten) {
    this.uncompressedBytesWritten = uncompressedBytesWritten;
  }

  public long getCompressedBytesWritten() {
    return compressedBytesWritten;
  }

  public void setCompressedBytesWritten(long compressedBytesWritten) {
    this.compressedBytesWritten = compressedBytesWritten;
  }

  public long getSerializeTimeMs() {
    return serializeTimeMs;
  }

  public void setSerializeTimeMs(long serializeTimeMs) {
    this.serializeTimeMs = serializeTimeMs;
  }

  public long getCopyTimeMs() {
    return copyTimeMs;
  }

  public void setCopyTimeMs(long copyTimeMs) {
    this.copyTimeMs = copyTimeMs;
  }

  public long getCompressTimeMs() {
    return compressTimeMs;
  }

  public void setCompressTimeMs(long compressTimeMs) {
    this.compressTimeMs = compressTimeMs;
  }

  public long getQueueWaitTimeMs() {
    return queueWaitTimeMs;
  }

  public void setQueueWaitTimeMs(long queueWaitTimeMs) {
    this.queueWaitTimeMs = queueWaitTimeMs;
  }

  public long getQueueStallTimeMs() {
    return queueStallTimeMs;
  }

  public void setQueueStallTimeMs(long queueStallTimeMs) {
    this.queueStallTimeMs = queueStallTimeMs;
  }

  public long getInflightWaitTimeMs() {
    return inflightWaitTimeMs;
  }

  public void setInflightWaitTimeMs(long inflightWaitTimeMs) {
    this.inflightWaitTimeMs = inflightWaitTimeMs;
  }

  public long getDrainWaitTimeMs() {
    return drainWaitTimeMs;
  }

  public void setDrainWaitTimeMs(long drainWaitTimeMs) {
    this.drainWaitTimeMs = drainWaitTimeMs;
  }

  public long getMaxPushRttMs() {
    return maxPushRttMs;
  }

  public void setMaxPushRttMs(long maxPushRttMs) {
    this.maxPushRttMs = maxPushRttMs;
  }

  @Override
  public boolean logEvent() {
    return true;
  }
}
