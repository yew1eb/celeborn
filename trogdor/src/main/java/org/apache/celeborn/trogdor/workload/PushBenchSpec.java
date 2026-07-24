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

package org.apache.celeborn.trogdor.workload;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.celeborn.trogdor.task.TaskController;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.task.TaskWorker;

/** Push data to Celeborn workers as fast as possible. */
public class PushBenchSpec extends TaskSpec {
  private final Set<String> targetNodes;
  private final String masterHost;
  private final int masterPort;
  private final int numMappers;
  private final int numPartitions;
  private final int bytesPerPush;
  private final long totalPushes;
  private final String userIdentifier;

  @JsonCreator
  public PushBenchSpec(
      @JsonProperty("startMs") long startMs,
      @JsonProperty("durationMs") long durationMs,
      @JsonProperty("targetNodes") Set<String> targetNodes,
      @JsonProperty("masterHost") String masterHost,
      @JsonProperty("masterPort") int masterPort,
      @JsonProperty("numMappers") int numMappers,
      @JsonProperty("numPartitions") int numPartitions,
      @JsonProperty("bytesPerPush") int bytesPerPush,
      @JsonProperty("totalPushes") long totalPushes,
      @JsonProperty("userIdentifier") String userIdentifier) {
    super(startMs, durationMs);
    this.targetNodes = targetNodes;
    this.masterHost = masterHost;
    this.masterPort = masterPort;
    this.numMappers = Math.max(1, numMappers);
    this.numPartitions = Math.max(1, numPartitions);
    this.bytesPerPush = Math.max(1, bytesPerPush);
    this.totalPushes = Math.max(0, totalPushes);
    this.userIdentifier =
        userIdentifier == null || userIdentifier.isEmpty() ? "default" : userIdentifier;
  }

  @JsonProperty
  public Set<String> targetNodes() {
    return targetNodes;
  }

  @JsonProperty
  public String masterHost() {
    return masterHost;
  }

  @JsonProperty
  public int masterPort() {
    return masterPort;
  }

  @JsonProperty
  public int numMappers() {
    return numMappers;
  }

  @JsonProperty
  public int numPartitions() {
    return numPartitions;
  }

  @JsonProperty
  public int bytesPerPush() {
    return bytesPerPush;
  }

  @JsonProperty
  public long totalPushes() {
    return totalPushes;
  }

  @JsonProperty
  public String userIdentifier() {
    return userIdentifier;
  }

  @Override
  public TaskController newController(String id) {
    return topology -> targetNodes;
  }

  @Override
  public TaskWorker newTaskWorker(String id) {
    return new PushBenchWorker(
        masterHost,
        masterPort,
        numMappers,
        numPartitions,
        bytesPerPush,
        totalPushes,
        userIdentifier);
  }
}
