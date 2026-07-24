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

/** Benchmarks local Celeborn RPC round-trip latency using an echo endpoint. */
public class RpcBenchSpec extends TaskSpec {
  private final Set<String> targetNodes;
  private final long totalRpcs;
  private final String payload;

  @JsonCreator
  public RpcBenchSpec(
      @JsonProperty("startMs") long startMs,
      @JsonProperty("durationMs") long durationMs,
      @JsonProperty("targetNodes") Set<String> targetNodes,
      @JsonProperty("totalRpcs") long totalRpcs,
      @JsonProperty("payload") String payload) {
    super(startMs, durationMs);
    this.targetNodes = targetNodes;
    this.totalRpcs = Math.max(0, totalRpcs);
    this.payload = payload == null || payload.isEmpty() ? "hello" : payload;
  }

  @JsonProperty
  public Set<String> targetNodes() {
    return targetNodes;
  }

  @JsonProperty
  public long totalRpcs() {
    return totalRpcs;
  }

  @JsonProperty
  public String payload() {
    return payload;
  }

  @Override
  public TaskController newController(String id) {
    return topology -> targetNodes;
  }

  @Override
  public TaskWorker newTaskWorker(String id) {
    return new RpcBenchWorker(totalRpcs, payload);
  }
}
