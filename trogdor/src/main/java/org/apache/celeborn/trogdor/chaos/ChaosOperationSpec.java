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

package org.apache.celeborn.trogdor.chaos;

import java.util.Collections;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.celeborn.trogdor.task.TaskController;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.task.TaskWorker;

/** A single chaos operation scheduled by the coordinator and executed on one agent node. */
public class ChaosOperationSpec extends TaskSpec {
  public enum OperationType {
    BASH,
    OCCUPY_CPU
  }

  private final Set<String> targetNodes;
  private final OperationType operationType;
  private final String command;
  private final int cores;
  private final long occupyCpuDurationMs;

  @JsonCreator
  public ChaosOperationSpec(
      @JsonProperty("startMs") long startMs,
      @JsonProperty("durationMs") long durationMs,
      @JsonProperty("targetNodes") Set<String> targetNodes,
      @JsonProperty("operationType") OperationType operationType,
      @JsonProperty("command") String command,
      @JsonProperty("cores") int cores,
      @JsonProperty("occupyCpuDurationMs") long occupyCpuDurationMs) {
    super(startMs, durationMs);
    this.targetNodes = targetNodes == null ? Collections.emptySet() : targetNodes;
    this.operationType = operationType == null ? OperationType.BASH : operationType;
    this.command = command;
    this.cores = cores;
    this.occupyCpuDurationMs = occupyCpuDurationMs;
  }

  @JsonProperty
  public Set<String> targetNodes() {
    return targetNodes;
  }

  @JsonProperty
  public OperationType operationType() {
    return operationType;
  }

  @JsonProperty
  public String command() {
    return command;
  }

  @JsonProperty
  public int cores() {
    return cores;
  }

  @JsonProperty
  public long occupyCpuDurationMs() {
    return occupyCpuDurationMs;
  }

  @Override
  public TaskController newController(String id) {
    return topology -> targetNodes;
  }

  @Override
  public TaskWorker newTaskWorker(String id) {
    return new ChaosOperationWorker(operationType, command, cores, occupyCpuDurationMs);
  }
}
