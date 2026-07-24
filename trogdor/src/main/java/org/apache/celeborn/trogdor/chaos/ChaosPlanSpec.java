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

/**
 * A Trogdor task specification that submits a chaos testing verification plan to the coordinator.
 * The coordinator-side {@link ChaosOrchestrator} parses the plan and schedules individual chaos
 * operations as native Trogdor tasks onto the agents.
 */
public class ChaosPlanSpec extends TaskSpec {
  private final Set<String> targetNodes;
  private final String planJson;

  @JsonCreator
  public ChaosPlanSpec(
      @JsonProperty("startMs") long startMs,
      @JsonProperty("durationMs") long durationMs,
      @JsonProperty("targetNodes") Set<String> targetNodes,
      @JsonProperty("planJson") String planJson) {
    super(startMs, durationMs);
    this.targetNodes = targetNodes == null ? Collections.emptySet() : targetNodes;
    this.planJson = planJson == null ? "{}" : planJson;
  }

  @JsonProperty
  public Set<String> targetNodes() {
    return targetNodes;
  }

  @JsonProperty
  public String planJson() {
    return planJson;
  }

  @Override
  public TaskController newController(String id) {
    return topology -> targetNodes;
  }

  @Override
  public TaskWorker newTaskWorker(String id) {
    return new ChaosPlanWorker(id);
  }
}
