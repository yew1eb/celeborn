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

package org.apache.celeborn.trogdor.rest;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Request to submit a chaos plan to the orchestrator. */
public class SubmitChaosPlanRequest {
  private final String planId;
  private final String planJson;
  private final Set<String> targetNodes;

  public SubmitChaosPlanRequest(
      @JsonProperty("planId") String planId,
      @JsonProperty("planJson") String planJson,
      @JsonProperty("targetNodes") Set<String> targetNodes) {
    this.planId = planId;
    this.planJson = planJson;
    this.targetNodes = targetNodes;
  }

  @JsonProperty
  public String planId() {
    return planId;
  }

  @JsonProperty
  public String planJson() {
    return planJson;
  }

  @JsonProperty
  public Set<String> targetNodes() {
    return targetNodes;
  }
}
