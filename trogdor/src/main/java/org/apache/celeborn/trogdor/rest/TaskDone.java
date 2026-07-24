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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.celeborn.trogdor.task.TaskSpec;

/** A task in the DONE state. */
public class TaskDone extends TaskState {
  private final long startedMs;
  private final long doneMs;
  private final String error;
  private final boolean cancelled;
  private final JsonNode status;

  public TaskDone(
      @JsonProperty("spec") TaskSpec spec,
      @JsonProperty("startedMs") long startedMs,
      @JsonProperty("doneMs") long doneMs,
      @JsonProperty("error") String error,
      @JsonProperty("cancelled") boolean cancelled,
      @JsonProperty("status") JsonNode status) {
    super(spec, State.DONE);
    this.startedMs = startedMs;
    this.doneMs = doneMs;
    this.error = error == null ? "" : error;
    this.cancelled = cancelled;
    this.status = status;
  }

  @JsonProperty
  public long startedMs() {
    return startedMs;
  }

  @JsonProperty
  public long doneMs() {
    return doneMs;
  }

  @JsonProperty
  public String error() {
    return error;
  }

  @JsonProperty
  public boolean cancelled() {
    return cancelled;
  }

  @Override
  public JsonNode status() {
    return status;
  }
}
