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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.celeborn.trogdor.task.TaskSpec;

/** The state of a task on the coordinator. */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
public abstract class TaskState {
  public enum State {
    PENDING,
    RUNNING,
    STOPPING,
    DONE
  }

  private final TaskSpec spec;
  private final State state;

  protected TaskState(@JsonProperty("spec") TaskSpec spec, @JsonProperty("state") State state) {
    this.spec = spec;
    this.state = state;
  }

  @JsonProperty
  public TaskSpec spec() {
    return spec;
  }

  @JsonProperty
  public State state() {
    return state;
  }

  @JsonProperty
  public abstract JsonNode status();
}
