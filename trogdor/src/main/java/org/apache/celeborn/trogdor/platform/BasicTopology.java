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

package org.apache.celeborn.trogdor.platform;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A basic topology implementation backed by a map of nodes. */
public class BasicTopology implements Topology {
  private final Map<String, Node> nodes;

  @JsonCreator
  public BasicTopology(@JsonProperty("nodes") Collection<BasicNode> nodes) {
    this.nodes =
        nodes == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(
                nodes.stream().collect(Collectors.toMap(Node::name, n -> n)));
  }

  @Override
  public Collection<Node> nodes() {
    return nodes.values();
  }

  @Override
  public Node node(String name) {
    return nodes.get(name);
  }
}
