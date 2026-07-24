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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A basic platform implementation that runs commands locally via ProcessBuilder. */
public class BasicPlatform implements Platform {
  private final BasicTopology topology;
  private final String curNodeName;

  @JsonCreator
  public BasicPlatform(
      @JsonProperty("nodes") java.util.Collection<BasicNode> nodes,
      @JsonProperty("curNode") String curNodeName) {
    this.topology = new BasicTopology(nodes);
    this.curNodeName = curNodeName;
    if (topology.node(curNodeName) == null) {
      throw new IllegalArgumentException(
          "Current node " + curNodeName + " is not present in the topology.");
    }
  }

  @Override
  public Topology topology() {
    return topology;
  }

  @Override
  public Node curNode() {
    return topology.node(curNodeName);
  }

  @Override
  public String runCommand(String nodeName, String[] command) throws Exception {
    Node target = topology.node(nodeName);
    if (target == null) {
      throw new IllegalArgumentException("Unknown node: " + nodeName);
    }
    if (!nodeName.equals(curNodeName)) {
      throw new UnsupportedOperationException(
          "BasicPlatform can only run commands on the current node.");
    }
    ProcessBuilder pb = new ProcessBuilder(command);
    pb.redirectErrorStream(true);
    Process process = pb.start();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String output = reader.lines().collect(Collectors.joining("\n"));
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        throw new RuntimeException(
            "Command "
                + String.join(" ", command)
                + " exited with code "
                + exitCode
                + ": "
                + output);
      }
      return output;
    }
  }

  @Override
  public void close() {}
}
