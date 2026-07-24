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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.celeborn.trogdor.rest.JsonUtil;

/** Configuration parser for Trogdor platforms. */
public final class PlatformConfig {
  private PlatformConfig() {}

  public static Platform parse(String nodeName, String configPath) throws Exception {
    if (configPath == null) {
      BasicNode node = new BasicNode(nodeName, "localhost", Collections.emptyMap());
      return new BasicPlatform(Collections.singletonList(node), nodeName);
    }
    Config config = JsonUtil.JSON_SERDE.readValue(new File(configPath), Config.class);
    if (config.nodes == null || config.nodes.isEmpty()) {
      throw new IllegalArgumentException("No nodes defined in config.");
    }
    List<BasicNode> nodes = new ArrayList<>();
    for (Map.Entry<String, JsonNode> entry : config.nodes.entrySet()) {
      String name = entry.getKey();
      JsonNode nodeJson = entry.getValue();
      String hostname = nodeJson.has("hostname") ? nodeJson.get("hostname").asText() : name;
      Map<String, String> nodeConfig =
          nodeJson.has("config")
              ? JsonUtil.JSON_SERDE.convertValue(nodeJson.get("config"), Map.class)
              : Collections.emptyMap();
      nodes.add(new BasicNode(name, hostname, nodeConfig));
    }
    return new BasicPlatform(nodes, nodeName);
  }

  static class Config {
    public final String platform;
    public final Map<String, JsonNode> nodes;

    @JsonCreator
    Config(
        @JsonProperty("platform") String platform,
        @JsonProperty("nodes") Map<String, JsonNode> nodes) {
      this.platform = platform;
      this.nodes = nodes;
    }
  }
}
