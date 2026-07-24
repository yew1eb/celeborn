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

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/** JSON utility for Trogdor. */
public final class JsonUtil {
  private JsonUtil() {}

  public static final ObjectMapper JSON_SERDE = new ObjectMapper();

  static {
    JSON_SERDE.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    JSON_SERDE.enable(SerializationFeature.INDENT_OUTPUT);
    JSON_SERDE.registerModule(new JavaTimeModule());
    JSON_SERDE.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
  }

  public static String toJsonString(Object obj) {
    try {
      return JSON_SERDE.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String toPrettyJsonString(Object obj) {
    try {
      return JSON_SERDE.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T fromJson(String json, Class<T> clazz) {
    try {
      return JSON_SERDE.readValue(json, clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static JsonNode toJsonNode(Object obj) {
    return JSON_SERDE.valueToTree(obj);
  }

  public static ObjectNode objectNode() {
    return JSON_SERDE.createObjectNode();
  }
}
