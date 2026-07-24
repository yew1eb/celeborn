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

package org.apache.celeborn.trogdor.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;

import org.apache.celeborn.trogdor.rest.ChaosPlanResponse;
import org.apache.celeborn.trogdor.rest.CoordinatorStatusResponse;
import org.apache.celeborn.trogdor.rest.CreateTaskRequest;
import org.apache.celeborn.trogdor.rest.DestroyTaskRequest;
import org.apache.celeborn.trogdor.rest.JsonUtil;
import org.apache.celeborn.trogdor.rest.StopTaskRequest;
import org.apache.celeborn.trogdor.rest.SubmitChaosPlanRequest;
import org.apache.celeborn.trogdor.rest.TaskRequest;
import org.apache.celeborn.trogdor.rest.TaskState;
import org.apache.celeborn.trogdor.rest.TasksResponse;
import org.apache.celeborn.trogdor.rest.UptimeResponse;

/** A simple HTTP client for the Trogdor coordinator. */
public class CoordinatorClient implements AutoCloseable {
  private final CloseableHttpClient httpClient;
  private final String baseUrl;

  public CoordinatorClient(String host, int port) {
    this.httpClient = HttpClients.createDefault();
    this.baseUrl = "http://" + host + ":" + port + "/api/v1/trogdor/coordinator";
  }

  public CoordinatorStatusResponse status() throws IOException {
    return execute(new HttpGet(baseUrl + "/status"), CoordinatorStatusResponse.class);
  }

  public UptimeResponse uptime() throws IOException {
    return execute(new HttpGet(baseUrl + "/uptime"), UptimeResponse.class);
  }

  public void createTask(CreateTaskRequest req) throws IOException {
    HttpPost request = new HttpPost(baseUrl + "/tasks");
    request.setEntity(new StringEntity(JsonUtil.toJsonString(req), ContentType.APPLICATION_JSON));
    executeVoid(request);
  }

  public void stopTask(StopTaskRequest req) throws IOException {
    HttpPut request = new HttpPut(baseUrl + "/tasks/" + req.id() + "/stop");
    executeVoid(request);
  }

  public void destroyTask(DestroyTaskRequest req) throws IOException {
    HttpDelete request = new HttpDelete(baseUrl + "/tasks/" + req.id());
    executeVoid(request);
  }

  public TasksResponse tasks() throws IOException {
    return execute(new HttpGet(baseUrl + "/tasks"), TasksResponse.class);
  }

  public TaskState task(TaskRequest req) throws IOException {
    return execute(new HttpGet(baseUrl + "/tasks/" + req.id()), TaskState.class);
  }

  public void submitChaosPlan(SubmitChaosPlanRequest req) throws IOException {
    HttpPost request = new HttpPost(baseUrl + "/chaos/plans");
    request.setEntity(new StringEntity(JsonUtil.toJsonString(req), ContentType.APPLICATION_JSON));
    executeVoid(request);
  }

  public ChaosPlanResponse showChaosPlan(String planId) throws IOException {
    return execute(new HttpGet(baseUrl + "/chaos/plans/" + planId), ChaosPlanResponse.class);
  }

  public void stopChaosPlan(String planId) throws IOException {
    executeVoid(new HttpPut(baseUrl + "/chaos/plans/" + planId + "/stop"));
  }

  private void executeVoid(org.apache.hc.core5.http.ClassicHttpRequest request) throws IOException {
    httpClient.execute(
        request,
        response -> {
          int code = response.getCode();
          if (code < 200 || code >= 300) {
            throw new IOException("Unexpected response code: " + code);
          }
          return null;
        });
  }

  private <T> T execute(org.apache.hc.core5.http.ClassicHttpRequest request, Class<T> responseClass)
      throws IOException {
    return httpClient.execute(
        request,
        response -> {
          int code = response.getCode();
          if (code < 200 || code >= 300) {
            throw new IOException("Unexpected response code: " + code);
          }
          String body =
              new String(readAllBytes(response.getEntity().getContent()), StandardCharsets.UTF_8);
          return JsonUtil.fromJson(body, responseClass);
        });
  }

  private static byte[] readAllBytes(java.io.InputStream in) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int read;
    while ((read = in.read(buffer)) != -1) {
      out.write(buffer, 0, read);
    }
    return out.toByteArray();
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }
}
