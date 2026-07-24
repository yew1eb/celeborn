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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;

import org.apache.celeborn.trogdor.rest.AgentStatusResponse;
import org.apache.celeborn.trogdor.rest.CreateWorkerRequest;
import org.apache.celeborn.trogdor.rest.DestroyWorkerRequest;
import org.apache.celeborn.trogdor.rest.JsonUtil;
import org.apache.celeborn.trogdor.rest.StopWorkerRequest;
import org.apache.celeborn.trogdor.rest.UptimeResponse;

/** A simple HTTP client for the Trogdor agent. */
public class AgentClient implements AutoCloseable {
  private final CloseableHttpClient httpClient;
  private final String baseUrl;

  public AgentClient(String host, int port) {
    this.httpClient = HttpClients.createDefault();
    this.baseUrl = "http://" + host + ":" + port + "/api/v1/trogdor/agent";
  }

  public AgentStatusResponse status() throws IOException {
    HttpGet request = new HttpGet(baseUrl + "/status");
    return execute(request, AgentStatusResponse.class);
  }

  public UptimeResponse uptime() throws IOException {
    HttpGet request = new HttpGet(baseUrl + "/uptime");
    return execute(request, UptimeResponse.class);
  }

  public void createWorker(CreateWorkerRequest req) throws IOException {
    HttpPost request = new HttpPost(baseUrl + "/workers");
    request.setEntity(new StringEntity(JsonUtil.toJsonString(req), ContentType.APPLICATION_JSON));
    executeVoid(request);
  }

  public void stopWorker(StopWorkerRequest req) throws IOException {
    HttpPut request = new HttpPut(baseUrl + "/workers/" + req.workerId() + "/stop");
    executeVoid(request);
  }

  public void destroyWorker(DestroyWorkerRequest req) throws IOException {
    HttpDelete request = new HttpDelete(baseUrl + "/workers/" + req.workerId());
    executeVoid(request);
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

  private static byte[] readAllBytes(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
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
