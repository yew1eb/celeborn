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

package org.apache.celeborn.trogdor.workload;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import scala.Option;
import scala.reflect.ClassTag$;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.metrics.source.Role;
import org.apache.celeborn.common.protocol.RpcNameConstants;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.trogdor.platform.Platform;
import org.apache.celeborn.trogdor.rest.JsonUtil;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.task.TaskWorker;
import org.apache.celeborn.trogdor.task.WorkerStatusTracker;

/**
 * Benchmarks Celeborn RPC round-trip latency. A local echo endpoint is started in the agent JVM and
 * the worker repeatedly sends synchronous ask requests to it.
 */
public class RpcBenchWorker implements TaskWorker {
  private final long totalRpcs;
  private final String payload;

  private volatile RpcEnv rpcEnv;
  private volatile RpcEndpointRef echoRef;
  private volatile Thread workerThread;
  private volatile boolean stopped = false;

  public RpcBenchWorker(long totalRpcs, String payload) {
    this.totalRpcs = totalRpcs;
    this.payload = payload;
  }

  @Override
  public void start(
      Platform platform,
      TaskSpec spec,
      WorkerStatusTracker status,
      CompletableFuture<String> haltFuture)
      throws Exception {
    workerThread =
        new Thread(
            () -> {
              try {
                runBench(spec, status, haltFuture);
              } catch (Throwable t) {
                haltFuture.complete(t.getMessage());
              }
            },
            "RpcBenchWorker");
    workerThread.setDaemon(true);
    workerThread.start();
  }

  private void runBench(
      TaskSpec spec, WorkerStatusTracker status, CompletableFuture<String> haltFuture)
      throws Exception {
    CelebornConf conf = new CelebornConf();
    rpcEnv =
        RpcEnv.create(
            RpcNameConstants.SHUFFLE_CLIENT_SYS,
            "rpc-app-client",
            Utils.localHostName(conf),
            0,
            conf,
            Role.CLIENT(),
            Option.empty());
    EchoRpcEndpoint echoEndpoint = new EchoRpcEndpoint(rpcEnv);
    rpcEnv.setupEndpoint("echo", echoEndpoint);
    echoRef = rpcEnv.setupEndpointRef(rpcEnv.address(), "echo");

    AtomicLong rpcCount = new AtomicLong(0);
    AtomicLong totalLatencyNs = new AtomicLong(0);
    long endTimeMs =
        spec.durationMs() <= 0 ? Long.MAX_VALUE : System.currentTimeMillis() + spec.durationMs();

    long remaining = totalRpcs;
    while (!stopped && remaining != 0 && System.currentTimeMillis() < endTimeMs) {
      long startNs = System.nanoTime();
      String reply = (String) echoRef.askSync(payload, ClassTag$.MODULE$.apply(String.class));
      if (!payload.equals(reply)) {
        throw new IllegalStateException(
            "Echo reply mismatch: expected " + payload + " got " + reply);
      }
      totalLatencyNs.addAndGet(System.nanoTime() - startNs);
      rpcCount.incrementAndGet();
      if (remaining > 0) {
        remaining--;
      }
      if (rpcCount.get() % 1000 == 0) {
        updateStatus(status, rpcCount.get(), totalLatencyNs.get());
      }
    }

    updateStatus(status, rpcCount.get(), totalLatencyNs.get());
    haltFuture.complete("");
  }

  private void updateStatus(WorkerStatusTracker status, long count, long totalLatencyNs) {
    ObjectNode node = JsonUtil.JSON_SERDE.createObjectNode();
    node.put("rpcCount", count);
    node.put("totalLatencyNs", totalLatencyNs);
    if (count > 0) {
      node.put("avgLatencyNs", totalLatencyNs / count);
    }
    status.update(node);
  }

  @Override
  public void stop(Platform platform) throws Exception {
    stopped = true;
    if (workerThread != null) {
      workerThread.interrupt();
    }
    if (rpcEnv != null) {
      rpcEnv.shutdown();
    }
  }
}
