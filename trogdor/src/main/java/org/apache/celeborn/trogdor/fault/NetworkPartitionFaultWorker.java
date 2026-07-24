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

package org.apache.celeborn.trogdor.fault;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.node.TextNode;

import org.apache.celeborn.trogdor.platform.Platform;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.task.TaskWorker;
import org.apache.celeborn.trogdor.task.WorkerStatusTracker;

/** Blocks traffic to a set of nodes using iptables. */
public class NetworkPartitionFaultWorker implements TaskWorker {
  private final Set<String> blockedNodes;
  private volatile Thread workerThread;
  private volatile boolean stopped = false;

  public NetworkPartitionFaultWorker(Set<String> blockedNodes) {
    this.blockedNodes = blockedNodes;
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
                List<String> blockedIps = resolveIps(platform, blockedNodes);
                status.update(new TextNode("blocking ips: " + blockedIps));
                for (String ip : blockedIps) {
                  iptables("-A", ip);
                }
                long duration = Math.max(0, spec.durationMs());
                if (duration <= 0) {
                  while (!stopped) {
                    Thread.sleep(100);
                  }
                } else {
                  Thread.sleep(duration);
                }
                for (String ip : blockedIps) {
                  iptables("-D", ip);
                }
                status.update(new TextNode("unblocked ips: " + blockedIps));
                haltFuture.complete("");
              } catch (Throwable t) {
                haltFuture.complete(t.getMessage());
              }
            },
            "NetworkPartitionFaultWorker");
    workerThread.setDaemon(true);
    workerThread.start();
  }

  @Override
  public void stop(Platform platform) throws Exception {
    stopped = true;
    if (workerThread != null) {
      workerThread.interrupt();
    }
  }

  private List<String> resolveIps(Platform platform, Set<String> nodeNames) {
    List<String> ips = new ArrayList<>();
    for (String nodeName : nodeNames) {
      org.apache.celeborn.trogdor.platform.Node node = platform.topology().node(nodeName);
      if (node != null) {
        ips.add(node.hostname());
      } else {
        ips.add(nodeName);
      }
    }
    return ips;
  }

  private void iptables(String action, String ip) throws Exception {
    ProcessBuilder pb =
        new ProcessBuilder("sudo", "iptables", action, "OUTPUT", "-d", ip, "-j", "DROP");
    pb.inheritIO();
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException("iptables failed with code " + exitCode);
    }
  }
}
