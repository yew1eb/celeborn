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

package org.apache.celeborn.trogdor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Test;

import org.apache.celeborn.trogdor.fault.*;
import org.apache.celeborn.trogdor.rest.JsonUtil;
import org.apache.celeborn.trogdor.task.TaskSpec;
import org.apache.celeborn.trogdor.workload.*;

public class TrogdorWorkloadFaultTest {

  @Test
  public void testRoundTripPushBenchSpec() {
    PushBenchSpec spec =
        new PushBenchSpec(
            0,
            60000,
            new HashSet<>(Arrays.asList("node1", "node2")),
            "master",
            9097,
            4,
            100,
            1024,
            10000,
            "tenant:user");
    String json = JsonUtil.toJsonString(spec);
    TaskSpec roundTripped = JsonUtil.fromJson(json, TaskSpec.class);
    assertTrue(roundTripped instanceof PushBenchSpec);
    PushBenchSpec push = (PushBenchSpec) roundTripped;
    assertEquals("master", push.masterHost());
    assertEquals(9097, push.masterPort());
    assertEquals(4, push.numMappers());
    assertEquals(100, push.numPartitions());
    assertEquals(1024, push.bytesPerPush());
    assertEquals(10000, push.totalPushes());
    assertEquals("tenant:user", push.userIdentifier());
  }

  @Test
  public void testRoundTripFetchBenchSpec() {
    FetchBenchSpec spec =
        new FetchBenchSpec(
            0, 60000, Collections.singleton("node1"), "master", 9097, 10, 1000, "default:default");
    String json = JsonUtil.toJsonString(spec);
    TaskSpec roundTripped = JsonUtil.fromJson(json, TaskSpec.class);
    assertTrue(roundTripped instanceof FetchBenchSpec);
    FetchBenchSpec fetch = (FetchBenchSpec) roundTripped;
    assertEquals(10, fetch.numPartitions());
    assertEquals(1000, fetch.fetchesPerPartition());
  }

  @Test
  public void testRoundTripRpcBenchSpec() {
    RpcBenchSpec spec = new RpcBenchSpec(0, 30000, Collections.singleton("node1"), 5000, "ping");
    String json = JsonUtil.toJsonString(spec);
    TaskSpec roundTripped = JsonUtil.fromJson(json, TaskSpec.class);
    assertTrue(roundTripped instanceof RpcBenchSpec);
    RpcBenchSpec rpc = (RpcBenchSpec) roundTripped;
    assertEquals(5000, rpc.totalRpcs());
    assertEquals("ping", rpc.payload());
  }

  @Test
  public void testRoundTripProcessStopFaultSpec() {
    ProcessStopFaultSpec spec =
        new ProcessStopFaultSpec(
            0, 10000, new HashSet<>(Arrays.asList("node1", "node2")), "celeborn-worker");
    String json = JsonUtil.toJsonString(spec);
    TaskSpec roundTripped = JsonUtil.fromJson(json, TaskSpec.class);
    assertTrue(roundTripped instanceof ProcessStopFaultSpec);
    ProcessStopFaultSpec fault = (ProcessStopFaultSpec) roundTripped;
    assertEquals("celeborn-worker", fault.processName());
  }

  @Test
  public void testRoundTripNetworkPartitionFaultSpec() {
    NetworkPartitionFaultSpec spec =
        new NetworkPartitionFaultSpec(
            0,
            10000,
            new HashSet<>(Arrays.asList("node1")),
            new HashSet<>(Arrays.asList("node2", "node3")));
    String json = JsonUtil.toJsonString(spec);
    TaskSpec roundTripped = JsonUtil.fromJson(json, TaskSpec.class);
    assertTrue(roundTripped instanceof NetworkPartitionFaultSpec);
    NetworkPartitionFaultSpec fault = (NetworkPartitionFaultSpec) roundTripped;
    assertEquals(new HashSet<>(Arrays.asList("node2", "node3")), fault.blockedNodes());
  }

  @Test
  public void testRoundTripDiskSlowFaultSpec() {
    DiskSlowFaultSpec spec =
        new DiskSlowFaultSpec(0, 10000, new HashSet<>(Arrays.asList("node1")), "/dev/sda", 100);
    String json = JsonUtil.toJsonString(spec);
    TaskSpec roundTripped = JsonUtil.fromJson(json, TaskSpec.class);
    assertTrue(roundTripped instanceof DiskSlowFaultSpec);
    DiskSlowFaultSpec fault = (DiskSlowFaultSpec) roundTripped;
    assertEquals("/dev/sda", fault.device());
    assertEquals(100, fault.delayMs());
  }

  @Test
  public void testRoundTripExternalCommandFaultSpec() {
    ExternalCommandFaultSpec spec =
        new ExternalCommandFaultSpec(
            0,
            10000,
            new HashSet<>(Arrays.asList("node1")),
            new String[] {"sleep", "5"},
            Collections.singletonMap("KEY", "VALUE"));
    String json = JsonUtil.toJsonString(spec);
    TaskSpec roundTripped = JsonUtil.fromJson(json, TaskSpec.class);
    assertTrue(roundTripped instanceof ExternalCommandFaultSpec);
    ExternalCommandFaultSpec fault = (ExternalCommandFaultSpec) roundTripped;
    assertTrue(Arrays.equals(new String[] {"sleep", "5"}, fault.command()));
    assertEquals("VALUE", fault.env().get("KEY"));
  }
}
