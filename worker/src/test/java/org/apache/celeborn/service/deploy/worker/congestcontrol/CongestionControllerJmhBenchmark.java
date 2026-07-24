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

package org.apache.celeborn.service.deploy.worker.congestcontrol;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

/**
 * JMH benchmark for celeborn's {@link CongestionController} rate-limit decision path.
 *
 * <p>{@link CongestionController#isUserCongested(UserCongestionControlContext)} is consulted on the
 * worker write hot path to decide whether a user should be throttled. It reads the worker-level
 * "over high watermark" flag, the user's produce speed (via {@link BufferStatusHub#avgBytesPerSec}),
 * and compares against watermarks. This benchmark measures that decision across a range of active
 * user counts, after pre-loading each user's buffer-status hub so the speed lookup is realistic.
 *
 * <p>Setup mirrors {@code TestCongestionController}: an anonymous subclass overrides
 * {@code getTotalPendingBytes}/{@code trimMemoryUsage} so the controller doesn't need a live
 * {@code MemoryManager}, and {@code shutDownCheckService()} stops the periodic check thread.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl worker -am test-compile
 *   build/mvn -pl worker exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.service.deploy.worker.congestcontrol.CongestionControllerJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class CongestionControllerJmhBenchmark {

  @Param({"1", "16", "128"})
  private int userCount;

  private CongestionController controller;
  private WorkerSource source;
  private UserCongestionControlContext[] contexts;
  private int index;

  @Setup
  public void setup() {
    CelebornConf celebornConf = new CelebornConf();
    celebornConf.set(
        CelebornConf.WORKER_CONGESTION_CONTROL_DISK_BUFFER_HIGH_WATERMARK().key(), "1000");
    celebornConf.set(
        CelebornConf.WORKER_CONGESTION_CONTROL_DISK_BUFFER_LOW_WATERMARK().key(), "500");
    celebornConf.set(
        CelebornConf.WORKER_CONGESTION_CONTROL_USER_PRODUCE_SPEED_HIGH_WATERMARK().key(),
        "20000");
    celebornConf.set(
        CelebornConf.WORKER_CONGESTION_CONTROL_USER_PRODUCE_SPEED_LOW_WATERMARK().key(), "10000");
    celebornConf.set(
        CelebornConf.WORKER_CONGESTION_CONTROL_WORKER_PRODUCE_SPEED_HIGH_WATERMARK().key(),
        "20000");
    celebornConf.set(
        CelebornConf.WORKER_CONGESTION_CONTROL_WORKER_PRODUCE_SPEED_LOW_WATERMARK().key(), "10000");
    celebornConf.set(
        CelebornConf.WORKER_CONGESTION_CONTROL_USER_INACTIVE_INTERVAL(), 2000L);

    source = new WorkerSource(celebornConf);
    controller =
        new CongestionController(source, 10, celebornConf, null) {
          @Override
          public long getTotalPendingBytes() {
            return 0L;
          }

          @Override
          public void trimMemoryUsage() {
            // no-op: avoid MemoryManager dependency
          }
        };
    controller.shutDownCheckService();

    // Build contexts for each user and pre-load a non-zero produce speed so avgBytesPerSec does
    // real work rather than the empty-hub fast path.
    contexts = new UserCongestionControlContext[userCount];
    long now = System.currentTimeMillis();
    for (int i = 0; i < userCount; i++) {
      UserIdentifier user = new UserIdentifier("tenant", "user-" + i);
      UserCongestionControlContext context = controller.getUserCongestionContext(user);
      controller.getUserBuffer(user).updateInfo(now, new BufferStatusHub.BufferStatusNode(1000));
      contexts[i] = context;
    }
    index = 0;
  }

  /** Round-robin over all user contexts — the per-push throttle decision. */
  @Benchmark
  public boolean isUserCongested(Blackhole blackhole) {
    boolean congested = controller.isUserCongested(contexts[index]);
    index = (index + 1) % userCount;
    blackhole.consume(congested);
    return congested;
  }

  /** Building/looking up a context for a user (computeIfAbsent path). */
  @Benchmark
  public UserCongestionControlContext getUserCongestionContext() {
    // Re-resolve an existing user's context; hits the computeIfAbsent fast path.
    return controller.getUserCongestionContext(new UserIdentifier("tenant", "user-0"));
  }

  @TearDown
  public void tearDown() {
    if (controller != null) {
      controller.close();
    }
    if (source != null) {
      source.destroy();
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
