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

import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmark for celeborn's {@link TimeSlidingHub} / {@link BufferStatusHub}.
 *
 * <p>The time-sliding window is the core of worker-side congestion control: every produced byte
 * chunk calls {@code add(timestamp, node)} and the periodic check reads {@code avgBytesPerSec()}.
 * Both {@code add} and {@code sum} are {@code synchronized}, so this benchmark quantifies the
 * write-path cost and the read-path cost under a realistic "produce a chunk every few ms, advance
 * the clock across 1s buckets" workload.
 *
 * <p>{@code currentTimeMillis()} is overridden (it is {@code @VisibleForTesting protected}) so the
 * benchmark can drive the clock deterministically instead of sleeping, mirroring the
 * {@code TestTimeSlidingHub.DummyTimeSlidingHub} pattern.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl worker -am test-compile
 *   build/mvn -pl worker exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.service.deploy.worker.congestcontrol.TimeSlidingHubJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class TimeSlidingHubJmhBenchmark {

  @Param({"3", "10"})
  private int timeWindowSecs;

  @Param({"1024"})
  private long bytesPerChunk;

  /** Advance the clock by a few ms per add so the 1s buckets get exercised. */
  @Param({"10"})
  private long millisPerAdd;

  private ControllableBufferStatusHub hub;
  private long clock;

  @Setup
  public void setup() {
    clock = 0L;
    hub = new ControllableBufferStatusHub(timeWindowSecs);
  }

  /**
   * Write path: record a produced chunk. Every ~100 calls the clock crosses a 1s bucket boundary,
   * triggering the add/combine/expire bookkeeping in {@link TimeSlidingHub#add}.
   */
  @Benchmark
  public void add() {
    clock += millisPerAdd;
    hub.add(clock, new BufferStatusHub.BufferStatusNode(bytesPerChunk));
  }

  /**
   * Read path: the periodic check reads the average rate. Internally calls {@code sum()} which is
   * synchronized and runs {@code removeExpiredNodes}.
   */
  @Benchmark
  public long avgBytesPerSec() {
    return hub.avgBytesPerSec();
  }

  @Benchmark
  public void addAndRead(Blackhole blackhole) {
    clock += millisPerAdd;
    hub.add(clock, new BufferStatusHub.BufferStatusNode(bytesPerChunk));
    blackhole.consume(hub.avgBytesPerSec());
  }

  /** {@link BufferStatusHub} subclass with a controllable clock, so no real sleeping is needed. */
  private final class ControllableBufferStatusHub extends BufferStatusHub {
    ControllableBufferStatusHub(int timeWindowsInSecs) {
      super(timeWindowsInSecs);
    }

    @Override
    protected long currentTimeMillis() {
      return clock;
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
