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

package org.apache.celeborn.service.deploy.worker.memory;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.celeborn.common.CelebornConf;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmark for celeborn's {@link BufferQueue}.
 *
 * <p>Each {@code MapPartitionData} owns a {@link BufferQueue} that pools read buffers locally and
 * overflows to the global pool. The {@code poll} / {@code recycle} round trip is on the read hot
 * path, and {@code poll} is {@code synchronized(buffers)} while {@code recycleToLocalPool} is
 * lock-free — so this benchmark measures both the synchronized poll cost and the
 * poll→recycleToLocalPool round trip.
 *
 * <p>{@link BufferQueue} holds a hard-coded {@link MemoryManager#instance()} reference, so the
 * benchmark must {@code MemoryManager.initialize(conf)} first (mirroring {@code MemoryManagerSuite})
 * and {@code MemoryManager.reset()} in teardown. {@code localBuffersTarget} is set high so
 * {@code recycle} takes the local-pool branch, avoiding the global dispatcher.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl worker -am test-compile
 *   build/mvn -pl worker exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.service.deploy.worker.memory.BufferQueueJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class BufferQueueJmhBenchmark {

  /** Number of buffers pooled in the queue. */
  @Param({"16", "256"})
  private int poolSize;

  @Param({"4096"})
  private int bufferSize;

  private BufferQueue bufferQueue;

  @Setup
  public void setup() {
    CelebornConf conf = new CelebornConf();
    MemoryManager.initialize(conf);
    bufferQueue = new BufferQueue();

    List<ByteBuf> initial = new ArrayList<>(poolSize);
    for (int i = 0; i < poolSize; i++) {
      initial.add(Unpooled.buffer(bufferSize, bufferSize));
    }
    bufferQueue.add(initial);
    // Route recycle to the local pool (avoid the global ReadBufferDispatcher path).
    bufferQueue.setLocalBuffersTarget(poolSize * 2);
  }

  /**
   * Synchronized poll then lock-free local recycle round trip — the core read-buffer acquire /
   * release cycle.
   */
  @Benchmark
  public void pollAndRecycleLocal(Blackhole blackhole) {
    ByteBuf buf = bufferQueue.poll();
    if (buf != null) {
      bufferQueue.recycleToLocalPool(buf);
    }
    blackhole.consume(buf);
  }

  /**
   * {@code recycle} dispatching to the local branch (numBuffersOccupied &lt;= localBuffersTarget),
   * then poll back out to keep the queue balanced.
   */
  @Benchmark
  public void recycleThenPoll(Blackhole blackhole) {
    ByteBuf buf = bufferQueue.poll();
    if (buf != null) {
      bufferQueue.recycle(buf);
    }
    blackhole.consume(buf);
  }

  @Benchmark
  public boolean bufferAvailable() {
    return bufferQueue.bufferAvailable();
  }

  @TearDown
  public void tearDown() {
    if (bufferQueue != null) {
      bufferQueue.release();
    }
    MemoryManager.reset();
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
