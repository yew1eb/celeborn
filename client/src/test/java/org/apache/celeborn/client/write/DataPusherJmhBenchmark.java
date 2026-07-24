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

package org.apache.celeborn.client.write;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

import org.apache.celeborn.client.DummyShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.Utils;

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

import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for celeborn's {@link DataPusher} client write path.
 *
 * <p>{@link DataPusher} is the client-side push buffer accumulator (the celeborn analogue of Kafka's
 * {@code RecordAccumulator}): {@code addTask} polls a reusable {@link PushTask} from the idle pool,
 * copies the record bytes in, and offers it to the working queue; a background push thread drains
 * the working queue. This benchmark isolates the {@code addTask} enqueue hot path by overriding
 * {@code pushData} to a no-op (no RPC), so the measurement captures idle-queue pool reuse, the
 * {@code arraycopy} into the pooled buffer, and the working-queue offer / drain round trip.
 *
 * <p>Setup mirrors {@code DataPushQueueSuiteJ}: a {@link DummyShuffleClient} (main-scope stub)
 * with {@code initReducePartitionMap}, and a {@link DataPusher} subclass whose {@code pushData} is
 * a no-op so no real network I/O occurs.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl client -am test-compile
 *   build/mvn -pl client exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.client.write.DataPusherJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class DataPusherJmhBenchmark {

  @Param({"256", "4096", "65536"})
  private int bytes;

  private static final int SHUFFLE_ID = 0;
  private static final int MAP_ID = 0;
  private static final int ATTEMPT_ID = 0;
  private static final int NUM_MAPPERS = 10;
  private static final int NUM_PARTITIONS = 100;
  private static final int NUM_WORKERS = 4;

  private DummyShuffleClient client;
  private DataPusher dataPusher;
  private byte[] buffer;
  private int partitionId;

  @Setup
  public void setup() throws Exception {
    CelebornConf conf = new CelebornConf();
    File tempFile =
        new File(
            Utils.createTempDir(System.getProperty("java.io.tmpdir"), "celeborn_jmh"),
            UUID.randomUUID().toString());
    client = new DummyShuffleClient(conf, tempFile);
    client.initReducePartitionMap(SHUFFLE_ID, NUM_PARTITIONS, NUM_WORKERS);

    LongAdder[] mapStatusLengths = new LongAdder[NUM_PARTITIONS];
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      mapStatusLengths[i] = new LongAdder();
    }

    // No-op pushData: isolate addTask enqueue path from any RPC work.
    dataPusher =
        new DataPusher(
            SHUFFLE_ID,
            MAP_ID,
            ATTEMPT_ID,
            0L,
            NUM_MAPPERS,
            NUM_PARTITIONS,
            conf,
            client,
            null,
            integer -> {},
            mapStatusLengths) {
          @Override
          protected void pushData(PushTask task) throws IOException {
            // no-op: do not invoke client.pushData
          }
        };

    buffer = new byte[bytes];
    for (int i = 0; i < bytes; i++) {
      buffer[i] = (byte) i;
    }
    partitionId = 0;
  }

  @Benchmark
  public void addTask(Blackhole blackhole) throws Exception {
    dataPusher.addTask(partitionId, buffer, bytes);
  }

  @TearDown
  public void tearDown() throws Exception {
    if (dataPusher != null) {
      dataPusher.waitOnTermination();
    }
    if (client != null) {
      client.shutdown();
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
