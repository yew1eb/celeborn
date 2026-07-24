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

package org.apache.celeborn.common;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.util.SplittableRandom;
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
 * JMH benchmark for {@link CelebornCRC32}. CelebornCRC32 is the checksum used by
 * {@link CommitMetadata} to incrementally accumulate a CRC32 across multiple data chunks, so the
 * benchmark exercises both the one-shot {@code compute} path and the streaming {@code addData}
 * accumulation path that the production code relies on.
 *
 * <p>Design follows the Kafka {@code Crc32CBenchmark} parameter matrix (seed / buffer length) but
 * measures celeborn's own checksum implementation rather than Kafka's {@code Crc32C}.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl common -am test-compile
 *   build/mvn -pl common exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.common.CelebornCRC32JmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class CelebornCRC32JmhBenchmark {

  /** Number of chunks accumulated per streaming invocation (mirrors CommitMetadata usage). */
  @Param({"1", "64", "1024"})
  private int chunks;

  @Param({"128", "1024", "4096", "65536"})
  private int bytes;

  @Param({"42"})
  private int seed;

  private byte[][] chunkData;

  @Setup
  public void setup() {
    SplittableRandom random = new SplittableRandom(seed);
    chunkData = new byte[chunks][];
    for (int c = 0; c < chunks; c++) {
      byte[] chunk = new byte[bytes];
      for (int o = 0; o < bytes; o++) {
        chunk[o] = (byte) random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1);
      }
      chunkData[c] = chunk;
    }
  }

  /**
   * One-shot checksum per chunk. This is what {@link CelebornCRC32#compute(byte[], int, int)} does
   * internally; note it allocates a fresh {@code CRC32} on every call.
   */
  @Benchmark
  public void computePerChunk(Blackhole blackhole) {
    for (int c = 0; c < chunks; c++) {
      blackhole.consume(CelebornCRC32.compute(chunkData[c], 0, bytes));
    }
  }

  /**
   * Streaming accumulation as used by {@link CommitMetadata}: a single CelebornCRC32 instance fed
   * chunk-by-chunk via {@code addData}, with the combined checksum read once at the end.
   */
  @Benchmark
  public void streamingAccumulate(Blackhole blackhole) {
    CelebornCRC32 crc = new CelebornCRC32();
    for (int c = 0; c < chunks; c++) {
      crc.addData(chunkData[c], 0, bytes);
    }
    blackhole.consume(crc.get());
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
