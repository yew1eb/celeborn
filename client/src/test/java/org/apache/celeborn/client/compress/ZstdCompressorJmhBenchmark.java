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

package org.apache.celeborn.client.compress;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.util.Arrays;
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
 * JMH benchmark for celeborn's {@link ZstdCompressor} / {@link ZstdDecompressor}.
 *
 * <p>Complements {@link Lz4CompressorJmhBenchmark} by measuring the Zstd codec path across the
 * same chunk-size matrix plus a compression-level parameter (mirroring Kafka KAFKA-7632 / KIP-390
 * compression-level support). Both the compression and decompression hot paths are exercised with
 * random input data.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl client -am test-compile
 *   build/mvn -pl client exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.client.compress.ZstdCompressorJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class ZstdCompressorJmhBenchmark {

  @Param({"65536", "262144", "1048576", "4194304"})
  private int chunkSize;

  @Param({"1", "3", "9"})
  private int level;

  @Param({"42"})
  private int seed;

  private byte[] data;
  private ZstdCompressor compressor;
  private ZstdDecompressor decompressor;
  private byte[][] compressedChunks;
  private byte[] decompressDst;

  @Setup
  public void setup() {
    int blockSize = 64 * 1024;
    compressor = new ZstdCompressor(blockSize, level);
    decompressor = new ZstdDecompressor();

    SplittableRandom random = new SplittableRandom(seed);
    data = new byte[chunkSize];
    for (int o = 0; o < chunkSize; o++) {
      data[o] = (byte) random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1);
    }

    // Pre-compress the chunk once so the decompression benchmark has a stable compressed input.
    compressor.compress(data, 0, chunkSize);
    byte[] compressed = compressor.getCompressedBuffer();
    int compressedLen = compressor.getCompressedTotalSize();
    compressedChunks = new byte[][] {Arrays.copyOf(compressed, compressedLen)};

    decompressDst = new byte[chunkSize];
  }

  @Benchmark
  public void compress(Blackhole blackhole) {
    compressor.compress(data, 0, chunkSize);
    blackhole.consume(compressor.getCompressedTotalSize());
    blackhole.consume(compressor.getCompressedBuffer());
  }

  @Benchmark
  public void decompress(Blackhole blackhole) throws Exception {
    for (byte[] compressed : compressedChunks) {
      blackhole.consume(decompressor.decompress(compressed, decompressDst, 0));
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
