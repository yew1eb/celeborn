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

import scala.Option;

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
 * JMH benchmark for celeborn's {@link Lz4Compressor} / {@link Lz4Decompressor}.
 *
 * <p>Replaces the older Scala {@code LZ4TPCDSDataBenchmark} (hand-rolled timing framework) with a
 * standard JMH benchmark over the same chunk-size matrix (64k / 256k / 1m / 4m), exercising both
 * the compression and decompression hot paths with random input data. The compressor/decompressor
 * instances and buffers are reused across iterations the same way the production push path reuses
 * them.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl client -am test-compile
 *   build/mvn -pl client exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.client.compress.Lz4CompressorJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class Lz4CompressorJmhBenchmark {

  @Param({"65536", "262144", "1048576", "4194304"})
  private int chunkSize;

  @Param({"42"})
  private int seed;

  private byte[] data;
  private Lz4Compressor compressor;
  private Lz4Decompressor decompressor;
  private byte[][] compressedChunks;
  private byte[] decompressDst;

  @Setup
  public void setup() {
    // Use the default configured push buffer size as the compressor's working buffer, matching
    // the production Lz4Compressor instantiation in Compressor.getCompressor.
    int blockSize = 64 * 1024;
    compressor = new Lz4Compressor(blockSize);
    decompressor = new Lz4Decompressor(Option.empty());

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
