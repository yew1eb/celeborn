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

package org.apache.celeborn.common.network.buffer;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.util.TransportConf;
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

/**
 * JMH benchmark for celeborn's {@link FileSegmentManagedBuffer} — the file-backed read path.
 *
 * <p>{@link FileSegmentManagedBuffer} backs a read chunk with a segment of a file. Its hot methods
 * are {@code convertToNetty} (zero-copy {@link io.netty.channel.DefaultFileRegion}), {@code
 * nioByteBuffer} (small files read into a heap buffer, large files memory-mapped — the threshold is
 * {@link TransportConf#memoryMapBytes()}), and {@code createInputStream}. This benchmark measures
 * the file-segment read hot path across segment sizes that cross the mmap threshold, so both the
 * read and mmap branches are exercised.
 *
 * <p>Setup needs only a temp file written with deterministic bytes and a {@link TransportConf} —
 * no netty channel, no background threads.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl common -am test-compile
 *   build/mvn -pl common exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.common.network.buffer.FileSegmentManagedBufferJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class FileSegmentManagedBufferJmhBenchmark {

  /** Segment length read from the file. Small values hit the read branch, large hit mmap. */
  @Param({"4096", "262144", "4194304"})
  private int length;

  private FileSegmentManagedBuffer buffer;
  private File tempFile;

  @Setup
  public void setup() throws Exception {
    CelebornConf celebornConf = new CelebornConf();
    TransportConf conf = new TransportConf("file", celebornConf);
    tempFile = new File(Utils.createTempDir(System.getProperty("java.io.tmpdir"), "celeborn_jmh"),
        "segment.bin");
    try (FileOutputStream out = new FileOutputStream(tempFile)) {
      byte[] chunk = new byte[8192];
      for (int i = 0; i < chunk.length; i++) {
        chunk[i] = (byte) i;
      }
      int written = 0;
      while (written < length) {
        int toWrite = (int) Math.min(chunk.length, (long) length - written);
        out.write(chunk, 0, toWrite);
        written += toWrite;
      }
    }
    buffer = new FileSegmentManagedBuffer(conf, tempFile, 0, length);
  }

  /** Zero-copy path: produces a {@link io.netty.channel.DefaultFileRegion} per call. */
  @Benchmark
  public Object convertToNetty(Blackhole blackhole) throws Exception {
    Object region = buffer.convertToNetty();
    blackhole.consume(region);
    return region;
  }

  /** Small files read into a heap buffer; large files memory-mapped. */
  @Benchmark
  public ByteBuffer nioByteBuffer(Blackhole blackhole) throws Exception {
    ByteBuffer bb = buffer.nioByteBuffer();
    blackhole.consume(bb);
    return bb;
  }

  @Benchmark
  public long size() {
    return buffer.size();
  }

  @TearDown
  public void tearDown() {
    if (tempFile != null && tempFile.exists()) {
      // best-effort cleanup; temp dir is under java.io.tmpdir
      tempFile.delete();
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
