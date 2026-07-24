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

package org.apache.celeborn.service.deploy.worker.storage;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

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
 * JMH benchmark for celeborn's {@code LocalFlushTask} — the worker disk-write hot path.
 *
 * <p>{@code LocalFlushTask.flush} is where buffered shuffle data lands on disk: it pulls the
 * {@code nioBuffers()} out of a {@link CompositeByteBuf} and writes them to a {@link FileChannel}
 * either via the gather write API ({@code fileChannel.write(ByteBuffer[])}) when
 * {@code gatherApiEnabled} is true, or one buffer at a time otherwise. This mirrors Kafka's
 * {@code TestLinearWriteSpeed} (mmap vs FileChannel) but measures celeborn's own FileChannel path
 * across both write modes — the {@code gatherApiEnabled} flag is a real config knob whose effect
 * this benchmark quantifies.
 *
 * <p>{@code LocalFlushTask}/{@code FlushTask} are {@code private[worker]}, so the benchmark lives
 * in the same package. Each invocation rewinds the channel to position 0 so we measure the write
 * itself rather than unbounded file growth.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl worker -am test-compile
 *   build/mvn -pl worker exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.service.deploy.worker.storage.LocalFlushTaskJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class LocalFlushTaskJmhBenchmark {

  /** Total bytes written per flush, split across several composite components. */
  @Param({"65536", "1048576"})
  private int bufferBytes;

  @Param({"true", "false"})
  private boolean gatherApiEnabled;

  private static final int COMPONENTS = 4;

  private File tempFile;
  private FileChannel fileChannel;
  private AbstractSource source;
  private FlushNotifier notifier;
  private CompositeByteBuf buffer;
  private LocalFlushTask flushTask;

  @Setup
  public void setup() throws Exception {
    File tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "celeborn_jmh");
    tempFile = new File(tempDir, "flush-target.bin");
    fileChannel = FileChannel.open(tempFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);

    source = new WorkerSource(new CelebornConf());
    notifier = new FlushNotifier();

    buffer = buildBuffer(bufferBytes);
    // keepBuffer=false so the task does not retain the buffer beyond flush
    flushTask = new LocalFlushTask(buffer, fileChannel, notifier, false, source, gatherApiEnabled);
  }

  /** Write the composite buffer to disk; rewind to position 0 each call to keep the file bounded. */
  @Benchmark
  public void flush(Blackhole blackhole) throws Exception {
    fileChannel.position(0);
    flushTask.flush(null);
    blackhole.consume(fileChannel.position());
  }

  private static CompositeByteBuf buildBuffer(int totalBytes) {
    CompositeByteBuf buf =
        new UnpooledByteBufAllocator(true).compositeBuffer(COMPONENTS);
    int perComponent = totalBytes / COMPONENTS;
    byte[] chunk = new byte[perComponent];
    for (int i = 0; i < perComponent; i++) {
      chunk[i] = (byte) i;
    }
    for (int i = 0; i < COMPONENTS; i++) {
      buf.addComponent(Unpooled.wrappedBuffer(chunk).retain());
    }
    buf.writerIndex(totalBytes);
    return buf;
  }

  @TearDown
  public void tearDown() throws Exception {
    if (fileChannel != null) {
      fileChannel.close();
    }
    if (buffer != null) {
      buffer.release();
    }
    if (tempFile != null && tempFile.exists()) {
      tempFile.delete();
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
