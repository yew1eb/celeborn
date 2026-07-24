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

package org.apache.celeborn.common.network.protocol;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;

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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmark for celeborn's {@link EncryptedMessageWithHeader} — the SSL send path.
 *
 * <p>When SSL is enabled, outbound messages are sent through {@link EncryptedMessageWithHeader}
 * instead of the zero-copy {@link MessageWithHeader}: it streams a header {@link ByteBuf} followed
 * by a body {@link InputStream} (or {@link io.netty.handler.stream.ChunkedStream}) via
 * {@code readChunk}. This benchmark measures that streaming readChunk cycle (header chunk + body
 * chunk + close) across body sizes. Setup mirrors {@code EncryptedMessageWithHeaderSuiteJ}: a
 * {@link ByteArrayInputStream} body, a {@link NettyManagedBuffer} source, and
 * {@link ByteBufAllocator#DEFAULT} — no netty channel, no threads.
 *
 * <p>Each invocation builds a fresh message (the stream is consumed by readChunk) and drives it to
 * completion + {@code close()}, so refcounts stay balanced across iterations.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl common -am test-compile
 *   build/mvn -pl common exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.common.network.protocol.EncryptedMessageWithHeaderJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class EncryptedMessageWithHeaderJmhBenchmark {

  @Param({"128", "4096", "65536"})
  private int bodyBytes;

  @Param({"42"})
  private int seed;

  private byte[] bodyData;
  private ByteBufAllocator allocator;

  @Setup
  public void setup() {
    SplittableRandom random = new SplittableRandom(seed);
    bodyData = new byte[bodyBytes];
    for (int i = 0; i < bodyBytes; i++) {
      bodyData[i] = (byte) random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1);
    }
    allocator = ByteBufAllocator.DEFAULT;
  }

  /**
   * Full SSL send cycle for one message: build header+body, read the header chunk, read the body
   * chunk(s) until end, close. Refcounts are released so the loop stays clean.
   */
  @Benchmark
  public void readChunks(Blackhole blackhole) throws Exception {
    ByteBuf sourceBuffer = Unpooled.wrappedBuffer(bodyData);
    InputStream body = new ByteArrayInputStream(bodyData);
    ByteBuf header = Unpooled.copyLong(42);
    ManagedBuffer managedBuf = new NettyManagedBuffer(sourceBuffer);

    EncryptedMessageWithHeader msg =
        new EncryptedMessageWithHeader(managedBuf, header, body, managedBuf.size());

    ByteBuf headerChunk = msg.readChunk(allocator);
    blackhole.consume(headerChunk);
    headerChunk.release();

    ByteBuf bodyChunk;
    do {
      bodyChunk = msg.readChunk(allocator);
      if (bodyChunk != null) {
        blackhole.consume(bodyChunk);
        bodyChunk.release();
      }
    } while (bodyChunk != null && !msg.isEndOfInput());

    msg.close();
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
