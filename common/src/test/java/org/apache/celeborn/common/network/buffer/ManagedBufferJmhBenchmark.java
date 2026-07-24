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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

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
 * JMH benchmark for celeborn's {@link ManagedBuffer} implementations.
 *
 * <p>Compares {@link NioManagedBuffer} (backed by a heap {@link ByteBuffer}) against
 * {@link NettyManagedBuffer} (backed by a netty {@link ByteBuf}) on the hot paths every send/recv
 * path touches: {@code convertToNetty} (the bridge into the netty pipeline), {@code size},
 * {@code nioByteBuffer}, and {@code retain}/{@code release}. {@code NioManagedBuffer.convertToNetty}
 * allocates a fresh {@code Unpooled.wrappedBuffer} each call, while {@code NettyManagedBuffer} only
 * does a {@code duplicate().retain()} — this benchmark quantifies that gap.
 *
 * <p>Every {@code convertToNetty} on {@link NettyManagedBuffer} bumps the refcount, so the
 * benchmark pairs it with {@code release()} to avoid leaking buffers across iterations.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl common -am test-compile
 *   build/mvn -pl common exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.common.network.buffer.ManagedBufferJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class ManagedBufferJmhBenchmark {

  @Param({"64", "4096", "65536"})
  private int bytes;

  private ByteBuffer nioBacking;
  private ByteBuf nettyBacking;
  private NioManagedBuffer nioBuffer;
  private NettyManagedBuffer nettyBuffer;

  @Setup
  public void setup() {
    byte[] data = new byte[bytes];
    for (int i = 0; i < bytes; i++) {
      data[i] = (byte) i;
    }
    nioBacking = ByteBuffer.wrap(data);
    nettyBacking = Unpooled.wrappedBuffer(data);
    nioBuffer = new NioManagedBuffer(nioBacking);
    nettyBuffer = new NettyManagedBuffer(nettyBacking);
  }

  @Benchmark
  public Object nioConvertToNetty() throws Exception {
    return nioBuffer.convertToNetty();
  }

  @Benchmark
  public Object nettyConvertToNetty() throws Exception {
    // paired with release below to keep refcount balanced across iterations
    Object converted = nettyBuffer.convertToNetty();
    nettyBuffer.release();
    return converted;
  }

  @Benchmark
  public long nioSize() {
    return nioBuffer.size();
  }

  @Benchmark
  public long nettySize() {
    return nettyBuffer.size();
  }

  @Benchmark
  public ByteBuffer nioNioByteBuffer() throws Exception {
    return nioBuffer.nioByteBuffer();
  }

  @Benchmark
  public ByteBuffer nettyNioByteBuffer() throws Exception {
    return nettyBuffer.nioByteBuffer();
  }

  @Benchmark
  public ManagedBuffer nettyRetainRelease() {
    nettyBuffer.retain();
    return nettyBuffer.release();
  }

  @Benchmark
  public ManagedBuffer nioRetainRelease() {
    nioBuffer.retain();
    return nioBuffer.release();
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
