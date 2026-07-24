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

package org.apache.celeborn.common.network.util;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;

import org.apache.celeborn.common.network.protocol.Heartbeat;
import org.apache.celeborn.common.network.protocol.Message;

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
 * JMH benchmark for celeborn's {@link TransportFrameDecoder}.
 *
 * <p>The frame decoder is the inbound hot path: every incoming byte goes through {@code
 * channelRead}, which reassembles {@code [msgSize(4)][type(1)][bodySize(4)][body]} frames and
 * dispatches decoded {@link Message}s downstream. This benchmark drives it with an
 * {@link EmbeddedChannel} (no real I/O thread) and a pre-encoded buffer of many small
 * {@link Heartbeat} frames, so the cost measured is the framing/decode loop itself — header
 * accumulation, {@code decodeNext}, {@link Message#decode} and {@code fireChannelRead}.
 *
 * <p>{@link Heartbeat} is body-less, so {@code channelRead} releases each frame buffer itself — no
 * refcount bookkeeping is needed in the benchmark.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl common -am test-compile
 *   build/mvn -pl common exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.common.network.util.TransportFrameDecoderJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class TransportFrameDecoderJmhBenchmark {

  /** Number of frames packed into the pre-encoded buffer per invocation. */
  @Param({"64", "1024"})
  private int framesPerBuffer;

  private ByteBuf encodedBuffer;
  private EmbeddedChannel channel;

  @Setup
  public void setup() {
    // Build a buffer holding framesPerBuffer Heartbeat frames, each = [msgSize=1][type=HEARTBEAT]
    // [bodySize=0][1 body byte]. msgSize=1 (the body byte), bodySize=0 would not carry a body, but
    // the decoder reads msgSize + bodySize then msgSize bytes of message; encode a 1-byte body so
    // msgSize=1 matches Heartbeat.encodedLength()=1.
    int frameSize = FrameDecoder.HEADER_SIZE + 1; // 9 header + 1 body
    encodedBuffer = Unpooled.buffer(frameSize * framesPerBuffer);
    Heartbeat heartbeat = new Heartbeat();
    for (int i = 0; i < framesPerBuffer; i++) {
      encodedBuffer.writeInt(1); // msgSize = encodedLength of Heartbeat = 1
      Message.Type.HEARTBEAT.encode(encodedBuffer); // type byte
      encodedBuffer.writeInt(0); // bodySize = 0 (Heartbeat has no body)
      heartbeat.encode(encodedBuffer); // the 1-byte message body
    }

    // A decoder channel followed by a counting sink so decoded messages are consumed.
    channel =
        new EmbeddedChannel(new TransportFrameDecoder(), new CountingInboundHandler());
  }

  @Benchmark
  public void channelRead(Blackhole blackhole) {
    // writeInbound drives the inbound pipeline: TransportFrameDecoder.channelRead fires decoded
    // Heartbeat messages into the counting handler. The decoder mutates its own headerBuf state
    // across frames, so we feed a fresh copy each invocation.
    channel.writeInbound(encodedBuffer.retainedDuplicate());
    blackhole.consume(channel.readInbound());
  }

  @TearDown
  public void tearDown() {
    if (channel != null) {
      channel.finishAndReleaseAll();
    }
    if (encodedBuffer != null) {
      encodedBuffer.release();
    }
  }

  /** Trivial inbound handler that just consumes (and drops) decoded messages downstream. */
  private static final class CountingInboundHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      // Heartbeat carries no refcounted buffer; nothing to release.
      ctx.fireChannelRead(msg);
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
