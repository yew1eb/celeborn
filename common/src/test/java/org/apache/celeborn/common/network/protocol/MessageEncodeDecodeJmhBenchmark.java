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

import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;

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
 * JMH benchmark for celeborn transport {@link Message} encode/decode.
 *
 * <p>{@link PushMergedData} is the richest concrete message (it exercises {@link Encoders.Strings},
 * {@link Encoders.StringArrays} and {@link Encoders.IntArrays}), so it serves as a representative
 * for the network message hot path. This mirrors the Kafka request/response serialization
 * benchmarks (KAFKA-8106 skip ByteBuffer allocation, KAFKA-14633 reduce data copy) but measures
 * celeborn's own message format.
 *
 * <p>The {@code decodeBody} parameter toggles whether decode materializes the body buffer
 * ({@code true} wraps the source {@link ByteBuf} via {@code NettyManagedBuffer}; {@code false} uses
 * {@link NettyManagedBuffer#EmptyBuffer}, avoiding the wrap). When {@code decodeBody=true} the
 * decoded message shares the source buffer's refcount, so the benchmark releases the decoded body
 * to keep refcounts balanced across iterations.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl common -am test-compile
 *   build/mvn -pl common exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.common.network.protocol.MessageEncodeDecodeJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class MessageEncodeDecodeJmhBenchmark {

  @Param({"1", "64", "1024"})
  private int partitionCount;

  @Param({"false", "true"})
  private boolean decodeBody;

  @Param({"42"})
  private int seed;

  private PushMergedData message;
  private int encodedLength;

  @Setup
  public void setup() {
    String[] partitionIds = new String[partitionCount];
    int[] batchOffsets = new int[partitionCount];
    int cumulative = 0;
    for (int i = 0; i < partitionCount; i++) {
      partitionIds[i] = "shuffle-0-map-0-" + i;
      batchOffsets[i] = cumulative;
      cumulative += 1024;
    }
    message =
        new PushMergedData(
            (byte) 0, "shuffle-0", partitionIds, batchOffsets, NettyManagedBuffer.EmptyBuffer);
    encodedLength = message.encodedLength();
  }

  @Benchmark
  public ByteBuf encode(Blackhole blackhole) {
    ByteBuf buf = Unpooled.buffer(encodedLength);
    message.encode(buf);
    blackhole.consume(buf);
    return buf;
  }

  @Benchmark
  public PushMergedData decode() {
    ByteBuf buf = Unpooled.buffer(encodedLength);
    message.encode(buf);
    PushMergedData decoded = PushMergedData.decode(buf, decodeBody);
    if (decodeBody) {
      // decoded body shares buf's refcount via NettyManagedBuffer; release to stay balanced
      decoded.body().release();
    }
    return decoded;
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
