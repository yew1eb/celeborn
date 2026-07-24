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

package org.apache.celeborn.common.util;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmark for celeborn's {@code JavaUtils.newConcurrentHashMap} map selection.
 *
 * <p>Replaces the older Scala {@code ComputeIfAbsentBenchmark} (hand-rolled timing framework) with
 * a standard JMH benchmark. It measures {@code computeIfAbsent} / {@code get} / {@code putIfAbsent}
 * on {@link JavaUtils#newConcurrentHashMap()} (which applies the CELEBORN-474 JDK8
 * computeIfAbsent fast-path) against a plain {@link HashMap} and a stock {@link ConcurrentHashMap},
 * following the Kafka {@code ConcurrentMapBenchmark} low-write / high-read shape with
 * {@link OperationsPerInvocation} and a {@code writePercentage} parameter.
 *
 * <p>{@code HashMap} is not thread-safe, so its benchmark methods must run single-threaded; the
 * concurrent variants are safe under the configured thread count.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl common -am test-compile
 *   build/mvn -pl common exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.common.util.ConcurrentMapJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class ConcurrentMapJmhBenchmark {

  private static final int TIMES = 100_000;

  @Param({"100", "1000"})
  private int mapSize;

  @Param({"0.1"})
  private double writePercentage;

  private Map<Integer, Integer> celebornConcurrentMap;
  private Map<Integer, Integer> stockConcurrentMap;
  private Map<Integer, Integer> hashMap;
  private int writePerLoops;

  @Setup
  public void setup() {
    Map<Integer, Integer> template =
        IntStream.range(0, mapSize).boxed().collect(Collectors.toMap(i -> i, i -> i));
    celebornConcurrentMap = JavaUtils.newConcurrentHashMap(template);
    stockConcurrentMap = new ConcurrentHashMap<>(template);
    // HashMap path is single-threaded only; kept as a baseline against the concurrent variants.
    hashMap = new HashMap<>(template);
    writePerLoops = TIMES / (int) Math.round(writePercentage * TIMES);
  }

  @Benchmark
  @OperationsPerInvocation(TIMES)
  public void celebornComputeIfAbsent(Blackhole blackhole) {
    for (int i = 0; i < TIMES; i++) {
      if (i % writePerLoops == 0) {
        // offset by mapSize so computeIfAbsent actually inserts a new entry
        blackhole.consume(celebornConcurrentMap.computeIfAbsent(i + mapSize, key -> key));
      } else {
        blackhole.consume(celebornConcurrentMap.get(i % mapSize));
      }
    }
  }

  @Benchmark
  @OperationsPerInvocation(TIMES)
  public void stockComputeIfAbsent(Blackhole blackhole) {
    for (int i = 0; i < TIMES; i++) {
      if (i % writePerLoops == 0) {
        blackhole.consume(stockConcurrentMap.computeIfAbsent(i + mapSize, key -> key));
      } else {
        blackhole.consume(stockConcurrentMap.get(i % mapSize));
      }
    }
  }

  @Benchmark
  @OperationsPerInvocation(TIMES)
  public void celebornGetReadOnly(Blackhole blackhole) {
    for (int i = 0; i < TIMES; i++) {
      blackhole.consume(celebornConcurrentMap.get(ThreadLocalRandom.current().nextInt(0, mapSize)));
    }
  }

  @Benchmark
  @OperationsPerInvocation(TIMES)
  public void stockGetReadOnly(Blackhole blackhole) {
    for (int i = 0; i < TIMES; i++) {
      blackhole.consume(stockConcurrentMap.get(ThreadLocalRandom.current().nextInt(0, mapSize)));
    }
  }

  /**
   * Single-threaded baseline against a plain {@link HashMap}. Mirrors the original
   * {@code ComputeIfAbsentBenchmark} which compared HashMap vs ConcurrentHashMap on putIfAbsent /
   * computeIfAbsent.
   */
  @Benchmark
  @OperationsPerInvocation(TIMES)
  public void hashMapComputeIfAbsent(Blackhole blackhole) {
    for (int i = 0; i < TIMES; i++) {
      if (i % writePerLoops == 0) {
        blackhole.consume(hashMap.computeIfAbsent(i + mapSize, key -> key));
      } else {
        blackhole.consume(hashMap.get(i % mapSize));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
