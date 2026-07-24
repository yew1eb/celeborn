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

package org.apache.celeborn.common.metrics;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmark for celeborn's {@link ResettableSlidingWindowReservoir}, the reservoir backing
 * {@link CelebornHistogram} / {@link CelebornTimer}.
 *
 * <p>{@code ResettableSlidingWindowReservoir} guards both {@code update} and {@code getSnapshot}
 * with {@code this.synchronized}, so under concurrent writers the write path is the primary cost.
 * This benchmark mirrors the Kafka {@code HistogramBenchmark} multi-writer / few-reader shape
 * using JMH {@link Group} / {@link GroupThreads} to expose that contention.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl common -am test-compile
 *   build/mvn -pl common exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.common.metrics.CelebornHistogramJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class CelebornHistogramJmhBenchmark {

  @Param({"1024", "8192"})
  private int reservoirSize;

  private static final long MAX_VALUE = TimeUnit.MINUTES.toMillis(1L);

  private Reservoir reservoir;

  @Setup
  public void setup() {
    reservoir = new ResettableSlidingWindowReservoir(reservoirSize);
  }

  /**
   * Writer: records a random value. {@code ResettableSlidingWindowReservoir.update} is
   * synchronized, so multiple writers contend here.
   */
  @Benchmark
  @Group("rw")
  @GroupThreads(3)
  public void update() {
    reservoir.update(ThreadLocalRandom.current().nextLong(MAX_VALUE));
  }

  /**
   * Reader: occasionally takes a snapshot and reads the 99.9th percentile. The {@code now % 199}
   * gate mirrors the Kafka HistogramBenchmark so the read path is exercised far less often than the
   * write path, and the measurement mostly reflects the snapshot cost rather than a cheap clock
   * read. Returns a value so it is not compiled away.
   */
  @Benchmark
  @Group("rw")
  @GroupThreads(1)
  public double readPercentile() {
    long now = System.currentTimeMillis();
    if (now % 199 == 0) {
      Snapshot snapshot = reservoir.getSnapshot();
      return snapshot.get999thPercentile();
    }
    return now;
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
