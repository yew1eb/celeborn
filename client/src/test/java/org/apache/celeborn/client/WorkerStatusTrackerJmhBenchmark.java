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

package org.apache.celeborn.client;

import java.util.concurrent.TimeUnit;

import scala.Tuple2;

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

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.protocol.message.StatusCode;

/**
 * JMH benchmark for celeborn's {@link WorkerStatusTracker}.
 *
 * <p>{@link WorkerStatusTracker#workerAvailable(WorkerInfo)} is on the hot path of the client
 * write path: every push consults it to decide whether a worker is still usable (excluded /
 * shutting workers are skipped). It reads two concurrent collections ({@code excludedWorkers}
 * and {@code shuttingWorkers}); this benchmark measures that lookup cost across a realistic range
 * of excluded-worker counts and both hit (excluded) and miss (available) cases.
 *
 * <p>{@code WorkerStatusTracker} is constructed with a {@code null} {@link LifecycleManager} (as in
 * {@code WorkerStatusTrackerSuite}), since {@code workerAvailable} does not touch it.
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl client -am test-compile
 *   build/mvn -pl client exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.client.WorkerStatusTrackerJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class WorkerStatusTrackerJmhBenchmark {

  /** Number of workers pre-populated in the excluded set. */
  @Param({"0", "100", "1000"})
  private int excludedCount;

  private WorkerStatusTracker tracker;
  /** A worker that is NOT excluded — the common (available) case. */
  private WorkerInfo availableWorker;
  /** A worker that IS excluded — the skip case. */
  private WorkerInfo excludedWorker;
  private int lookupIndex;

  @Setup
  public void setup() {
    CelebornConf conf = new CelebornConf();
    tracker = new WorkerStatusTracker(conf, null);

    long now = System.currentTimeMillis();
    for (int i = 0; i < excludedCount; i++) {
      WorkerInfo w = new WorkerInfo("excluded-" + i, -1, -1, -1, -1);
      tracker.excludedWorkers().put(w, new Tuple2<>(StatusCode.WORKER_EXCLUDED, now));
    }

    availableWorker = new WorkerInfo("available-host", -1, -1, -1, -1);
    excludedWorker =
        excludedCount > 0
            ? new WorkerInfo("excluded-0", -1, -1, -1, -1)
            : new WorkerInfo("excluded-0", -1, -1, -1, -1);
    lookupIndex = 0;
  }

  @Benchmark
  public void workerAvailableMiss(Blackhole blackhole) {
    // The common case: a healthy worker not in the excluded/shutting sets.
    blackhole.consume(tracker.workerAvailable(availableWorker));
  }

  @Benchmark
  public void workerAvailableHit(Blackhole blackhole) {
    // The skip case: a worker present in the excluded set.
    blackhole.consume(tracker.workerAvailable(excludedWorker));
  }

  @Benchmark
  public void workerExcluded(Blackhole blackhole) {
    blackhole.consume(tracker.workerExcluded(excludedWorker));
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
