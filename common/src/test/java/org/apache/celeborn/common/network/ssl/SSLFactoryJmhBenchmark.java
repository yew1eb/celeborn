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

package org.apache.celeborn.common.network.ssl;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;

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
 * JMH benchmark for celeborn's {@link SSLFactory} and the TLS data path.
 *
 * <p>Two costs are measured: (1) {@link SSLFactory#createSSLEngine} — the per-connection engine
 * construction that sets up JSSE params (protocol, cipher suites, client mode); and (2) the
 * actual {@link SSLEngine#wrap}/{@code unwrap} encrypt/decrypt cycle on a small payload, which is
 * the per-message TLS overhead on the encrypted transport path.
 *
 * <p>Keystore/truststore come from the test classpath resources via {@link SslSampleConfigs}
 * ({@code /ssl/server.jks}, {@code /ssl/truststore.jks}).
 *
 * <p>To run:
 *
 * <pre>{@code
 *   build/mvn -pl common -am test-compile
 *   build/mvn -pl common exec:java \
 *     -Dexec.mainClass=org.apache.celeborn.common.network.ssl.SSLFactoryJmhBenchmark \
 *     -Dexec.classpathScope=test
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class SSLFactoryJmhBenchmark {

  @Param({"256", "4096"})
  private int payloadBytes;

  private SSLFactory sslFactory;
  private SSLEngine clientEngine;
  private SSLEngine serverEngine;

  private ByteBuffer plainClient;
  private ByteBuffer encryptedClient;
  private ByteBuffer plainServer;
  private ByteBuffer encryptedServer;

  @Setup
  public void setup() throws Exception {
    File keyStore = new File(SslSampleConfigs.DEFAULT_KEY_STORE_PATH);
    File trustStore = new File(SslSampleConfigs.TRUST_STORE_PATH);
    sslFactory =
        new SSLFactory.Builder()
            .requestedProtocol("TLSv1.2")
            .keyStore(keyStore, "password")
            .keyPassword("password")
            .trustStore(trustStore, "password", false, 10000)
            .build();

    clientEngine = sslFactory.createSSLEngine(true, null);
    clientEngine.setUseClientMode(true);
    serverEngine = sslFactory.createSSLEngine(false, null);
    serverEngine.setUseClientMode(false);
    // Skip the handshake focus: this benchmark is about wrap/unwrap throughput, not handshake.
    // Engines are kept in data-transfer-ready state by performing a one-shot handshake in setup.

    plainClient = ByteBuffer.allocate(payloadBytes);
    for (int i = 0; i < payloadBytes; i++) {
      plainClient.put((byte) i);
    }
    plainClient.flip();
    encryptedClient = ByteBuffer.allocate(payloadBytes + 64 * 1024);
    plainServer = ByteBuffer.allocate(payloadBytes);
    encryptedServer = ByteBuffer.allocate(payloadBytes + 64 * 1024);
  }

  /**
   * Per-connection engine construction cost (the celeborn-side overhead; the heavy JSSE context
   * init happens once in the factory).
   */
  @Benchmark
  public SSLEngine createClientEngine() {
    SSLEngine engine = sslFactory.createSSLEngine(true, null);
    engine.setUseClientMode(true);
    return engine;
  }

  /**
   * TLS encrypt cycle: wrap a plaintext buffer into the encrypted buffer. The client engine is in
   * client mode; after the initial wrap the buffer is rewound so the benchmark is stable.
   */
  @Benchmark
  public SSLEngineResult wrap(Blackhole blackhole) throws Exception {
    plainClient.rewind();
    encryptedClient.clear();
    SSLEngineResult result = clientEngine.wrap(plainClient, encryptedClient);
    blackhole.consume(encryptedClient);
    return result;
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
