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

package org.apache.celeborn.client.write;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.DummyShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.common.write.PushState;

public class DataPushQueueSuiteJ {
  private static final Logger LOG = LoggerFactory.getLogger(DataPushQueueSuiteJ.class);
  private static File tempDir = null;

  private final int shuffleId = 0;
  private final int numPartitions = 1000000;

  @BeforeClass
  public static void beforeAll() {
    tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "celeborn_test");
  }

  @AfterClass
  public static void afterAll() {
    try {
      JavaUtils.deleteRecursively(tempDir);
    } catch (IOException e) {
      LOG.error("Failed to delete temporary directory.", e);
    }
  }

  @Test
  public void testDataPushQueue() throws Exception {
    final int numWorker = 30;
    List<List<Integer>> workerData = new ArrayList<>();
    for (int i = 0; i < numWorker; i++) {
      workerData.add(new ArrayList<>());
    }
    for (int i = 0; i < numPartitions; i++) {
      workerData.get(i % numWorker).add(i);
    }
    List<List<Integer>> tarWorkerData = new ArrayList<>();
    for (int i = 0; i < numWorker; i++) {
      tarWorkerData.add(new ArrayList<>());
    }

    Map<Integer, Integer> partitionBatchIdMap = JavaUtils.newConcurrentHashMap();

    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.CLIENT_PUSH_MAX_REQS_IN_FLIGHT_PERWORKER().key(), "2");

    int shuffleId = 0;
    int mapId = 0;
    int attemptId = 0;
    int numMappers = 10;
    final File tempFile = new File(tempDir, UUID.randomUUID().toString());
    DummyShuffleClient client = new DummyShuffleClient(conf, tempFile);
    client.initReducePartitionMap(shuffleId, numPartitions, numWorker);

    LongAdder[] mapStatusLengths = new LongAdder[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      mapStatusLengths[i] = new LongAdder();
    }
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    PushState pushState = client.getPushState(mapKey);
    Map<Integer, PartitionLocation> reducePartitionMap =
        client.getReducePartitionMap().get(shuffleId);

    DataPusher dataPusher =
        new DataPusher(
            shuffleId,
            mapId,
            attemptId,
            0,
            numMappers,
            numPartitions,
            conf,
            client,
            null,
            integer -> {},
            mapStatusLengths) {
          @Override
          protected void pushData(PushTask task) throws IOException {
            byte[] buffer = task.getBuffer();
            int partitionId = task.getPartitionId();
            tarWorkerData.get(partitionId % numWorker).add(bytesToInt(buffer));
            pushState.removeBatch(
                partitionBatchIdMap.get(partitionId),
                reducePartitionMap.get(partitionId).hostAndPushPort());
          }
        };

    for (int i = 0; i < numPartitions; i++) {
      byte[] b = intToBytes(workerData.get(i % numWorker).get(i / numWorker));
      int batchId = pushState.nextBatchId();
      pushState.addBatch(batchId, b.length, reducePartitionMap.get(i).hostAndPushPort());
      partitionBatchIdMap.put(i, batchId);
      dataPusher.addTask(i, b, b.length);
    }

    dataPusher.waitOnTermination();

    for (int i = 0; i < numWorker; i++) {
      Assert.assertArrayEquals(workerData.get(i).toArray(), tarWorkerData.get(i).toArray());
    }

    client.shutdown();
  }

  @Test
  public void testDataPusherUnCaughtException() throws Exception {
    int shuffleId = 0;
    int mapId = 0;
    int attemptId = 0;
    int numMappers = 10;
    CelebornConf conf = new CelebornConf();
    final File tempFile = new File(tempDir, UUID.randomUUID().toString());
    DummyShuffleClient client = new DummyShuffleClient(conf, tempFile);
    LongAdder[] mapStatusLengths = new LongAdder[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      mapStatusLengths[i] = new LongAdder();
    }
    DataPusher dataPusher =
        new DataPusher(
            shuffleId,
            mapId,
            attemptId,
            0,
            numMappers,
            numPartitions,
            conf,
            client,
            null,
            integer -> {},
            mapStatusLengths) {
          @Override
          protected void pushData(PushTask task) throws IOException {
            throw new OutOfMemoryError();
          }
        };
    dataPusher.addTask(0, new byte[10], 0);
    try {
      dataPusher.waitOnTermination();
    } catch (Throwable e) {
      Assert.assertTrue(e.getCause() instanceof OutOfMemoryError);
    }
    client.shutdown();
  }

  @Test
  public void testReviveGateSkipsAndResumes() throws Exception {
    int shuffleId = 0;
    int mapId = 0;
    int attemptId = 0;
    int numMappers = 1;
    int numPartitions = 2;
    int gatedPartition = 0;

    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.CLIENT_PUSH_REVIVE_GATE_ENABLED().key(), "true");
    conf.set(CelebornConf.CLIENT_PUSH_TAKE_TASK_WAIT_INTERVAL().key(), "10ms");
    conf.set(CelebornConf.CLIENT_PUSH_TAKE_TASK_REVIVE_MAX_WAIT_ATTEMPTS().key(), "10000");

    final Set<Integer> reviving = ConcurrentHashMap.newKeySet();
    reviving.add(gatedPartition);
    final File tempFile = new File(tempDir, UUID.randomUUID().toString());
    DummyShuffleClient client =
        new DummyShuffleClient(conf, tempFile) {
          @Override
          public boolean isPartitionReviving(int shuffleId, int partitionId) {
            return reviving.contains(partitionId);
          }
        };
    client.initReducePartitionMap(shuffleId, numPartitions, 1);

    LongAdder[] mapStatusLengths = new LongAdder[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      mapStatusLengths[i] = new LongAdder();
    }
    List<Integer> pushed = Collections.synchronizedList(new ArrayList<>());
    DataPusher dataPusher =
        new DataPusher(
            shuffleId,
            mapId,
            attemptId,
            0,
            numMappers,
            numPartitions,
            conf,
            client,
            null,
            integer -> {},
            mapStatusLengths) {
          @Override
          protected void pushData(PushTask task) throws IOException {
            pushed.add(task.getPartitionId());
          }
        };

    dataPusher.addTask(gatedPartition, new byte[10], 10);
    dataPusher.addTask(1, new byte[10], 10);

    // The normal partition is pushed while the reviving one stays in the queue.
    waitUntil(() -> pushed.contains(1));
    Assert.assertFalse(pushed.contains(gatedPartition));

    // After the revive window ends, the gated partition is pushed too.
    reviving.clear();
    dataPusher.waitOnTermination();
    Assert.assertTrue(pushed.contains(gatedPartition));
    client.shutdown();
  }

  @Test
  public void testReviveGateDisabled() throws Exception {
    int shuffleId = 0;
    int mapId = 0;
    int attemptId = 0;
    int numMappers = 1;
    int numPartitions = 1;

    // reviveGateEnabled defaults to false, the gate must not take effect.
    CelebornConf conf = new CelebornConf();
    final File tempFile = new File(tempDir, UUID.randomUUID().toString());
    DummyShuffleClient client =
        new DummyShuffleClient(conf, tempFile) {
          @Override
          public boolean isPartitionReviving(int shuffleId, int partitionId) {
            return true;
          }
        };
    client.initReducePartitionMap(shuffleId, numPartitions, 1);

    LongAdder[] mapStatusLengths = new LongAdder[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      mapStatusLengths[i] = new LongAdder();
    }
    List<Integer> pushed = Collections.synchronizedList(new ArrayList<>());
    DataPusher dataPusher =
        new DataPusher(
            shuffleId,
            mapId,
            attemptId,
            0,
            numMappers,
            numPartitions,
            conf,
            client,
            null,
            integer -> {},
            mapStatusLengths) {
          @Override
          protected void pushData(PushTask task) throws IOException {
            pushed.add(task.getPartitionId());
          }
        };

    dataPusher.addTask(0, new byte[10], 10);
    dataPusher.waitOnTermination();
    Assert.assertTrue(pushed.contains(0));
    client.shutdown();
  }

  @Test
  public void testReviveGateSafetyValve() throws Exception {
    int shuffleId = 0;
    int mapId = 0;
    int attemptId = 0;
    int numMappers = 1;
    int numPartitions = 1;

    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.CLIENT_PUSH_REVIVE_GATE_ENABLED().key(), "true");
    conf.set(CelebornConf.CLIENT_PUSH_TAKE_TASK_WAIT_INTERVAL().key(), "10ms");
    conf.set(CelebornConf.CLIENT_PUSH_TAKE_TASK_REVIVE_MAX_WAIT_ATTEMPTS().key(), "2");

    final File tempFile = new File(tempDir, UUID.randomUUID().toString());
    // The revive flag is never cleared, the task must be taken anyway after
    // takeTaskReviveMaxWaitAttempts rounds instead of waiting forever.
    DummyShuffleClient client =
        new DummyShuffleClient(conf, tempFile) {
          @Override
          public boolean isPartitionReviving(int shuffleId, int partitionId) {
            return true;
          }
        };
    client.initReducePartitionMap(shuffleId, numPartitions, 1);

    LongAdder[] mapStatusLengths = new LongAdder[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      mapStatusLengths[i] = new LongAdder();
    }
    List<Integer> pushed = Collections.synchronizedList(new ArrayList<>());
    DataPusher dataPusher =
        new DataPusher(
            shuffleId,
            mapId,
            attemptId,
            0,
            numMappers,
            numPartitions,
            conf,
            client,
            null,
            integer -> {},
            mapStatusLengths) {
          @Override
          protected void pushData(PushTask task) throws IOException {
            pushed.add(task.getPartitionId());
          }
        };

    dataPusher.addTask(0, new byte[10], 10);
    dataPusher.waitOnTermination();
    Assert.assertTrue(pushed.contains(0));
    client.shutdown();
  }

  private void waitUntil(CheckCallable condition) throws Exception {
    long deadline = System.currentTimeMillis() + 10000;
    while (!condition.check()) {
      if (System.currentTimeMillis() > deadline) {
        Assert.fail("Timed out waiting for condition.");
      }
      Thread.sleep(10);
    }
  }

  private interface CheckCallable {
    boolean check();
  }

  public static byte[] intToBytes(int value) {
    byte[] src = new byte[4];
    src[0] = (byte) (value & 0xFF);
    src[1] = (byte) ((value >> 8) & 0xFF);
    src[2] = (byte) ((value >> 16) & 0xFF);
    src[3] = (byte) ((value >> 24) & 0xFF);
    return src;
  }

  public static int bytesToInt(byte[] src) {
    int value;
    value =
        (int)
            ((src[0] & 0xFF)
                | ((src[1] & 0xFF) << 8)
                | ((src[2] & 0xFF) << 16)
                | ((src[3] & 0xFF) << 24));
    return value;
  }
}
