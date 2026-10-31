# Spark 侧 Shuffle 线程模型与指标口径

> **基线**：代码依据 `celeborn-main`（分支 `parallel-partition-write`），行号均经代码核实
> **数据**：某生产作业，922.8 GiB 写 / 523.5 GiB 读，zstd-3
> **整理**：2026-08-09（线程模型与指标排查）、2026-08-10（UI 指标审计），2026-10-31 合并
>
> 本文由 `celeborn-shuffle-thread-model-analysis.md` 与 `celeborn-ui-metrics-audit.md` 合并：**第一部分解释"时间花在哪"（线程模型 + 生产指标归因），第二部分核对"指标记得对不对"（Spark UI 逐字段审计）**。

---

## 0. 结论速览

- **写侧根因**：压缩 CPU 瓶颈（因）→ DataPusher drain 慢 → Queue Wait（果）。网络全程健康（avgRtt 1ms、slowPush=0、无 worker 拥塞）。
- **读侧**：解压与网络预取 pipeline 重叠良好，**无需处理**。
- **指标**：现有数值统计正确（无错位、无重复计数），问题在口径注解缺失、2 个死字段，以及写侧 7 项 / 读侧 3 项 / 展示层 2 项缺口。

---

# 第一部分 线程模型与指标归因

## 1. Shuffle Write 线程模型（mapper 侧）

### 1.1 HashBasedShuffleWriter（默认 hash 模式）

两条线程，生产者-消费者模型：

```
Spark task 线程 (Executor task launch worker)          DataPusher 线程 (celeborn-client-data-pusher-<taskId>)   Netty IO 线程
───────────────────────────────                      ────────────────────────────────────────────────        ─────────────────
records.next()                                        while (stillRunning) {                                   push 响应回调
  partitioner.getPartition(key)                         tasks = dataPushQueue.takePushTasks()                    → 更新 in-flight
  序列化 key/value → serBuffer                            for task:                                               → 释放额度
  (serialize 指标, HashBasedShuffleWriter.java:275)        client.pushData(...)                                  → 失败时触发 revive
  copy 到 per-partition sendBuffer                         → pushOrMergeData(doPush=true)
  (copy 指标)                                              → compress! (ShuffleClientImpl.java:1299-1308)
  buffer 满 → flushSendBuffer(:337)                        → encrypt (若开启)
    dataPusher.addTask(:348)                               → netty 异步 push, 不等响应
      CRC 计算 (mapper 线程, DataPusher.java:156)
      idleQueue.poll / workingQueue.offer
      【Queue Wait 在此计时: DataPusher.java:176】
```

关键事实：

- **每个 map task 恰好 1 个 DataPusher 线程**（DataPusher.java:107-151），压缩/加密/push 全部串行在这一个线程
- 缓冲：512 个 PushTask × 64KiB = 32MiB（`client.push.queue.capacity` × `client.push.buffer.max.size`）
- **Queue Wait = task 线程被背压阻塞的时间**（DataPusher.java:157-177：等空闲 buffer 或等入队）
- **Background Compress = pusher 线程的压缩 CPU 时间**（ShuffleClientImpl.java:1304）
- **Background Queue Stall = pusher 线程因 per-worker in-flight 满而 sleep**（DataPushQueue.java:131-139，`client.push.maxReqsInFlight.perWorker=16`）
- push 是异步 fire-and-track：不等响应继续压下一批，靠 in-flight 额度限流

### 1.2 SortBasedShuffleWriter（sort 模式，无 DataPusher）

- 序列化后插入 `SortBasedPusher` 内存排序区；放不下了 `doPush()`（SortBasedShuffleWriter.java:314-318）**在 task 线程上同步**压缩+push
- 即 sort 模式下压缩直接占 task 线程墙钟，没有后台线程兜底

### 1.3 收尾

`dataPusher.waitOnTermination()` 等队列 drain（**Drain Wait**）→ `mapperEnd` → MapStatus 上报 driver。

## 2. Shuffle Read 线程模型（reducer 侧）

单线程为主，Netty 回调做异步预取：

```
Spark task 线程                                              Netty IO 线程
───────────────────────────────                             ─────────────────
CelebornShuffleReader.read():
  对每个 partition: openStream RPC (批 OpenStream, 串行)
  逐 partition 顺序读 CelebornInputStream:
    currentReader.next() (WorkerPartitionReader.java:218)
      fetchChunks(): 按 fetchMaxReqsInFlight 异步发
        client.fetchChunk(..., callback) (:343)  ──────→   chunk 到达 → callback 放入
      results.poll(pollChunkWaitTime) 【Chunk Wait】          results 阻塞队列
    decompressor.decompress 【Decompress, task 线程,
      CelebornInputStream.java:1065】
    反序列化 → 交给 Spark iterator
```

关键事实：

- **解压、反序列化都在 task 线程**；网络 fetch 由 Netty 线程异步完成并预取（in-flight 窗口）
- 读路径天然 pipeline：task 线程解压 chunk N 时，chunk N+1..N+k 已在网络上
- 多 partition 之间是串行的（读完一个 partition 再开下一个的流）

## 3. 本次任务的指标解读（结合线程模型）

### 3.1 Stage 7 单 task 明细（46 min，25.6 GiB 写，1/1 task）

| 指标 | 值 | 解读 |
|---|---|---|
| compress | 947s (15.8min) | 唯一 DataPusher 线程单线程 zstd-3 压全部数据，折算吞吐仅 ~30-70 MB/s/线程，异常偏低（正常 200+ MB/s）→ executor CPU 受限/争抢 |
| queueWait | 422s (7min) | task 线程等 32MiB 队列：pusher 压缩慢 drain 不动 → 背压。是压缩瓶颈的**症状** |
| queueStall / inflightWait / drainWait | 312ms / 1792ms / 0 | in-flight 限流几乎没触发 → **不是网络/worker 问题** |
| maxPushRtt / slowPush | 230ms / 0 | worker 侧零拥塞（50 个 worker avgRtt=1ms） |

### 3.2 全作业汇总

- Queue Wait 4.44h ≈ 写总时长 4.82h 的 **92%**；Background Compress 6.55h ≈ Task CPU 16.35h 的 **40%**（压缩与 task 并行，合计可超写墙钟）
- Stage 7 只贡献 422s queueWait，~97% 来自 Stage 4（587 task / 523.5 GiB / 4.2 min）：executor 高并发 task，每 task 一个压缩线程互抢 CPU
- **结论链：压缩 CPU 瓶颈（因）→ pusher drain 慢 → Queue Wait（果）。网络全程健康（avgRtt 1ms、slowPush=0、无 worker 拥塞）。**

### 3.3 Decompress 38.6 min —— 其实不高

- 摊到 1000 个读 task ≈ 2.3s/task；Chunk Wait 全作业仅 28.9s；Fetch Wait 3.8m；Read Time/Task CPU = 0.4%
- 绝对值大只因读了 ~1.4 TiB 未压缩数据；且解压与网络预取 pipeline 重叠良好。**读侧无需处理**

## 4. 建议（按优先级，均为配置/SQL 层，无需改代码）

1. **治理 Stage 7 倾斜**：1 task × 25.3 GiB 跑 46 min 是作业墙钟最大单点 → AQE skew split / salting / repartition。即便压缩免费，该 task 计算本身也要 ~39 min
2. **降压缩级别**：`spark.celeborn.client.shuffle.compression.zstd.level` 3 → 1（压缩率损失通常 <5%，CPU 省 30-50%）。网络证据（avgRtt 1ms、worker 零拥塞、观测写速仅 57 MB/s）说明带宽富余，用带宽换 CPU 划算；更激进可试 lz4
3. **增大 batch**：`celeborn.client.push.buffer.max.size` 64k → 128k/256k，减少压缩调用次数和批开销
4. 不建议调大 `celeborn.client.push.queue.capacity`：只是把阻塞换成更多内存占用，不解决 drain 慢
5. 核查 executor CPU quota：单线程 zstd-3 只有 30-70 MB/s 异常偏低，若 container 限核，降压缩级别的收益会进一步放大

---

# 第二部分 Spark UI 指标审计

## 5. 现有指标与链路环节对应关系

### 5.1 Write Times（MapperEnd 携带 WriteMetrics → driver 聚合，CelebornListener.scala:188-200）

| UI 字段 | 采集点 | 线程 | 口径核实 |
|---|---|---|---|
| Serialize | HashBasedShuffleWriter:275 / SortBased:332 | task | ✅ 正确（但见问题 2） |
| Copy | HashBased:243/256（fastWrite0 内计时） | task | ✅ |
| Queue Wait | DataPusher.addTask finally（DataPusher.java:176） | task | ✅ 定义准确：背压阻塞 |
| Background Compress | ShuffleClientImpl.java:1304（pushOrMergeData 内） | pusher 线程 | ⚠️ 见问题 4/5 |
| Background Queue Stall | DataPushQueue.java:138 | pusher 线程 | ✅ |
| Background Inflight Wait | InFlightRequestTracker.java:202 | pusher 线程 | ✅ |
| Drain Wait | InFlightRequestTracker.java:250 | task（收尾） | ✅ |
| Max Push RTT / Slow Push | PushState.recordPushRtt，阈值 `client.push.slowPush.threshold`（默认 5s），回调 ShuffleClientImpl.java:1394/:1850 | Netty 回调 | ✅ |
| 压缩率（uncompressedBytes） | writer close() flush | - | ✅ |

字段顺序核对：`new WriteMetrics(copy, serialize, compress, ...)`（ShuffleClientImpl.java:2168-2179）与 case class 定义（ControlMessages.scala:57-67）及 Listener 聚合映射（CelebornListener.scala:191-200）**三者一致，无错位 bug**。

### 5.2 Read Times（reportReadMetrics → ReadMetrics → Listener:208-221）

| UI 字段 | 采集点 | 口径核实 |
|---|---|---|
| Open Stream | CelebornShuffleReader.scala:404（batch open） | ⚠️ 只有 batch 级，见缺口 R2 |
| Chunk Wait | WorkerPartitionReader.next() results.poll（:232） | ✅ |
| Decompress | CelebornInputStream.java:1066 | ✅ |
| Retry Wait | CelebornInputStream:523/566/620/634 | ✅ |
| Max Chunk RTT / Slow Chunk | fetch 回调 | ✅ |

## 6. 发现的正确性问题

1. **"Total Shuffle Write Time" 口径偏窄**：Spark writeTime 只覆盖 flushSendBuffer（:349，含 queueWait）+ giant push（:319）+ closeWrite/pushMergedData（:413）+ drain+mapperEnd（:426），**不含主循环 serialize/copy**。所以 "Write Time/Task CPU 29.5%" 低估了写链路真实占比（serialize/copy 被藏进 task 计算时间）。这也解释了 4.82h ≈ queueWait 4.44h（92%）——写时间几乎全是等队列。
2. **Serialize=0 不是 bug 但 UI 有误导**：fastWrite0 快路径（UnsafeRow + PartitionIdPassthrough，HashBasedShuffleWriter:186-188/215-261）无 Java 序列化，只有 Platform.copyMemory → Serialize 恒 0、Copy 有值。本次任务即此情况。UI 无法区分"未发生"与"未统计"，建议加注。
3. **跨线程时间并列展示无提示**：Queue Wait 是 task 线程墙钟，Background Compress/Stall/Inflight 是 pusher 线程时间，两者并行发生、**不可相加**；UI 并排平铺容易误读为串行分解。
4. **sort 模式下 "Background Compress" 名不符实**：SortBasedShuffleWriter 的 doPush()（:314-318）在 task 线程同步压缩，压缩时间仍计入 compressTimeMs，UI 标签写成 Background。
5. **giant record 路径口径混杂**：pushGiantRecord 在 task 线程同步压缩+push，其压缩计入 compressTimeMs、等待却不计 queueWait。占比小，可接受但需知悉。
6. **ReadMetrics 两个死字段**：`deserializeTimeMs`、`copyTimeMs` 的 adder（ReadStreamStats.java:88/92）**全仓库无调用方**，恒为 0，仍走 proto/serde/聚合。要么补采集要么删除。

## 7. 指标缺口建议

### 7.1 写路径（按价值排序）

- W1 **mapperEnd RPC 时长**：现混在 writeTime 里无分解，LM 繁忙时是重要排查对象
- W2 **写侧 revive 次数/耗时**：容错开销完全不可见（UI 只有 driver 侧 reassign 汇总）
- W3 **CRC 计算时间**：`integrityCheck.enabled=true` 时在 task 线程 addTask 内（DataPusher.java:156），未计时
- W4 **push 失败计数（按原因）**：现仅 lastPushFailureReason 字符串，无计数
- W5 **batch 数 / 平均 batch 大小 / giant record 计数**：判断 batch 效率（64KiB 是否打满）
- W6 sort 模式专属：sort/spill 时间
- W7 加密时间（crypto 启用时）

### 7.2 读路径

- R1 **已采集未展示**：retryCount、peerSwitchCount、excludeCount 已聚合（Listener:215-220）但没上页面，零成本补上
- R2 **per-location openStream 耗时**：读中途切流/revive 时的 openStream RPC（CelebornInputStream 内）未计时，只有 reader 初始化 batch 级
- R3 partition 数 / 跳过 location 数（skipCount 已在日志 summary，未上报）

### 7.3 展示层

- D1 写时间分解表加注"跨线程并行，不可相加"；serialize=0 时提示可能在 fastWrite0 快路径
- D2 增加"未归因时间"派生项 = writeTime − (queueWait + drain + giant + closeWrite)，让口径缺口可见

## 8. 结论

- 现有指标**数值本身统计正确**（无错位、无重复计数 bug），主要问题是口径注解缺失（问题 1-5）和两个死字段（问题 6）
- 指标**不够**：写侧缺 mapperEnd/revive/CRC/失败计数，读侧有 3 个已采集指标没展示
