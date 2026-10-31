# Apache Celeborn 引擎客户端源码阅读指南（Flink / MapReduce / Tez）

> 基于版本：**0.7.0-SNAPSHOT**　整理日期：**2026-08-05**
>
> 范围：`client-flink/`（标准 RSS + Tiered 双路径）、`client-mr/`、`client-tez/`。
> 阅读目标：理解各计算引擎如何注册 Celeborn shuffle、写/读数据的关键路径，以及不同引擎集成深度上的差异。

---

## 1. 各引擎 Hook 点总览

| 引擎 | Hook 机制 | 入口类 | 集成深度 |
|------|-----------|--------|----------|
| Spark | 实现 Spark `ShuffleManager` SPI | `SparkShuffleManager`（`client-spark/spark-3/src/main/java/org/apache/spark/shuffle/celeborn/`） | 最深：Hash/Sort 双 writer、columnar shuffle 变体 |
| Flink | 实现 Flink `ShuffleServiceFactory` SPI / Tiered Storage API | `RemoteShuffleServiceFactory` / `CelebornTierFactory` | 深：自定义 `ResultPartition` / `InputGate`，支持 tiered hybrid shuffle |
| MapReduce | 配置替换 map-side collector 与 reduce-side shuffle consumer 插件 | `MRAppMasterWithCeleborn` + `CelebornMapOutputCollector` + `CelebornShuffleConsumer` | 薄封装：复用 Hadoop `MergeManager` |
| Tez | DAG AM 反射改写 edge input/output 类名 | `CelebornDagAppMaster` | 薄封装（Experimental）：复用 Tez merge/shuffle 抽象 |

> Spark 客户端不在本文展开：本组文档无 Spark 专题，请参考官方开发者文档 `docs/developers/client.md`、`docs/developers/shuffleclient.md` 与 `docs/developers/lifecyclemanager.md`。

---

## 2. Flink 客户端（标准 RSS + Tiered 双路径）

### 2.1 模块定位与版本覆盖

`client-flink/` 是面向 **Apache Flink 1.16–2.3** 的多版本集成模块，包含 9 个版本子模块（`flink-1.16`、`flink-1.17`、`flink-1.18`、`flink-1.19`、`flink-1.20`、`flink-2.0`、`flink-2.1`、`flink-2.2`、`flink-2.3`，各有对应 `*-shaded` 模块），通过 Flink 的 **ShuffleService SPI** 把 Celeborn 注册为可插拔 shuffle 服务，并提供两套实现：

| 模式 | 入口类 | Flink API | 适用场景 |
|------|--------|-----------|----------|
| 标准 Remote Shuffle Service | `RemoteShuffleServiceFactory` | `ShuffleServiceFactory` | 传统批处理 blocking shuffle |
| Tiered / Hybrid Shuffle | `CelebornTierFactory`（各版本子类，仅 1.20+ 模块提供） | Flink Tiered Storage API | Flink 1.20+ 混合 shuffle |

两套路径共用同一数据平面客户端 `FlinkShuffleClientImpl`、同一 `BufferPacker` 与同一套版本 shim，但 writer/reader 的封装方式不同。

### 2.2 核心类与职责

**标准 Remote Shuffle Service**（均在 `client-flink/common/src/main/java/org/apache/celeborn/plugin/flink/` 下）：

| 类 | 职责 |
|----|------|
| `RemoteShuffleServiceFactory` | Flink `ShuffleServiceFactory` 入口；创建 `RemoteShuffleMaster` 与 `RemoteShuffleEnvironment`；内嵌 `NettyShuffleServiceFactory` 作为 fallback。 |
| `RemoteShuffleMaster` | JobMaster 侧 `ShuffleMaster`；管理 `LifecycleManager`、注册/注销作业、生成 `RemoteShuffleDescriptor`、判断 fallback。 |
| `RemoteShuffleEnvironment` | TaskManager 侧 `ShuffleEnvironment`；创建 `RemoteShuffleResultPartition` 与 `RemoteShuffleInputGate`，并把 fallback partition 转交给 Netty 环境。 |
| `RemoteShuffleResultPartition` / `RemoteShuffleResultPartitionDelegation` | 写侧 result partition：用 `SortBasedDataBuffer` 缓冲记录，排序后写入 `RemoteShuffleOutputGate`。 |
| `RemoteShuffleOutputGate` | 单个 result partition 的传输门：registerShuffle、pushDataHandShake、regionStart、pushDataToLocation、regionFinish、mapPartitionMapperEnd。 |
| `RemoteShuffleInputGate` / `RemoteShuffleInputGateDelegation` | 读侧 input gate；并发创建 `RemoteBufferStreamReader`，接收 Netty buffer，unpack 后返回 `BufferOrEvent`。 |
| `FlinkShuffleClientImpl`（`client/` 子包） | 单例数据平面客户端，继承自 `ShuffleClientImpl`；管理 region/segment 状态、revive、credit-based buffer stream。 |
| `CelebornBufferStream`（`client/` 子包） | credit-based 流读取器，维护当前 `PartitionLocation`，失败时自动 failover 到下一个 location。 |
| `BufferPacker`（`buffer/` 子包） | 写时把多个 Flink buffer 打包成一个 Celeborn push buffer；读时拆包。 |
| `CelebornFlinkShim` | 抽象版本适配层，通过 `ServiceLoader` 加载对应版本的 shim（如 `CelebornFlinkShimV120`），隐藏 Flink API 差异。 |
| `fallback/` 子包 | `WorkersAvailableFallbackPolicy`、`QuotaFallbackPolicy`、`ForceFallbackPolicy` 等回退策略。 |

**Tiered / Hybrid Shuffle**（均在 `client-flink/common-tiered/src/main/java/org/apache/celeborn/plugin/flink/` 下）：

| 类 | 职责 |
|----|------|
| `BaseCelebornTierFactory`（`tiered/` 子包） | 公共 `TierFactory` 基类；构建 `CelebornTierMasterAgent`、`CelebornTierProducerAgent`、`CelebornTierConsumerAgent`。各版本模块提供子类 `CelebornTierFactory`（如 `flink-1.20/.../tiered/CelebornTierFactory.java`）。 |
| `CelebornTierMasterAgent` | Flink `TierMasterAgent`；管理 `LifecycleManager`，把 Flink job/partition 映射为 Celeborn shuffle descriptor。 |
| `CelebornTierProducerAgent` | Flink `TierProducerAgent`；接收 Flink hybrid-shuffle buffer，按 segment 聚合，发送 `segmentStart`、批量 buffer、注入 `EndOfSegmentEvent`。 |
| `CelebornTierConsumerAgent` | Flink `TierConsumerAgent`；创建 `CelebornChannelBufferReader`，为 tiered scheduler 提供 `peekNextBufferSubpartitionId` / `getNextBuffer`。 |
| `CelebornChannelBufferReader` | 单个 subpartition 的 credit-based reader。 |
| `ReceivedNoHeaderBufferPacker`（`buffer/` 子包，位于 `common` 模块） | tiered 模式下接收“无 header 数据 buffer”，在 producer 侧构造复合 header+data buffer。 |

### 2.3 作业注册与 fallback 判断

`RemoteShuffleServiceFactory.createShuffleMaster(...)` 返回 `RemoteShuffleMaster`：

```java
public class RemoteShuffleServiceFactory implements ShuffleServiceFactory {
    private final ShuffleServiceFactory fallbackFactory; // NettyShuffleServiceFactory
    ...
    @Override
    public ShuffleMaster<RemoteShuffleDescriptor> createShuffleMaster(...) {
        return new RemoteShuffleMaster(conf, fallbackFactory.createShuffleMaster(...));
    }
}
```

`RemoteShuffleMaster.registerJob(...)` 是作业 shuffle 化的起点：

1. 检查 `ExecutionOptions.BATCH_SHUFFLE_MODE == ALL_EXCHANGES_BLOCKING`，否则直接抛异常。Flink 流式/流水线 shuffle 不兼容标准 RSS。
2. 评估 fallback policy：
   - `WorkersAvailableFallbackPolicy`：无可用 worker 时回退。
   - `QuotaFallbackPolicy`：配额超限时回退。
   - 配置项 `celeborn.client.flink.shuffle.fallback.policy`：
     - `ALWAYS`：总是用 Netty。
     - `AUTO`（默认）：按 policy 判断。
     - `NEVER`：强制 Celeborn，不满足条件则失败。
3. 若走 Celeborn，创建 `LifecycleManager`（每个 JobManager 一个），并注册 job、生成 `RemoteShuffleDescriptor`。

### 2.4 标准写路径

`RemoteShuffleResultPartition.emitRecord(...)` / `broadcastRecord(...)` 把记录写入 `SortBasedDataBuffer`：

```java
// RemoteShuffleResultPartitionDelegation.flushDataBuffer
public void flushDataBuffer() {
    // 1. 从 buffer pool 申请 MemorySegment
    // 2. 按 subpartition 排序/取 buffer
    // 3. 可选压缩
    // 4. 调用 outputGate.write(buffer, subIdx)
}
```

`RemoteShuffleOutputGate` 写流程：

1. `registerShuffle()`：调用 `FlinkShuffleClientImpl.registerMapPartitionTask()`，得到唯一的 `PartitionLocation`。
2. `pushDataHandShake(...)`：针对当前 region 做握手，协商 epoch 与 location。
3. `regionStart(...)`：发送 `REGION_START` 消息，标记 region 边界。
4. `BufferPacker.add(...)` 累积 Flink buffer，达到阈值后调用 `FlinkShuffleClientImpl.pushDataToLocation(...)` 发送 `PushData`。
5. `regionFinish()`：发送 `REGION_FINISH`。
6. task 结束时 `mapPartitionMapperEnd(...)`：通知 worker 当前 map attempt 已完成。

### 2.5 标准读路径

`RemoteShuffleInputGate.setup()` 创建一组 `RemoteBufferStreamReader`：

```java
// RemoteShuffleInputGateDelegation.pollNext()
public BufferOrEvent pollNext() throws IOException, InterruptedException {
    // 1. 按需打开 reader，受 numConcurrentReading 限制
    // 2. 每个 reader 背后是一个 credit-based CelebornBufferStream
    // 3. 收到 ByteBuf 后 BufferPacker.unpack 成 Flink Buffer
    // 4. 封装为 BufferOrEvent 返回
}
```

`CelebornBufferStream` 的核心是维护当前 location，并在 `HARD_SPLIT` / 连接失败时通过 `FlinkShuffleClientImpl.revive(...)` 获取新 location，实现透明 failover。

### 2.6 Tiered 写路径

`CelebornTierProducerAgent` 直接消费 Flink hybrid-shuffle 框架传来的 buffer：

1. 按 subpartition + segment 聚合 buffer，segment 上限约 8 MB（`BaseCelebornTierFactory.MAX_BYTES_PER_SEGMENT = 8 * 1024 * 1024`，硬编码）。
2. 每 segment 开始发送 `segmentStart`。
3. 批量 buffer 通过 `FlinkShuffleClientImpl.pushDataToLocation(...)` 发送。
4. segment 结束注入 `EndOfSegmentEvent`，使下游即使上游仍在运行也能开始消费。

### 2.7 Tiered 读路径

`CelebornTierConsumerAgent.start()` 创建 `CelebornChannelBufferReader`，每个 reader 对应一个 subpartition。Flink tiered scheduler 调用 `peekNextBufferSubpartitionId()` / `getNextBuffer()` 拉取数据，实现“上游未完成、下游已可读”的混合流水线效果。

### 2.8 与 Master / Worker / LifecycleManager 的交互

| 阶段 | 交互对象 | 消息/调用 | 说明 |
|------|----------|-----------|------|
| 作业注册 | Master / LifecycleManager | `LifecycleManager.registerJob` → `RegisterApplication` | 每个 JobManager 一个 `LifecycleManager`。 |
| 获取 partition location | Worker / LifecycleManager | `FlinkShuffleClientImpl.registerMapPartitionTask` → `ReserveSlots` | Flink map partition 只保留 primary location。 |
| 数据发送 | Worker push server | `PushData` / `PushMergedData` | 通过 `FlinkShuffleClientImpl` 复用数据面连接池。 |
| region/segment 边界 | Worker | `REGION_START` / `REGION_FINISH` / `segmentStart` | 让 worker 按 region/segment 组织文件。 |
| mapper end | Worker | `MapPartitionMapperEnd` | 触发 worker 侧 commit / 文件元数据上报。 |
| 读流建立 | Worker fetch server | `OpenStream` / `ChunkFetchRequest` | credit-based 流控。 |
| revive / failover | Master / Worker | `ChangePartitionLocation` / `ReserveSlots` | `HARD_SPLIT` 或连接失败时换 worker。 |

---

## 3. MapReduce / Tez 客户端（薄封装路径）

### 3.1 模块定位

| 模块 | 规模 | 状态 | 核心策略 |
|------|------|------|----------|
| `client-mr/mr` | ~1,150 行 Java | 稳定可用 | 替换 map-side collector 与 reduce-side shuffle consumer，复用 Hadoop `MergeManager`。 |
| `client-tez/tez` | ~4,900 行 Java | **Experimental** | 通过 DAG AM 反射改写 edge input/output 类名，复用 Tez 的 merge/shuffle 抽象。 |

两者都是**薄封装**：不替换引擎完整 shuffle 子系统，而是把 Celeborn 的 block 流伪装成引擎原生的 `MapOutput` / `FetchedInput`，让引擎继续用现有排序/合并代码。

### 3.2 核心类与职责

**MapReduce 集成**：

| 引擎 Hook | Celeborn 类 | 路径 | 职责 |
|-----------|------------|------|------|
| `yarn.app.mapreduce.am.command-opts` | `MRAppMasterWithCeleborn` | `client-mr/mr/src/main/java/org/apache/celeborn/mapreduce/v2/app/MRAppMasterWithCeleborn.java` | 继承 Hadoop `MRAppMaster`；创建 `LifecycleManager`；把 LM host/port/appId 通过 `celeborn.xml` + distributed cache 分发给 task。 |
| `mapreduce.job.map.output.collector.class` | `CelebornMapOutputCollector` | `client-mr/mr/src/main/java/org/apache/hadoop/mapred/CelebornMapOutputCollector.java` | 替换 map-side collector；序列化记录到 `CelebornSortBasedPusher`。 |
| `mapreduce.job.reduce.shuffle.consumer.plugin.class` | `CelebornShuffleConsumer` | `client-mr/mr/src/main/java/org/apache/hadoop/mapreduce/task/reduce/CelebornShuffleConsumer.java` | 替换 reduce-side shuffle plugin；打开 Celeborn 输入流并喂给 `MergeManager`。 |
| — | `CelebornSortBasedPusher` | `client-mr/mr/src/main/java/org/apache/hadoop/mapred/CelebornSortBasedPusher.java` | 内存排序 + 打包 Celeborn push buffer。 |
| — | `CelebornShuffleFetcher` | `client-mr/mr/src/main/java/org/apache/hadoop/mapreduce/task/reduce/CelebornShuffleFetcher.java` | 从 Celeborn 输入流读 4-byte block，包装为 fake `MapOutput`。 |
| — | `HadoopUtils` | `client-mr/mr/src/main/java/org/apache/celeborn/util/HadoopUtils.java` | 配置前缀转换（`MR_PREFIX = "mapreduce."`，即 `mapreduce.celeborn.*` → `celeborn.*`）。 |

**Tez 集成**：

| 引擎 Hook | Celeborn 类 | 路径 | 职责 |
|-----------|------------|------|------|
| `tez.am.launch.cmd-opts` | `CelebornDagAppMaster` | `client-tez/tez/src/main/java/org/apache/tez/dag/app/CelebornDagAppMaster.java` | 继承 Tez `DAGAppMaster`；创建 `LifecycleManager`；反射改写每个 edge 的 input/output 类名为 Celeborn 变体。 |
| Edge output 替换 | `CelebornOrderedPartitionedKVOutput` / `CelebornUnorderedPartitionedKVOutput` / `CelebornUnorderedKVOutput` | `client-tez/tez/src/main/java/org/apache/tez/runtime/library/output/` | Map-side writer，把数据发向 Celeborn。 |
| Edge input 替换 | `CelebornOrderedGroupedKVInput` / `CelebornOrderedGroupedMergedKVInput` / `CelebornUnorderedKVInput` | `client-tez/tez/src/main/java/org/apache/tez/runtime/library/input/` | Reduce-side reader，从 Celeborn 拉取数据。 |
| — | `CelebornTezWriter` | `client-tez/tez/src/main/java/org/apache/celeborn/client/CelebornTezWriter.java` | 封装 `ShuffleClient.pushData` / `mergeData` / `pushMergedData` / `mapperEnd`。 |
| — | `CelebornTezReader` | `client-tez/tez/src/main/java/org/apache/celeborn/client/CelebornTezReader.java` | 封装 `ShuffleClient.readPartition` / chunk 读取。 |
| — | `CelebornSorter` / `CelebornSortBasedPusher` | `client-tez/tez/src/main/java/org/apache/tez/runtime/library/sort/` | 有序/无序输出共享的排序与 push 逻辑。 |
| — | `CelebornShuffleManager` / `CelebornFetcher` | `client-tez/tez/src/main/java/org/apache/tez/runtime/library/common/shuffle/impl/` | 无序输入的 shuffle 调度与 fetch。 |
| — | `CelebornShuffle` / `CelebornScheduler` / `CelebornTezShuffleDataFetcher` / `CelebornTezBypassWriter` | `client-tez/tez/src/main/java/org/apache/tez/runtime/library/common/shuffle/orderedgrouped/` | 有序 grouped 输入的调度、fetch 与 merge 旁路写。 |
| — | `CelebornTezUtils` | `client-tez/tez/src/main/java/org/apache/celeborn/tez/plugin/util/CelebornTezUtils.java` | 配置转换（`TEZ_PREFIX = "tez."`）等工具方法。 |

### 3.3 MapReduce 写路径

1. `CelebornMapOutputCollector.collect()` 把每条记录写入 `CelebornSortBasedPusher`。
2. `CelebornSortBasedPusher` 维护一块大 buffer（`mapreduce.task.io.sort.mb`）和 `HashMap<partition, List<SerializedKV>>`。
3. 达到 spill 阈值（`mapreduce.map.sort.spill.percent`）后，按 partition 排序，打包为 Celeborn push buffer：
   - 4-byte 长度头
   - 重复 `(varint keyLen, varint valueLen, serialized record)`
   - terminator `-1, -1`
4. 调用 `ShuffleClient.pushData(shuffleId=0, mapId, attempt, partition, ...)`。
5. `close()` 时调用 `ShuffleClient.mapperEnd(...)`。

> MapReduce 固定 `shuffleId=0`，因为 MR 只有一个全局 shuffle。

### 3.4 MapReduce 读路径

1. `CelebornShuffleConsumer.run()` 为 `shuffleId=0` 和当前 reducer partition 打开 `CelebornInputStream`。
2. `CelebornShuffleFetcher` 从流中读取 4-byte block。
3. 每个 block 被包装成一个**伪装的** Hadoop `InMemoryMapOutput`，附带合成的 `TaskAttemptID`，提交给 Hadoop `MergeManager`。
4. 所有 block 消费完后 `merger.close()`，返回最终 `RawKeyValueIterator`。

> 只支持 in-memory `MapOutput`，disk `MapOutput` 会抛异常。

### 3.5 Tez 写路径

1. DAG AM 为每条 edge 注入一个 `shuffleId`。
2. 每个 output 创建 `CelebornTezWriter`。
3. 有序输出使用 `CelebornSorter` + `CelebornSortBasedPusher`（`needSort=true`）。
4. 无序输出使用 `CelebornUnorderedPartitionedKVWriter` + `CelebornSortBasedPusher`（`needSort=false`）。
5. `CelebornTezWriter` 通过异步 `DataPusher` 发送 `pushData`，通过 `shuffleClient.mergeData` 发送最终 merge buffer，关闭时调用 `pushMergedData` 与 `mapperEnd`。
6. Writer 通过 `CelebornTezPerPartitionRecord` 上报每 partition 记录数，用于生成 Tez 事件。

### 3.6 Tez 读路径

- **有序 grouped 输入**：`CelebornOrderedGroupedKVInput` → `CelebornShuffle` → `CelebornScheduler` → `CelebornTezShuffleDataFetcher` → `CelebornTezReader` → 经 `CelebornTezBypassWriter` 写入 `MergeManager`。仅支持 memory `MapOutput`。
- **无序输入**：`CelebornUnorderedKVInput` → `CelebornShuffleManager` → `CelebornFetcher` → `CelebornTezReader`。`CelebornTezBypassWriter.write(FetchedInput)` 支持 memory 与 disk；disk 场景会写 TIF 头 + CRC32。

两套路径都把 Celeborn block 视为合成 `InputAttemptIdentifier`，从而复用 Tez 原生的 merge/sort 代码。

---

## 4. 跨引擎对比

| 维度 | MapReduce / Tez | Spark | Flink |
|------|-----------------|-------|-------|
| Hook 点 | 通过配置/反射替换 AM 与 shuffle 插件类 | 实现 Spark `ShuffleManager` | 实现 Flink `ShuffleServiceFactory` / Tiered Storage API |
| Merge 行为 | 复用引擎自身 `MergeManager`，伪造 map output / `FetchedInput` | Celeborn 直接提供 `CelebornInputStream`，不经过 Hadoop merge | Flink 使用 Celeborn 自定义 `ResultPartition` / `InputGate`，不伪造 map output |
| Shuffle ID | MR 固定 `0`；Tez 每 edge 一个 | Spark shuffleId 与 Celeborn shuffleId 复杂映射以支持 stage 重跑 | 与 Flink result partition / input gate ID 绑定 |
| Push 方式 | MR 同步 push；Tez 异步 `DataPusher` | 带发送缓冲池的 push writer，含 columnar 变体 | 网络缓冲池 buffer-based push，支持 hybrid tiered |
| 版本覆盖 | 单一 MR 模块；单一 Tez 模块（实验） | Spark 2.x/3.x/4.x 多模块、columnar shuffle | Flink 1.16–2.3 共 9 个版本模块；tiered shuffle 仅 1.20+ |
| 反射依赖 | 很重，Tez 大量 `getParentPrivateField` | 少量跨版本反射 | 集中在版本 shim，较收敛 |

**结论**：MR/Tez 集成更薄、侵入更小，但因此无法充分利用 Celeborn 的零拷贝、列式、分层等高级特性；Spark/Flink 集成更深，控制力更强。

---

## 5. 各引擎配置

> 配置键名与默认值均以 `common/src/main/scala/org/apache/celeborn/common/CelebornConf.scala` 为准；功能域全集导读见 [celeborn-conf-functional-map.md](./celeborn-conf-functional-map.md)。

### 5.1 Flink 专属配置

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.client.flink.shuffle.fallback.policy` | `AUTO` | `ALWAYS` / `AUTO` / `NEVER`，控制是否回退到 Flink Netty shuffle。 |
| `celeborn.client.flink.inputGate.concurrentReadings` | `Int.MaxValue` | 每个 input gate 最大并发读 channel 数。 |
| `celeborn.client.flink.resultPartition.memory` | `64m` | 每个 result partition 的网络内存。 |
| `celeborn.client.flink.inputGate.memory` | `32m` | 每个 input gate 的网络内存。 |
| `celeborn.client.flink.inputGate.supportFloatingBuffer` | `true` | input gate 是否允许 floating buffer。 |
| `celeborn.client.flink.resultPartition.supportFloatingBuffer` | `true` | result partition 是否允许 floating buffer。 |
| `celeborn.client.flink.compression.enabled` | `true` | Flink 插件内是否启用数据压缩。 |
| `celeborn.client.flink.open.stream.threads` | #cores | 并发打开 credit-based buffer stream 的线程池大小。 |
| `celeborn.client.flink.partitionConnectionException.enabled` | `false` | 连接失败是否翻译为 Flink `PartitionConnectionException` 以触发重算。 |

### 5.2 通用 Celeborn 配置在 Flink 下的特殊行为

| 配置键 | 说明 |
|--------|------|
| `celeborn.client.push.replicate.enabled` | **必须 false**；Flink 当前不支持 replica，`FlinkUtils.toCelebornConf` 检测到该键为 `true` 时直接抛 `IllegalArgumentException`。 |
| `celeborn.client.checked.useAllocatedWorkers` | Flink 下通过 `setIfMissing` 默认设为 `true`（全局默认 `false`）。 |
| `celeborn.client.push.revive.maxRetries` | 默认 `5`；控制 `HARD_SPLIT` / push 失败后的 revive 次数。 |
| `celeborn.<module>.io.maxRetries` | IO 失败重试次数，默认 `3`。 |
| `celeborn.client.shuffle.integrityCheck.enabled` | 默认 `false`；Spark/Flink 端到端完整性校验。 |

### 5.3 Flink 遗留别名（仍生效）

- `remote-shuffle.job.concurrent-readings-per-gate`
- `remote-shuffle.job.memory-per-partition`
- `remote-shuffle.job.memory-per-gate`
- `remote-shuffle.job.support-floating-buffer-per-input-gate`
- `remote-shuffle.job.support-floating-buffer-per-output-gate`
- `remote-shuffle.job.enable-data-compression`
- `remote-shuffle.job.compression.codec`

### 5.4 MapReduce

| 配置键 | 说明 |
|--------|------|
| `mapreduce.celeborn.master.endpoints` / 环境变量 `CELEBORN_MASTER_ENDPOINTS` | Celeborn master 端点（`HadoopUtils.MR_PREFIX = "mapreduce."` 拼接 `celeborn.master.endpoints`）。 |
| `mapreduce.job.map.output.collector.class` | 设为 `org.apache.hadoop.mapred.CelebornMapOutputCollector`。 |
| `mapreduce.job.reduce.shuffle.consumer.plugin.class` | 设为 `org.apache.hadoop.mapreduce.task.reduce.CelebornShuffleConsumer`。 |
| `yarn.app.mapreduce.am.command-opts` | 启动 `MRAppMasterWithCeleborn`。 |
| `mapreduce.task.io.sort.mb` / `mapreduce.map.sort.spill.percent` | 内存排序 buffer 与 spill 阈值。 |
| `celeborn.client.mr.pushData.max` | 单次 push buffer 最大值，默认 `32m`，必须 < 2GB。 |

### 5.5 Tez

| 配置键 | 说明 |
|--------|------|
| `tez.celeborn.master.endpoints` / 环境变量 `CELEBORN_MASTER_ENDPOINTS` | Celeborn master 端点（`CelebornTezUtils.TEZ_PREFIX = "tez."` 拼接）。 |
| `tez.am.launch.cmd-opts` | 启动 `CelebornDagAppMaster`。 |
| `celeborn.client.mr.pushData.max` | Tez 复用 MR 命名空间的配置（无独立 `celeborn.client.tez.*`）。 |

### 5.6 通用配置前缀转换与 AM 强制修改

- MR：`mapreduce.celeborn.*` → 去掉 `mapreduce.` → `celeborn.*`
- Tez：`tez.celeborn.*` → 去掉 `tez.` → `celeborn.*`

AM 还会强制修改若干引擎配置：

- MR：关闭 AM recovery，reduce slow-start 设为 1.0。
- Tez：min/max src-fraction 设为 1.0，关闭 transfer-data-via-events、pipelined shuffle、unhealthy-node rescheduling。

---

## 6. 各引擎已知问题与可优化点

### 6.1 Flink

1. **单例 `FlinkShuffleClientImpl` 的全局状态**：静态 `_instance` 依赖 timestamp 比较来感知 Driver 变化，对测试与多租户 classloader 不友好。
2. **TODO 未闭环**：
   - `pushDataHandShake` 失败原因未用于 revive（`// TODO revive with handshake failure reason`）。
   - segment 可见场景下空 location 应重试而非抛 `PartitionUnRetryAbleException`。
   - tiered consumer reader 打开失败应重试。
3. **硬编码常量**：`MAX_BYTES_PER_SEGMENT = 8 MB`、result partition 7/8 排序与 1/8 传输的固定拆分比例未暴露为配置。
4. **反射 shim 脆弱**：`CelebornFlinkShimV120` 通过反射访问 `resultPartitionBytes` 等私有字段，跨 Flink 版本升级风险高。
5. **多租户用户映射缺失**：多处使用 `new UserIdentifier("default", "default")`，未与 Celeborn 租户系统打通。
6. **混合模式复杂度高**：`RemoteShuffleEnvironment` 需同时维护 Celeborn 与 Netty 两套 result partition，fallback 判断与状态追踪较复杂。

### 6.2 MapReduce / Tez

1. **反射过重**：`CelebornDagAppMaster`、`CelebornShuffleManager`、`CelebornScheduler`、`CelebornShuffle` 大量读写父类私有字段，跨 Hadoop/Tez 版本脆弱。
2. **伪 map-output 模型带来额外拷贝**：MR/Tez 有序合并路径把 Celeborn block 转成合成 `MapOutput`/`FetchedInput`，让 reducer 再次缓冲/合并 Celeborn 已外部存储的数据。
3. **不支持 disk-backed 有序合并**：`CelebornShuffleFetcher.writeShuffle()` 与 `CelebornTezBypassWriter.write(MapOutput)` 在 disk `MapOutput` 时抛异常，迫使提升 reducer 内存。
4. **静态可变 ID 存在并发风险**：
   - `CelebornUnorderedPartitionedKVOutput.mapId`
   - `CelebornUnorderedKVOutput.mapId`
   - fetcher 中的 static `uniqueMapId`
   这些共享可变状态在多线程/多 task JVM 中可能冲突。
5. **参数映射疑似错误**：若干 output 类调用 `new CelebornTezWriter(shuffleId, mapId, mapId, attemptId, ...)`，构造函数签名为 `(shuffleId, mapId, attemptNumber, taskAttemptId, ...)`，第二个 `mapId` 实际被当作 attempt number 传入。
6. **广播/one-to-one 处理用静态可变状态**：`CelebornUnorderedKVOutput` 的 `static broadcastOrOntToOne`、`static mapId` 在 session 模式下存在风险。
7. **Metrics callback 为空实现**：MR/Tez 的 `MetricsCallback` 均未实现 `incBytesRead` / `incReadTime`，丢失有用的 shuffle 读指标。
8. **MR fetcher 异常吞没**：`CelebornShuffleFetcher.fetchAndMerge()` 捕获 `Exception` 后继续循环，可能在持久错误时空转。
9. **HashMap 分区记账成本高**：`CelebornSortBasedPusher` 使用 `HashMap<Integer, List<SerializedKV>>` 与 per-partition `AtomicInteger`/`AtomicLong`，对常见稠密分区可用原生数组替代。
10. **Tez 配置命名空间泄露**：Tez 复用 `celeborn.client.mr.pushData.max`，建议新增 `celeborn.client.tez.*` 命名空间。

---

## 7. 延伸阅读

- [client-control-plane-deep-dive.md](./client-control-plane-deep-dive.md) — `LifecycleManager`、commit、revive 的控制平面细节。
- [network-and-protocol-deep-dive.md](./network-and-protocol-deep-dive.md) — 数据面消息格式、push/pull 协议、Netty RPC、连接池、消息编解码。
- [celeborn-conf-functional-map.md](./celeborn-conf-functional-map.md) — 配置键功能域导读（含 §6–8 各引擎客户端配置）。
- [cli-and-rest-api-analysis.md](./cli-and-rest-api-analysis.md) — CLI 与 REST API 运维入口。
- 官方开发者文档：`docs/developers/client.md`（Spark 客户端）、`docs/developers/shuffleclient.md`、`docs/developers/lifecyclemanager.md`。
- Flink 官方文档：`https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/batch/`
