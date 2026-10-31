# Apache Celeborn 配置功能域导读

> 基于版本：**0.7.0-SNAPSHOT**　整理日期：**2026-08-05**
>
> 本文按功能域人工精选 `CelebornConf.scala` 中的关键配置键（键名 / 默认值 / 一句话说明），作为源码阅读与调优的导航。
> 与官方文档的关系：`docs/configuration/` 下的官方配置文档由 `ConfigurationSuite` 自动生成，是**全量、权威**的参考；本文是**人工精选的功能域导读**，不求全，只求准——所有键名与默认值均以 `common/src/main/scala/org/apache/celeborn/common/CelebornConf.scala` 为准。

---

## 1. HA / Ratis

Master 高可用基于 Apache Ratis（Raft）实现，相关键前缀为 `celeborn.master.ha.*`（旧别名 `celeborn.ha.*` 仍生效）。

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.master.ha.enabled` | `false` | 开启后 master 以 Raft 集群模式运行。 |
| `celeborn.master.ha.node.id` | —（按 hostname 推断） | 本节点在 Raft 集群中的 node id。 |
| `celeborn.master.ha.node.<id>.host` | `<required>` | HA 模式下节点 `<id>` 的绑定 host。 |
| `celeborn.master.ha.node.<id>.port` | `9097` | HA 模式下节点 `<id>` 的绑定端口。 |
| `celeborn.master.ha.node.<id>.ratis.port` | `9872` | 节点 `<id>` 的 Ratis 端口。 |
| `celeborn.master.ha.ratis.raft.rpc.type` | `netty` | Ratis RPC 类型：`netty` / `grpc`。 |
| `celeborn.master.ha.ratis.raft.server.storage.dir` | `/tmp/ratis` | RaftServer 数据根目录。 |
| `celeborn.master.ha.ratis.raft.server.rpc.request.timeout` | `3s` | Raft server RPC 请求超时。 |
| `celeborn.master.ha.ratis.raft.server.rpc.timeout.min` / `.max` | `3s` / `5s` | Raft RPC 超时最小/最大值（选举超时区间）。 |
| `celeborn.master.ha.ratis.first.election.timeout.min` / `.max` | `3s` / `5s` | 首次选举超时区间。 |
| `celeborn.master.ha.ratis.raft.server.notification.no-leader.timeout` | `30s` | 无 leader 通知超时。 |
| `celeborn.master.ha.ratis.raft.server.snapshot.auto.trigger.enabled` | `true` | 自动触发 snapshot。 |
| `celeborn.master.ha.ratis.raft.server.snapshot.auto.trigger.threshold` | `200000` | 触发 snapshot 的日志条目阈值。 |
| `celeborn.master.ha.graceful.shutdown.enabled` | `false` | 优雅停机前转移 Raft leadership（0.7.0 新增）。 |
| `celeborn.master.ha.graceful.shutdown.timeout` | `30s` | 优雅停机（含 leadership 转移）超时。 |

---

## 2. Master

### 2.1 基础与心跳

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.master.host` | `<localhost>` | Master 绑定 host。 |
| `celeborn.master.port` | `9097` | Master 绑定端口。 |
| `celeborn.master.heartbeat.application.timeout` | `300s` | 应用心跳超时。 |
| `celeborn.master.heartbeat.worker.timeout` | `120s` | Worker 心跳超时。 |
| `celeborn.master.workerUnavailableInfo.expireTimeout` | `1800s` | Worker 不可用信息保留时间（`-1` 不过期）。 |
| `celeborn.master.allowWorkerHostPattern` / `celeborn.master.denyWorkerHostPattern` | — | 允许/拒绝注册的 worker host 正则。 |

### 2.2 Slot 分配

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.master.slot.assign.policy` | `ROUNDROBIN` | 分配策略：`ROUNDROBIN` / `LOADAWARE`（启用 HDFS 时 LOADAWARE 不生效）。 |
| `celeborn.master.slot.assign.loadAware.numDiskGroups` | `5` | load-aware 算法的磁盘分组数。 |
| `celeborn.master.slot.assign.loadAware.diskGroupGradient` | `0.1` | 快盘组相对慢盘组的负载梯度。 |
| `celeborn.master.slot.assign.loadAware.flushTimeWeight` / `fetchTimeWeight` | `0` / `1` | load-aware 排序中 flush/fetch 时间权重。 |
| `celeborn.master.slot.assign.extraSlots` | `2` | 分配时的额外 slot 数。 |
| `celeborn.master.slot.assign.maxWorkers` | `10000` | 单 shuffle slot 可分配的最大 worker 数（与客户端侧取较小值）。 |
| `celeborn.master.slot.assign.minWorkers` | `100` | 单 shuffle slot 应分配的最小 worker 数。 |
| `celeborn.master.slot.assign.interruptionAware` | `false` | 优先把分区放到近期无维护计划的 worker 上（0.7.0 新增）。 |

### 2.3 分区大小估算

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.master.estimatedPartitionSize.initialSize` | `64mb` | 分区大小估算初始值。 |
| `celeborn.master.estimatedPartitionSize.minSize` | `8mb` | 小于该值的分区不参与估算。 |
| `celeborn.master.estimatedPartitionSize.update.interval` | `10min` | 估算更新间隔。 |

---

## 3. Worker 基础

### 3.1 端口与注册

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.worker.rpc.port` / `push.port` / `fetch.port` / `replicate.port` | `0` | Worker RPC / push / fetch / replicate 服务端口，`0` 表示随机。 |
| `celeborn.worker.register.timeout` | `180s` | Worker 向 master 注册超时。 |
| `celeborn.worker.closeIdleConnections` | `false` | Worker 是否关闭空闲连接。 |

### 3.2 存储目录与类型

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.worker.storage.dirs` | — | shuffle 数据目录列表，建议每盘一个目录，可带 `:capacity=:disktype=:flushthread=` 参数。 |
| `celeborn.worker.storage.workingDir` | `celeborn-worker/shuffle_data` | Worker 工作目录名。 |
| `celeborn.worker.storage.expireDirs.timeout` | `1h` | 磁盘上过期目录删除超时。 |
| `celeborn.storage.availableTypes` | `HDD` | 启用存储类型：`MEMORY,HDD,SSD,HDFS,S3,OSS`。 |

### 3.3 Flush 与 Commit

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.worker.flusher.buffer.size` | `256k` | 单个本地 flusher 的 buffer 大小。 |
| `celeborn.worker.flusher.threads` / `hdd.threads` / `ssd.threads` | `16` / `1` / `16` | 未知类型 / HDD / SSD 每盘 flush 线程数。 |
| `celeborn.worker.flusher.shutdownTimeout` | `3s` | flusher 关闭超时。 |
| `celeborn.worker.commitFiles.threads` | `32` | 异步 commit 文件线程数（启用 HDFS 建议 ≥128）。 |
| `celeborn.worker.commitFiles.timeout` | `120s` | 单 shuffle commit 文件超时。 |
| `celeborn.worker.commitFiles.fsync` | `false` | commit 时 fsync 落盘保证持久性（0.7.0 新增）。 |
| `celeborn.worker.clean.threads` | `64` | 清理过期 shuffle 的线程数。 |
| `celeborn.worker.writer.close.timeout` | `120s` | 文件 writer 关闭超时。 |
| `celeborn.worker.shuffle.partitionSplit.max` | `2g` | 分区切分上限，单分区文件始终小于该值。 |

### 3.4 复制与优雅停机

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.worker.replicate.threads` | `64` | worker 间复制数据线程数。 |
| `celeborn.worker.replicate.fastFail.duration` | `60s` | 复制请求超时快速失败时长（HDFS 建议 ≥240s）。 |
| `celeborn.worker.graceful.shutdown.enabled` | `false` | 优雅停机：等待已释放 slot 完成 commit/destroy。 |
| `celeborn.worker.graceful.shutdown.timeout` | `600s` | Worker 优雅停机总超时。 |

---

## 4. Client 基础

客户端（各引擎共用）连接 master 与 LifecycleManager 的基石配置；注册/push/fetch 等细节见第 5 节。

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.master.endpoints` | `<localhost>:9097` | 客户端连接的 master 端点列表。 |
| `celeborn.master.endpoints.resolver` | `StaticMasterEndpointResolver` | master 端点解析器，可自定义实现动态发现。 |
| `celeborn.client.shuffle.manager.port` | `0` | Driver 上 LifecycleManager 端口，`0` 表示随机。 |
| `celeborn.masterClient.rpc.askTimeout` | `rpc.askTimeout` | HA client ask 超时。 |
| `celeborn.masterClient.maxRetries` | `15` | 连接 master 端点最大重试（至少等于端点数）。 |
| `celeborn.client.closeIdleConnections` | `true` | 客户端是否关闭空闲连接。 |
| `celeborn.client.push.sort.randomizePartitionId.enabled` | `false` | sort push 随机化 partitionId 以避免 worker 倾斜。 |

---

## 5. Client / LifecycleManager

### 5.1 注册与预留 Slot

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.client.registerShuffle.maxRetries` | `3` | 注册 shuffle 最大重试。 |
| `celeborn.client.registerShuffle.retryWait` | `3s` | 注册重试等待。 |
| `celeborn.client.reserveSlots.maxRetries` | `3` | 预留 slot 最大重试。 |
| `celeborn.client.reserveSlots.retryWait` | `3s` | 预留重试等待。 |
| `celeborn.client.reserveSlots.rackaware.enabled` | `false` | 副本跨机架放置。 |
| `celeborn.client.slot.assign.maxWorkers` | `10000` | 客户端侧单 shuffle 最大 worker 数。 |
| `celeborn.client.checked.useAllocatedWorkers` | `false` | 检查丢失数据时使用已分配 worker。 |
| `celeborn.client.shuffle.register.filterExcludedWorker.enabled` | `false` | 注册时过滤排除 worker。 |
| `celeborn.client.shuffle.reviseLostShuffles.enabled` | `false` | 修正丢失 shuffle。 |
| `celeborn.client.shuffle.checkWorker.enabled` | `true` | 注册前检查集群可用性。 |

### 5.2 应用心跳

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.client.application.heartbeatInterval` | `10s` | 客户端向 master 心跳间隔。 |
| `celeborn.client.application.unregister.enabled` | `true` | 应用关闭时通知 master。 |
| `celeborn.client.application.uuidSuffix.enabled` | `false` | 应用 ID 追加 UUID 后缀（Spark/MR）。 |

### 5.3 Commit 与 RPC

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.client.requestCommitFiles.maxRetries` | `4` | commit-files RPC 最大重试。 |
| `celeborn.client.requestCommitFiles.retryWait` | `10s` | commit-files 重试等待。 |
| `celeborn.client.commitFiles.ignoreExcludedWorker` | `false` | commit 时跳过排除 worker。 |
| `celeborn.client.rpc.reserveSlots.askTimeout` | `rpc.askTimeout` | reserveSlots ask 超时。 |
| `celeborn.client.rpc.registerShuffle.askTimeout` | `rpc.askTimeout` | registerShuffle ask 超时。 |
| `celeborn.client.rpc.requestPartition.askTimeout` | `rpc.askTimeout` | change/revive/split ask 超时。 |
| `celeborn.client.rpc.commitFiles.askTimeout` | `rpc.askTimeout` | commitFiles ask 超时。 |
| `celeborn.client.rpc.getReducerFileGroup.askTimeout` | `rpc.askTimeout` | getReducerFileGroup ask 超时。 |
| `celeborn.client.rpc.shared.threads` | `16` | LifecycleManager 共享 RPC 线程。 |
| `celeborn.client.rpc.maxRetries` | `3` | 客户端 RPC 最大重试。 |
| `celeborn.client.rpc.cache.size` | `256` | RPC 缓存条目数。 |
| `celeborn.client.rpc.cache.expireTime` | `15s` | RPC 缓存过期时间。 |

### 5.4 Push Client

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.client.push.replicate.enabled` | `false` | 是否开启双副本。 |
| `celeborn.client.push.buffer.max.size` | `64k` | hash writer 单分区 buffer 上限。 |
| `celeborn.client.push.buffer.initial.size` | `8k` | 初始 push buffer 大小。 |
| `celeborn.client.push.queue.capacity` | `512` | 每 task push buffer 队列大小。 |
| `celeborn.client.push.maxReqsInFlight.total` | `256` | 全局在途 push 请求数。 |
| `celeborn.client.push.maxReqsInFlight.perWorker` | `32` | 单 worker 在途 push 请求数。 |
| `celeborn.client.push.maxBytesSizeInFlight.enabled` | `false` | 是否按字节限制在途。 |
| `celeborn.client.push.maxBytesSizeInFlight.total` | — | 全局在途字节上限。 |
| `celeborn.client.push.maxBytesSizeInFlight.perWorker` | — | 单 worker 在途字节上限。 |
| `celeborn.client.push.timeout` | `120s` | push data 超时。 |
| `celeborn.client.push.revive.maxRetries` | `5` | revive 最大重试。 |
| `celeborn.client.push.revive.interval` | `100ms` | revive 批量触发间隔。 |
| `celeborn.client.push.revive.batchSize` | `2048` | 单次 revive 最大分区数。 |
| `celeborn.client.push.excludeWorkerOnFailure.enabled` | `false` | push 失败时排除 worker。 |
| `celeborn.client.push.retry.threads` | `8` | push 数据重发线程数。 |
| `celeborn.client.push.stageEnd.timeout` | network timeout | stageEnd 超时。 |
| `celeborn.client.push.limit.strategy` | `SIMPLE` | 限速策略：`SIMPLE` / `SLOWSTART`。 |
| `celeborn.client.push.slowStart.initialSleepTime` | `500ms` | slow-start 初始 sleep。 |
| `celeborn.client.push.slowStart.maxSleepTime` | `2s` | slow-start 最大 sleep。 |
| `celeborn.client.push.sendBufferPool.expireTimeout` | `60s` | send-buffer pool 空闲过期。 |

### 5.5 Fetch Client

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.client.fetch.timeout` | `600s` | open-stream / fetch-chunk 超时。 |
| `celeborn.client.fetch.pollChunk.wait` | `500ms` | worker 空 chunk 等待。 |
| `celeborn.client.fetch.buffer.size` | `64k` | reducer 端 fetch buffer。 |
| `celeborn.client.fetch.maxReqsInFlight` | `3` | 并发 chunk fetch 请求数。 |
| `celeborn.client.fetch.maxRetriesForEachReplica` | `3` | 每副本 fetch 重试。 |
| `celeborn.client.fetch.excludeWorkerOnFailure.enabled` | `false` | fetch 失败排除 worker。 |
| `celeborn.client.fetch.excludedWorker.expireTimeout` | 1.5×worker heartbeat | fetch 排除 worker 过期时间。 |
| `celeborn.client.spark.stageRerun.enabled` | `true` | 抛 `FetchFailedException` 而非 Celeborn IO 异常。 |
| `celeborn.client.spark.fetch.cleanFailedShuffle` | `false` | 清理不可 fetch shuffle 的磁盘空间。 |
| `celeborn.client.chunk.prefetch.enabled` | `false` | 创建输入流时预取 chunk。 |
| `celeborn.client.inputStream.creation.window` | `16` | 合并读时预创建输入流窗口。 |
| `celeborn.client.partition.reader.checkpoint.enabled` | `false` | 读取 checkpoint 避免重读。 |

### 5.6 批量 Shuffle 操作

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.client.shuffle.batchHandleChangePartition.enabled` | `true` | 批量 change-partition。 |
| `celeborn.client.shuffle.batchHandleChangePartition.threads` | `8` | 批量 change 线程。 |
| `celeborn.client.shuffle.batchHandleChangePartition.interval` | `100ms` | 批量 change 调度间隔。 |
| `celeborn.client.shuffle.batchHandleChangePartition.partitionBuckets` | `256` | change partition 并发桶。 |
| `celeborn.client.shuffle.batchHandleCommitPartition.enabled` | `true` | 批量 commit。 |
| `celeborn.client.shuffle.batchHandleCommitPartition.threads` | `8` | 批量 commit 线程。 |
| `celeborn.client.shuffle.batchHandleCommitPartition.interval` | `5s` | 批量 commit 调度间隔。 |
| `celeborn.client.shuffle.batchHandleReleasePartition.enabled` | `true` | 批量 release。 |
| `celeborn.client.shuffle.batchHandleReleasePartition.threads` | `8` | 批量 release 线程。 |
| `celeborn.client.shuffle.batchHandleReleasePartition.interval` | `5s` | 批量 release 调度间隔。 |
| `celeborn.client.shuffle.batchHandleRemoveExpiredShuffles.enabled` | `false` | 批量移除过期 shuffle。 |

### 5.7 Shuffle 行为

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.client.shuffle.partition.type` | `REDUCE` | 分区类型：`REDUCE` / `MAP`。 |
| `celeborn.client.shuffle.partitionSplit.threshold` | `1G` | 触发分区切分的文件大小阈值。 |
| `celeborn.client.shuffle.partitionSplit.mode` | `SOFT` | `SOFT` 可超限，`HARD` 严格。 |
| `celeborn.client.shuffle.expired.checkInterval` | `60s` | 过期 shuffle 检查间隔。 |
| `celeborn.client.shuffle.dynamicResourceEnabled` | `false` | 心跳时动态拉取候选 worker。 |
| `celeborn.client.shuffle.dynamicResourceFactor` | `0.5` | 动态资源触发不可用比例。 |
| `celeborn.client.shuffle.integrityCheck.enabled` | `false` | Spark/Flink 端到端完整性校验。 |
| `celeborn.client.shuffleDataLostOnUnknownWorker.enabled` | `false` | 未知 worker 时标记数据丢失。 |
| `celeborn.shuffle.chunk.size` | `8m` | reducer 合并 fetch 最大 chunk。 |
| `celeborn.client.fetch.dfsReadChunkSize` | `8m` | DFS partition reader chunk 大小。 |

### 5.8 排除 Worker 与失败处理

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.client.excludedWorker.expireTimeout` | `180s` | 客户端排除 worker 过期时间。 |
| `celeborn.client.excludePeerWorkerOnFailure.enabled` | `true` | replica push 失败时排除 peer worker。 |

---

## 6. Spark Client

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.client.spark.shuffle.writer` | `HASH` | shuffle writer：`HASH` / `SORT`。 |
| `celeborn.client.spark.push.dynamicWriteMode.enabled` | `false` | 按分区数自动切换 writer。 |
| `celeborn.client.spark.push.dynamicWriteMode.partitionNum.threshold` | `2000` | 动态切换分区数阈值。 |
| `celeborn.client.spark.push.sort.memory.threshold` | `64m` | sort pusher flush 内存阈值。 |
| `celeborn.client.spark.push.sort.memory.useAdaptiveThreshold` | `false` | 自适应 sort writer 阈值。 |
| `celeborn.client.spark.push.sort.memory.smallPushTolerateFactor` | `0.2` | 自适应小 push 容忍因子。 |
| `celeborn.client.spark.push.sort.memory.maxMemoryFactor` | `0.4` | sort writer 最大内存占比。 |
| `celeborn.client.spark.push.unsafeRow.fastWrite.enabled` | `true` | UnsafeRow 快速写优化。 |
| `celeborn.client.spark.shuffle.fallback.policy` | `AUTO` | `ALWAYS`/`AUTO`/`NEVER` 回退 Spark shuffle。 |
| `celeborn.client.spark.shuffle.fallback.numPartitionsThreshold` | `Int.MaxValue` | AUTO 回退最大分区数。 |
| `celeborn.client.spark.shuffle.forceFallback.enabled` | `false` | 已废弃，用 `fallback.policy`。 |
| `celeborn.client.spark.shuffle.getReducerFileGroup.broadcast.enabled` | `false` | 广播 reducer file-group 响应。 |
| `celeborn.client.spark.shuffle.getReducerFileGroup.broadcast.miniSize` | `512k` | 广播触发最小响应大小。 |
| `celeborn.client.spark.batch.openStream.parallelClientCreation.enabled` | `true` | 批量 open-stream 前并行创建 data client。 |
| `celeborn.client.readLocalShuffleFile.enabled` | `false` | 与 NodeManager 同机时读本地 shuffle 文件。 |
| `celeborn.client.readLocalShuffleFile.threads` | `4` | 本地 shuffle 文件读取线程。 |
| `celeborn.client.eagerlyCreateInputStream.threads` | `32` | 提前创建输入流线程。 |
| `celeborn.client.adaptive.optimizeSkewedPartitionRead.enabled` | `false` | 自适应切分倾斜分区读。 |
| `celeborn.client.shuffle.rangeReadFilter.enabled` | `false` | 跳过倾斜 Spark 分区的无关范围。 |

---

## 7. Flink Client

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.client.flink.shuffle.fallback.policy` | `AUTO` | `ALWAYS`/`AUTO`/`NEVER` 回退 Flink shuffle。 |
| `celeborn.client.flink.inputGate.memory` | `32m` | 每 input gate 内存。 |
| `celeborn.client.flink.resultPartition.memory` | `64m` | 每 result partition 内存。 |
| `celeborn.client.flink.inputGate.concurrentReadings` | `Int.MaxValue` | 每 input gate 最大并发读 channel。 |
| `celeborn.client.flink.inputGate.supportFloatingBuffer` | `true` | input gate floating buffer。 |
| `celeborn.client.flink.resultPartition.supportFloatingBuffer` | `true` | result partition floating buffer。 |
| `celeborn.client.flink.compression.enabled` | `true` | Flink 插件压缩开关。 |
| `celeborn.client.flink.open.stream.threads` | #cores | 打开 buffer stream 线程池。 |
| `celeborn.client.flink.metrics.scope.shuffle` | `<host>.taskmanager...` | Flink shuffle 指标 scope。 |
| `celeborn.client.flink.partitionConnectionException.enabled` | `false` | 连接失败转 Flink `PartitionConnectionException`。 |

> 遗留别名仍生效：`remote-shuffle.job.concurrent-readings-per-gate`、`remote-shuffle.job.memory-per-partition` 等。

---

## 8. MapReduce / Tez Client

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `mapreduce.celeborn.master.endpoints` / `CELEBORN_MASTER_ENDPOINTS` | — | MR master 端点。 |
| `tez.celeborn.master.endpoints` / `CELEBORN_MASTER_ENDPOINTS` | — | Tez master 端点。 |
| `celeborn.client.mr.pushData.max` | `32m` | MR/Tez 单次 push buffer 最大值（<2GB）。 |

> Tez 目前复用 `celeborn.client.mr.pushData.max`，尚无独立 `celeborn.client.tez.*` 命名空间。

---

## 9. Network / RPC / Netty

### 9.1 全局网络

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.network.timeout` | `240s` | 默认网络操作超时。 |
| `celeborn.network.connect.timeout` | `10s` | socket 连接超时。 |
| `celeborn.network.bind.preferIpAddress` | `true` | 绑定优先使用 IP。 |
| `celeborn.network.advertise.preferIpAddress` | fallback to bind | 广播地址优先使用 IP。 |
| `celeborn.network.bind.wildcardAddress` | `false` | 是否绑定通配地址。 |
| `celeborn.port.maxRetries` | `16` | 端口占用重试次数。 |

### 9.2 RPC 框架

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.rpc.askTimeout` | `60s` | RPC ask 超时。 |
| `celeborn.rpc.lookupTimeout` | `30s` | RPC lookup 超时。 |
| `celeborn.rpc.io.threads` | #cores | RPC IO 线程。 |
| `celeborn.rpc.connect.threads` | `64` | RPC 连接建立线程。 |
| `celeborn.rpc.dispatcher.threads` | `0` | dispatcher 线程，`0`=#cores。 |
| `celeborn.<role>.rpc.dispatcher.threads` | fallback | 按角色 dispatcher 线程。 |
| `celeborn.rpc.inbox.capacity` | `0` | inbox 容量，`0` 无界。 |
| `celeborn.rpc.retryWait` | `1s` | RPC 超时重试等待。 |
| `celeborn.rpc.slow.threshold` | `1s` | 慢 RPC 日志阈值。 |
| `celeborn.rpc.dump.interval` | `60s` | RPC 性能汇总 dump 间隔。 |
| `celeborn.masterClient.rpc.askTimeout` | `rpc.askTimeout` | HA client ask 超时。 |
| `celeborn.masterClient.maxRetries` | `15` | master 端点最大重试。 |

### 9.3 传输模块（`<module>`）

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.<module>.io.mode` | EPOLL/KQUEUE/NIO | Netty event-loop 后端。 |
| `celeborn.<module>.io.preferDirectBufs` | `true` | 优先堆外 buffer。 |
| `celeborn.<module>.io.connectTimeout` | network.connect.timeout | 连接超时。 |
| `celeborn.<module>.io.connectionTimeout` | network.timeout | 连接活跃超时。 |
| `celeborn.<module>.io.numConnectionsPerPeer` | `1` | 两点间并发连接数。 |
| `celeborn.<module>.io.backLog` | `0` | TCP backlog。 |
| `celeborn.<module>.io.threads` | `8` | 默认线程池大小。 |
| `celeborn.<module>.io.serverThreads` | `0` | server 线程池，`0`=2×cores。 |
| `celeborn.<module>.io.clientThreads` | `0` | client 线程池，`0`=2×cores。 |
| `celeborn.<module>.io.conflictAvoidChooser.enable` | `false` | 冲突避免 event-executor 选择器。 |
| `celeborn.<module>.io.receiveBuffer` | `0` | `SO_RCVBUF`。 |
| `celeborn.<module>.io.sendBuffer` | `0` | `SO_SNDBUF`。 |
| `celeborn.<module>.io.maxRetries` | `3` | IO 异常重试。 |
| `celeborn.<module>.io.retryWait` | `5s` | IO 重试等待。 |
| `celeborn.<module>.io.lazyFD` | `true` | 延迟创建文件描述符。 |
| `celeborn.<module>.storage.memoryMapThreshold` | `2m` | 内存映射最小块大小。 |

### 9.4 Push/Fetch 超时检查

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.<module>.push.timeoutCheck.interval` | `5s` | push 超时检查间隔。 |
| `celeborn.<module>.push.timeoutCheck.threads` | `4` | push 超时检查线程。 |
| `celeborn.<module>.fetch.timeoutCheck.interval` | `5s` | fetch 超时检查间隔。 |
| `celeborn.<module>.fetch.timeoutCheck.threads` | `4` | fetch 超时检查线程。 |
| `celeborn.<module>.heartbeat.interval` | `60s` | worker↔client 心跳间隔。 |
| `celeborn.shuffle.io.maxChunksBeingTransferred` | — | 最大并发传输 chunk 数。 |
| `celeborn.io.maxDefaultNettyThreads` | `64` | 默认 Netty 线程上限。 |

### 9.5 内存分配器

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.network.memory.allocator.type` | `POOLED` | `POOLED`/`UNPOOLED`/`ADAPTIVE`。 |
| `celeborn.network.memory.allocator.share` | `true` | 跨 channel 共享 allocator。 |
| `celeborn.network.memory.allocator.allowCache` | `false` | 启用线程本地缓存。 |
| `celeborn.network.memory.allocator.numArenas` | max(#cores,2) | arena 数量。 |
| `celeborn.network.memory.allocator.verbose.metric` | `false` | 详细 allocator 指标。 |

---

## 10. SASL / SSL / Security

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.auth.enabled` | `false` | 启用 SASL 认证（需 internal port）。 |
| `celeborn.internal.port.enabled` | `false` | 创建免认证 internal port。 |
| `celeborn.rpc.RpcEndpointVerifier.separate.enabled` | `true` | 单独处理 verifier 请求。 |
| `celeborn.<module>.io.saslTimeout` | `30s` | SASL 单次往返超时。 |
| `celeborn.ssl.<module>.enabled` | `false` | 模块 SSL 开关。 |
| `celeborn.ssl.<module>.protocol` | `TLSv1.2` | TLS 协议。 |
| `celeborn.ssl.<module>.enabledAlgorithms` | — | 加密套件。 |
| `celeborn.ssl.<module>.keyStore` | — | 密钥库。 |
| `celeborn.ssl.<module>.keyStorePassword` | — | 密钥库密码。 |
| `celeborn.ssl.<module>.trustStore` | — | 信任库。 |
| `celeborn.ssl.<module>.trustStorePassword` | — | 信任库密码。 |
| `celeborn.ssl.<module>.trustStoreReloadingEnabled` | `false` | 信任库热加载。 |
| `celeborn.ssl.<module>.trustStoreReloadIntervalMs` | `10s` | 热加载间隔。 |
| `celeborn.ssl.<module>.sslHandshakeTimeoutMs` | `10s` | SSL 握手超时。 |
| `celeborn.ssl.<module>.maxEncryptedBlockSize` | `64k` | SSL 加密块大小。 |
| `celeborn.ssl.<module>.autoSslEnabled` | `false` | 自签名证书（仅 rpc_app）。 |
| `celeborn.serializer.deserialization.filter.enabled` | `true` | Java 反序列化过滤器。 |
| `celeborn.serializer.deserialization.filter.allowedPackages` | `java.,scala.,org.apache.celeborn.,...` | 反序列化包白名单。 |
| `celeborn.serializer.deserialization.filter.maxDepth` | `100` | 对象图深度。 |
| `celeborn.serializer.deserialization.filter.maxArrayLength` | `10000` | 数组长度限制。 |
| `celeborn.serializer.deserialization.filter.maxReferences` | `100000` | 引用数限制。 |
| `celeborn.serializer.deserialization.filter.maxStreamBytes` | `100000000` | 流字节限制。 |
| `celeborn.redaction.regex` | <code>(?i)secret&#124;password&#124;token&#124;access[.]key</code> | 日志脱敏正则。 |

### Kerberos (HDFS)

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.storage.hdfs.kerberos.principal` | — | HDFS Kerberos principal。 |
| `celeborn.storage.hdfs.kerberos.keytab` | — | HDFS Kerberos keytab。 |

---

## 11. HTTP Service / REST / Dynamic Config

### 11.1 Master / Worker HTTP

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.master.http.host` / `celeborn.worker.http.host` | `<localhost>` | HTTP host。 |
| `celeborn.master.http.port` / `celeborn.worker.http.port` | `9098` / `9096` | HTTP 端口。 |
| `celeborn.<role>.http.maxWorkerThreads` | `200` | Jetty worker 线程池。 |
| `celeborn.<role>.http.stopTimeout` | `5s` | 停止超时。 |
| `celeborn.<role>.http.idleTimeout` | `30s` | 连接空闲超时。 |
| `celeborn.<role>.http.ssl.enabled` | `false` | HTTPS 开关。 |
| `celeborn.<role>.http.ssl.keystore.path` | — | 密钥库路径。 |
| `celeborn.<role>.http.ssl.keystore.password` | — | 密钥库密码。 |
| `celeborn.<role>.http.ssl.keystore.type` | — | 密钥库类型。 |
| `celeborn.<role>.http.ssl.keystore.algorithm` | — | KeyManagerFactory 算法。 |
| `celeborn.<role>.http.ssl.disallowed.protocols` | `SSLv2, SSLv3` | 禁用协议。 |
| `celeborn.<role>.http.ssl.include.ciphersuites` | — | 允许加密套件。 |

### 11.2 HTTP 认证

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.<role>.http.auth.supportedSchemes` | `Nil` | `SPNEGO`/`BASIC`/`BEARER`。 |
| `celeborn.<role>.http.auth.basic.provider` | `AnonymousAuthenticationProviderImpl` | BASIC provider。 |
| `celeborn.<role>.http.auth.bearer.provider` | `AnonymousAuthenticationProviderImpl` | BEARER provider。 |
| `celeborn.<role>.http.auth.administers` | `[]` | 管理员列表。 |
| `celeborn.<role>.http.proxy.client.ip.header` | `X-Real-IP` | 反向代理客户端 IP 头。 |
| `celeborn.http.auth.bypass.api.paths` | `[]` | 额外免认证路径。 |

### 11.3 动态配置

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.dynamicConfig.store.backend` | — | `FS` 或 `DB`，未设置禁用。 |
| `celeborn.dynamicConfig.refresh.interval` | `120s` | 缓存刷新间隔。 |
| `celeborn.dynamicConfig.store.fs.path` | `${CELEBORN_CONF_DIR}/dynamicConfig.yaml` | FS 后端路径。 |
| `celeborn.dynamicConfig.store.db.fetch.pageSize` | `1000` | DB 分页大小。 |
| `celeborn.dynamicConfig.store.db.hikari.driverClassName` | — | JDBC 驱动。 |
| `celeborn.dynamicConfig.store.db.hikari.jdbcUrl` | — | JDBC URL。 |
| `celeborn.dynamicConfig.store.db.hikari.username` | — | DB 用户名。 |
| `celeborn.dynamicConfig.store.db.hikari.password` | — | DB 密码。 |
| `celeborn.dynamicConfig.store.db.hikari.connectionTimeout` | `30s` | 连接超时。 |
| `celeborn.dynamicConfig.store.db.hikari.idleTimeout` | `600s` | 空闲超时。 |
| `celeborn.dynamicConfig.store.db.hikari.maxLifetime` | `1800s` | 最大生命周期。 |
| `celeborn.dynamicConfig.store.db.hikari.maximumPoolSize` | — | 最大连接池大小。 |

---

## 12. Metrics / Monitoring

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.metrics.enabled` | `true` | 启用指标系统。 |
| `celeborn.metrics.conf` | classpath metrics.properties | 自定义 metrics 配置文件。 |
| `celeborn.metrics.sample.rate` | `1.0` | timer 采样率。 |
| `celeborn.metrics.timer.slidingWindow.size` | `4096` | timer 滑动窗口。 |
| `celeborn.metrics.capacity` | `4096` | 每 source 最大指标数。 |
| `celeborn.metrics.collectPerfCritical.enabled` | `false` | 收集可能影响性能的指标。 |
| `celeborn.metrics.extraLabels` | — | 额外静态标签。 |
| `celeborn.metrics.prometheus.path` | `/metrics/prometheus` | Prometheus 路径。 |
| `celeborn.metrics.json.path` | `/metrics/json` | JSON 指标路径。 |
| `celeborn.metrics.json.pretty.enabled` | `true` | JSON 美化输出。 |
| `celeborn.metrics.worker.appLevel.enabled` | `true` | worker 应用级指标（高基数）。 |
| `celeborn.metrics.worker.app.topResourceConsumption.count` | `0` | 资源消耗 Top-N 应用。 |
| `celeborn.metrics.worker.pauseSpentTime.forceAppend.threshold` | `10` | 强制追加 pause 时间指标阈值。 |
| `celeborn.metrics.loggerSink.scrape.interval` | `30min` | logger sink 自 scrape 间隔。 |
| `celeborn.metrics.loggerSink.output.enabled` | `false` | 输出指标到 logger。 |

---

## 13. Memory / Off-Heap / Buffer

### 13.1 Worker 内存管理

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.worker.directMemoryRatioToPauseReceive` | `0.85` | 暂停接收 push 的堆外内存比例。 |
| `celeborn.worker.directMemoryRatioToPauseReplicate` | `0.95` | 暂停 replicate 的堆外内存比例。 |
| `celeborn.worker.directMemoryRatioToResume` | `0.7` | 恢复服务的堆外内存比例。 |
| `celeborn.worker.pinnedMemoryRatioToResume` | `0.3` | 恢复所需的 pinned memory 比例。 |
| `celeborn.worker.monitor.memory.check.interval` | `10ms` | 堆外内存检查间隔。 |
| `celeborn.worker.monitor.pinnedMemory.check.enabled` | `true` | 恢复时考虑 pinned memory。 |
| `celeborn.worker.monitor.pinnedMemory.check.interval` | `10s` | pinned memory 检查间隔。 |
| `celeborn.worker.monitor.pinnedMemory.resumeKeepTime` | `1s` | 恢复后保持时间。 |
| `celeborn.worker.monitor.memory.report.interval` | `10s` | 堆外内存追踪日志间隔。 |
| `celeborn.worker.monitor.memory.trimChannelWaitInterval` | `1s` | 触发 channel 缓存清理后等待。 |
| `celeborn.worker.monitor.memory.trimFlushWaitInterval` | `1s` | 触发 flush 后等待。 |

### 13.2 读 Buffer 与内存文件

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.worker.directMemoryRatioForReadBuffer` | `0.35` | 读 buffer 最大堆外比例。 |
| `celeborn.worker.directMemoryRatioForMemoryFileStorage` | `0.0` | 内存层文件堆外比例（实验）。 |
| `celeborn.worker.memoryFileStorage.maxFileSize` | `8MB` | 内存层文件最大大小（<2GB）。 |
| `celeborn.worker.memoryFileStorage.evict.aggressiveMode.enabled` | `false` | PAUSED 时积极驱逐内存文件。 |
| `celeborn.worker.memoryFileStorage.evict.ratio` | `0.5` | 内存文件驱逐比例阈值。 |
| `celeborn.worker.partition.initial.readBuffersMin` | `1` | 每分区最小初始读 buffer。 |
| `celeborn.worker.partition.initial.readBuffersMax` | `1024` | 每分区最大初始读 buffer。 |
| `celeborn.worker.bufferStream.threadsPerMountpoint` | `8` | 每挂载点读 buffer 线程。 |
| `celeborn.worker.readBuffer.allocationWait` | `50ms` | buffer dispatcher 无法分配时等待。 |
| `celeborn.worker.readBuffer.processTimeout` | `600s` | 读 buffer 请求处理超时。 |
| `celeborn.worker.readBuffer.target.ratio` | `0.9` | 预读目标内存比例。 |
| `celeborn.worker.readBuffer.target.updateInterval` | `100ms` | 预读目标重算间隔。 |
| `celeborn.worker.readBuffer.toTriggerReadMin` | `32` | 触发 map-data 读的最小 buffer 数。 |

### 13.3 分区排序与 Merge Buffer

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.worker.partitionSorter.directMemoryRatioThreshold` | `0.1` | 分区排序最大堆外比例。 |
| `celeborn.worker.pushdata.mergeBuffer.enabled` | `false` | 合并低利用率 push buffer。 |
| `celeborn.worker.directMemoryRatioToMergeBuffer` | `0.4` | 触发 merge buffer 的堆外比例。 |

---

## 14. Storage (HDFS / OSS / S3)

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.storage.hdfs.dir` | — | HDFS shuffle 数据根目录。 |
| `celeborn.storage.s3.dir` | — | S3 shuffle 数据根目录。 |
| `celeborn.storage.s3.endpoint.region` | — | S3 endpoint region。 |
| `celeborn.storage.s3.mpu.maxRetries` | `5` | S3 分段上传重试。 |
| `celeborn.storage.s3.mpu.baseDelay` | `100ms` | S3 重试基础延迟。 |
| `celeborn.storage.s3.mpu.maxBackoff` | `20s` | S3 重试最大退避。 |
| `celeborn.storage.oss.dir` | — | OSS shuffle 数据根目录。 |
| `celeborn.storage.oss.endpoint` | — | OSS endpoint。 |
| `celeborn.storage.oss.access.key` | — | OSS access key。 |
| `celeborn.storage.oss.secret.key` | — | OSS secret key。 |
| `celeborn.storage.oss.ignore.credentials` | `true` | 忽略 OSS 凭证（支持 Jindo SDK）。 |
| `celeborn.master.dfs.expireDirs.timeout` | `1h` | DFS 过期目录清理超时。 |
| `celeborn.worker.reuse.hdfs.outputStream.enabled` | `false` | 复用 HDFS 输出流。 |
| `celeborn.worker.writer.hdfs.createAuxiliaryFile.maxRetries` | `5` | HDFS 索引/成功文件创建重试。 |

---

## 15. Quota / ACL / Identity

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.quota.enabled` | `true` | 启用配额检查。 |
| `celeborn.quota.cluster.enabled` | `true` | 集群级配额。 |
| `celeborn.quota.tenant.enabled` | `true` | 租户级配额。 |
| `celeborn.quota.user.enabled` | `true` | 用户级配额。 |
| `celeborn.quota.interruptShuffle.enabled` | `false` | 配额超限时中断 shuffle。 |
| `celeborn.identity.provider` | `DefaultIdentityProvider` | 身份提供类。 |
| `celeborn.identity.user-specific.tenant` | `default_tenant` | 静态租户 ID。 |
| `celeborn.identity.user-specific.userName` | `default_username` | 静态用户名。 |
| `celeborn.client.application.info.provider` | `DefaultApplicationInfoProvider` | 应用信息提供类。 |
| `celeborn.container.info.provider` | `DefaultContainerInfoProvider` | 容器信息提供类。 |
| `celeborn.quota.cluster.diskBytesWritten` | `Long.MaxValue` | 集群磁盘写入配额。 |
| `celeborn.quota.tenant.diskBytesWritten` | `Long.MaxValue` | 租户磁盘写入配额。 |
| `celeborn.quota.user.diskBytesWritten` | `Long.MaxValue` | 用户磁盘写入配额。 |
| `celeborn.quota.cluster.hdfsBytesWritten` | `Long.MaxValue` | 集群 HDFS 写入配额。 |
| `celeborn.quota.tenant.hdfsBytesWritten` | `Long.MaxValue` | 租户 HDFS 写入配额。 |
| `celeborn.quota.user.hdfsBytesWritten` | `Long.MaxValue` | 用户 HDFS 写入配额。 |

---

## 16. Congestion Control

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.worker.congestionControl.enabled` | `false` | 启用 worker 拥塞控制。 |
| `celeborn.worker.congestionControl.sample.time.window` | `10s` | 生产/消费速率统计窗口。 |
| `celeborn.worker.congestionControl.diskBuffer.low.watermark` | `Long.MaxValue` | 低于此 pending disk buffer 停止拥塞。 |
| `celeborn.worker.congestionControl.diskBuffer.high.watermark` | `Long.MaxValue` | 高于此 pending disk buffer 开始拥塞。 |
| `celeborn.worker.congestionControl.userProduceSpeed.low.watermark` | `Long.MaxValue` | 低于此用户生产速度停止拥塞。 |
| `celeborn.worker.congestionControl.userProduceSpeed.high.watermark` | `Long.MaxValue` | 高于此用户生产速度开始拥塞。 |
| `celeborn.worker.congestionControl.workerProduceSpeed.low.watermark` | `Long.MaxValue` | 低于此 worker 生产速度停止拥塞。 |
| `celeborn.worker.congestionControl.workerProduceSpeed.high.watermark` | `Long.MaxValue` | 高于此 worker 生产速度开始拥塞。 |
| `celeborn.worker.congestionControl.user.inactive.interval` | `10min` | 用户速率跟踪不活跃阈值。 |
| `celeborn.worker.congestionControl.check.interval` | `10ms` | 拥塞检查间隔。 |

---

## 17. Compression & Columnar Shuffle

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.client.shuffle.compression.codec` | `LZ4` | 压缩算法：`LZ4`/`ZSTD`/`NONE`。 |
| `celeborn.client.shuffle.compression.zstd.level` | `1` | Zstd 压缩级别（`-5` ~ `22`）。 |
| `celeborn.client.shuffle.decompression.lz4.xxhash.instance` | — | Lz4 xxhash 实例：`JNI`/`JAVASAFE`/`JAVAUNSAFE`。 |
| `celeborn.columnarShuffle.enabled` | `false` | 启用列式 shuffle。 |
| `celeborn.columnarShuffle.batch.size` | `10000` | 向量批次大小。 |
| `celeborn.columnarShuffle.offHeap.enabled` | `false` | 列式向量使用堆外。 |
| `celeborn.columnarShuffle.encoding.dictionary.enabled` | `false` | 字典编码。 |
| `celeborn.columnarShuffle.encoding.dictionary.maxFactor` | `0.3` | 字典最大因子。 |
| `celeborn.columnarShuffle.codegen.enabled` | `false` | 列式 shuffle codegen。 |

---

## 18. Misc / Internal

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.logConf.enabled` | `false` | 启动时打印生效配置。 |
| `celeborn.test.client.fetchFailure` | `false` | 测试注入 fetch 失败。 |
| `celeborn.test.client.mockDestroySlotsFailure` | `false` | 测试注入 destroy slots 失败。 |
| `celeborn.test.mockCommitFilesFailure` | `false` | 测试注入 commit files 失败。 |
| `celeborn.test.client.mockShuffleLost` | `false` | 测试模拟 shuffle 丢失。 |
| `celeborn.test.client.retryRevive` | `false` | 测试强制 push/revive 重试。 |

---

## 19. 配置前缀速查

| 前缀 | 含义 |
|------|------|
| `celeborn.master.*` | Master 服务端 |
| `celeborn.worker.*` | Worker 服务端 |
| `celeborn.client.*` | 通用客户端（Spark/Flink/MR/Tez 共用） |
| `celeborn.client.spark.*` | Spark 客户端 |
| `celeborn.client.flink.*` | Flink 客户端 |
| `celeborn.client.mr.*` | MapReduce 客户端 |
| `celeborn.rpc.*` | RPC 框架 |
| `celeborn.network.*` | 通用网络 |
| `celeborn.<module>.io.*` | 传输模块网络参数 |
| `celeborn.ssl.*` | SSL/TLS |
| `celeborn.auth.*` / `celeborn.internal.*` | SASL 认证与 internal port |
| `celeborn.dynamicConfig.*` | 动态配置 |
| `celeborn.metrics.*` | 指标监控 |
| `celeborn.storage.*` | 远程存储（HDFS/S3/OSS） |
| `celeborn.quota.*` / `celeborn.identity.*` | 配额与身份 |
| `celeborn.columnarShuffle.*` | 列式 shuffle |

---

## 20. 延伸阅读

- [client-control-plane-deep-dive.md](./client-control-plane-deep-dive.md) — 客户端控制平面与配置实际生效路径。
- [engine-clients-deep-dive.md](./engine-clients-deep-dive.md) — 各引擎客户端配置（§6–8）的使用场景。
- [network-and-protocol-deep-dive.md](./network-and-protocol-deep-dive.md) — `celeborn.rpc.*` 与 `celeborn.<module>.io.*` 在 Netty 中的使用。
- [cli-and-rest-api-analysis.md](./cli-and-rest-api-analysis.md) — REST API 与动态配置相关运维入口。
- [service-and-security-deep-dive.md](./service-and-security-deep-dive.md) — 动态配置服务、HTTP 配置与 `celeborn.auth.*` / `celeborn.ssl.*` 安全机制。
- 源码：`common/src/main/scala/org/apache/celeborn/common/CelebornConf.scala`
