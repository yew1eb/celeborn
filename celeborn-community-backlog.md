# Celeborn 社区贡献机会：模块篇全量清单

> 基线：逐篇沿用来源文档基线（`ai-1` 0.7.0-SNAPSHOT｜上游 `main@626a07cfc`｜生产 0.6.2.x｜`parallel-partition-write` 分支），**行号不跨基线比较**
> 整理：2026-10-31 ｜ 来源：根目录 21 篇调研 / 设计文档 ｜ 与 [celeborn-community-and-ci.md](celeborn-community-and-ci.md) 互补，不重复其 T / M / A / P / C 条目
> 证据：✅ 已回读源码 ｜ ⚠️ 来源结论，动工前需按基线复核 ｜ 🧪 生产实证（0.6.2.x，非当前 HEAD）
> 状态：**待提**（可直接开 JIRA）｜**需讨论**（dev@ / CIP 先行）｜**已有 JIRA**｜**已合入**｜**仅内部**；⭐ = 建议首批

---

## 1. 使用说明与边界

### 1.1 收录与排除

**收录**：代码 bug / 正确性与资源泄漏、有明确收益的性能与可维护性改进、指标与配置缺口、可上游化的设计提案。

**排除**（只在 §14.2 / §14.3 登记，不进正文）：
- 生产部署调参（JVM 堆/堆外配比、scrape 间隔、leakDetection 级别等）；
- 得物内部 fork 专属逻辑（SKYNET 身份、App 级拥塞控制实现）；
- 来源文档已明确“核查后剔除”的候选（§14.2）；
- 纯业务治理建议与已 won't fix 的项。

### 1.2 ID、体量与状态

- ID 前缀：`MA` Master 控制面、`HA` Master HA、`WK` Worker 存储/流控、`WM` Worker 内存/生产、`LM` Client 控制面、`SC` Client 数据面、`NW` 网络、`SV` Service/安全、`CLI` CLI、`EN` 引擎客户端、`SP` Spark 指标、`CF` 配置/文档/架构、`DS` 设计提案。与既有 `T/M/A/P/C` 不冲突；ID 分配后不复用。
- 体量：`0.1d` 一行级 / `0.5d` 半天含单测 / `1-2d` / `3-5d` / **设计级**（需 CIP 或跨模块改造）。
- 本文**不定义 P0/P1/P2**：优先级只在 [performance-optimization-analysis.md](performance-optimization-analysis.md) §11 定义，这里只用“首批 / 体量 / 状态”做提交排序。

### 1.3 与 celeborn-community-and-ci.md 的分工

| 既有条目 | 主题 | 本文处理 |
|---|---|---|
| T1~T6 | 代码 TODO（ThreadUtils、TagsManager、SASL init、SPNEGO、Commit 装箱） | 不重复；`TagsManager` 只在 MA 章引用 T2 |
| M1~M20 | Celeborn Metrics / Grafana 面板 | 不重复；SP 章只写 **Spark 侧**指标，层级不同 |
| A1~A9 | Auth / SASL 核心（checkAuth、replicate SASL、secret 生命周期） | 不重复；SV 章对 A6（secret 明文）、D1（机制可配置）只引用 |
| P1~P11 | CI 日志问题台账 | 不重复 |
| C1~C5 | CI 优化提案 | 不重复 |

两文按 `file:line + 问题本质` 去重；同一问题只有一处展开。

### 1.4 基线差异（看行号前必读）

- `master-control` / `master-ha` / `worker-storage` / `client-*` / `network` / `service` 的 §可优化点章节已于 **2026-09 对照 0.7.0-SNAPSHOT 逐项核查**，优先级最高。
- `performance-optimization-analysis.md` 成文于 **2026-08**，其中被上述 deep-dive 否掉的条目**不再复活**（见 §14.2）。
- `production-performance-analysis.md`、`worker-memory-and-optimization.md` 数据来自 **0.6.2.x 生产**，标 🧪；类名/行号已对照 HEAD，但结论需在生产复测。
- `celeborn-shuffle-pipeline.md` 第四部分基于 `parallel-partition-write` 分支，**本工作树没有**其自定义 Write/Read Times 页面，SP 章相关条目多为分支专属。
- `dynamic-partition-write-parallelism-design.md` 的分支实现**不存在于本工作树**（`adaptivePartitionWriteParallelism` 全仓无匹配）。

---

## 2. 总览与首批

### 2.1 方向总览

| 章节 | 条目 | 章内首推 | 主要来源 |
|---|---|---|---|
| §3 Master 控制面 | 10 | MA-03 | master-control-plane §7 |
| §4 Master HA | 13 | HA-03 | master-ha-ratis §7；production 第五章 |
| §5 Worker 存储/流控 | 14 | WK-11 | worker-storage §8 |
| §5 Worker 内存/生产 | 17 | WM-09 | worker-memory；production；cluster-degradation 附录 C |
| §6 Client 控制面 | 7 | LM-02 | client-control-plane §6 |
| §6 Client 数据面 | 14 | SC-07 | client-data-plane §8 |
| §7 网络与协议 | 7 | NW-01 | network-and-protocol §6 |
| §8 Service / REST / 安全 | 16 | SV-05 | service-and-security §8 |
| §9 CLI | 28 | CLI-02 | cli-and-rest-api §5/§6 |
| §10 引擎客户端 | 18 | EN-13 | engine-clients §6 |
| §11 Spark 指标 | 12 | — | shuffle-pipeline §4.6/4.7（多为分支专属） |
| §12 配置 / 文档 / 架构债 | 4 | — | ARCHITECTURE §6、performance §10 |
| §13 设计提案 / CIP | 6 | — | 6 篇设计 / 提案文档 |
| **合计** | **166** | **8** | 去重后正文条目 |

> 状态分布：待提 78（含 8 条首批）/ 需讨论 72 / 仅内部 14 / 已有 JIRA 1 / 已合入 1。“需讨论”不代表不可提，而是需要先发 dev@ 或 JIRA 对齐语义；仅内部主要是 SP 分支指标与 DS-03/04。
>
> 核对提示：来源文档有 3 处已过时——NW-03 已由 CELEBORN-2455 修复、MA-03 内层 Set 已由 CELEBORN-2392 改为 `newKeySet`、WK-06 的 `release(refCnt)` 为有意语义；均已在 §14.1 登记，不再作为待办。

### 2.2 建议首批（8 条，均非既有文档重复）

| ID | 一句话 | 为什么先做 |
|---|---|---|
| **MA-03** | `updateUnregisterShuffleMeta` 的 get→remove→compute 判空并原子化（内层 Set 已由 CELEBORN-2392 修复） | 改动小、并发正确性明确、易写单测 |
| **HA-03** | 快照 `renameTo` 改 `Files.move(ATOMIC_MOVE)`，md5 改到成功后写 | 现网“每次快照静默失败”级 bug，定位清晰 |
| **WM-09** | per-user gauge 随快照消失移除；`WorkerInfo` 删 key 而非置 0 | ~15 行、指标基数从“历史累计”收敛到“活跃” |
| **SV-05** | 动态配置两个 REST 接口输出前过 `Utils.redact` | 安全小修，与静态配置行为对齐 |
| **CLI-02** | `DECOMMISSION_THEN_IDLE` 枚举名不一致，命令必现失败 | 用户可见的确定性 bug |
| **CLI-06** | `--version` 缺 `s` 插值，`$CELEBORN_HOME` 原样输出 | 一行修复 |
| **SC-07** | 空 poll 也 `incReadTime`，读耗时被超时等待污染；三处等待语义不一致 | 小修、指标口径明确、易验证 |
| **WM-12** | Flusher timer key 固定、`metricNameWithCustomizedLabels` 缓存 | 单行级，消 Random CAS 与每 push 字符串分配 |

> 与既有文档首批（T1 → M1~M3 → A2→A1 → C2）合并后的顺序见 §15。

### 2.3 PR 打包原则

1. 同一 `file` 且无行为耦合的条目可合一个 PR（如 CLI-02/05/06/09 打包为“CLI 参数与提示修复”）。
2. 涉及配置默认值/新增配置的必须带 `ConfigurationSuite` 文档重生成（`UPDATE=1`）。
3. 涉及协议 / Raft / 存储语义的（HA-01/HA-04/HA-05、WK-01、DS-*）先发 JIRA 或 dev@，不直接提 PR。
4. 每个 PR：`CELEBORN-XXXX` Jira → `./dev/reformat` → 相关模块单测 → 必要时 `tests/spark-it`。

---

## 3. Master 控制面（MA）

> 来源：`master-control-plane-deep-dive.md` §7（2026-09 逐项核查）。Raft 写路径类问题归 §4，不在此重复。

| ID | 条目（位置 + 问题） | 建议改法 | 体量 | 状态 | 来源 |
|---|---|---|---|---|---|
| `MA-01` ✅ | `Master.scala:975-1000` RequestSlots 分配持 `workersMap` 全局锁；同一 monitor 上心跳要取 2~3 次锁，LOADAWARE 锁内 O(D log D) | 分配改为对不可变快照的无锁计算，锁内只做校验与结果提交；心跳取锁合并 | 3-5d | 需讨论（先解 MA-05） | §7.1 |
| `MA-02` ✅ | `Master.scala:661-682` WorkerLost 判重不覆盖 `self.send` in-flight 窗口；单轮无限速、逐条 raft 提交 | 一条消息携带 worker 列表批量处理；判重提前到发送侧并延迟清除 | 1-2d | 待提 | §7.2 |
| `MA-03` ✅ | `AbstractMetaManager.java:72,149-170` 内层 Set 已由 CELEBORN-2392 改为 `newKeySet`；残留：`updateUnregisterShuffleMeta` 的 get→remove→compute 未判空，并发删 key 时 `shuffles.size()` NPE；batch 版 containsKey+get+remove 非原子 | 单个 `compute` 内完成 remove + 空则删 key（先判空）；batch 同理 | 0.5d | 待提 ⭐ | §7.3 |
| `MA-04` ✅ | `AbstractMetaManager.java:92,606-637` `estimatedPartitionSize` 为 plain `long`，无锁写、多线程读 | 改 `volatile long` 或 `AtomicLong` | 0.1d | 待提 | §7.4 |
| `MA-05` ✅ | `SlotsAllocator.java:59-60,176-178` `taskAllocationRatio` 静态字段首次初始化后永不更新；check-then-act 无线程安全，隐式依赖全局锁 | 改为按 `(diskGroupCount, gradient)` 参数化的实例缓存，去掉 static | 1-2d | 需讨论（MA-01 的前置） | §7.5 |
| `MA-06` ✅ | `Master.scala:1297-1306` 每次 RequestSlots 全量拷贝 `availableWorkers`；有排除集时两次拷贝 | 维护惰性失效快照；排除集改“随机跳过”而非整体拷贝 | 1-2d | 待提 | §7.6 |
| `MA-07` ✅ | `Master.scala:1220-1262` 应用心跳回包全量下发 excluded/shutdown 集合与 registeredShuffles，按 app 数 × 10s 放大 | 版本号增量下发：集合未变只回版本号 | 1-2d | 待提 | §7.9 |
| `MA-08` ✅ | `SlotsAllocator.java:137-185,672` LOADAWARE 每请求重建 usableDisks/diskToWorkerMap、全量排序；4 轮兜底重复构造 rack 列表与 HashMap | 按“心跳代次/磁盘指纹”缓存 disk groups；预聚合每 worker 剩余配额 | 3-5d | 需讨论 | §7.8 |
| `MA-09` ✅ | `Master.scala:975` + `AbstractMetaManager.java:296,606-637` DiskInfo 三方锁协议不一致（workersMap / WorkerInfo.this / 无锁），可能读撕裂槽位 | 统一经 `WorkerInfo` 同步访问器读快照，或 `updatePartitionSize` 进 workersMap 锁 | 3-5d | 需讨论 | §7.10 |
| `MA-10` ✅ | `Master.scala` 1639 行：`receiveAndReply` 198 行/18 分支，`handleRequestSlots` 134 行，`nonEagerHandler` 线程池 64 硬编码 | 按消息类别拆独立 handler；线程池大小提为配置 | 3-5d | 需讨论 | §7.11 |

> `TagsManager.getTagStore` 每次请求按 tag 数全量拷贝的问题见既有文档 **T2**，此处不重复。  
> 已剔除：`generateRackAwareWorkers` O(W²)、QuotaManager 全量聚合、tags 过滤在锁内等 7 项，见 §14.2。

---

## 4. Master HA / Ratis（HA）

> 来源：`master-ha-ratis-deep-dive.md` §7（2026-09 核查）+ `production-performance-analysis.md` 第五章（🧪）。CIP-23（CELEBORN-2429）已合入，心跳“批量化”不再是待办，但“状态变更才复制 / 统计字段本地化”仍未做。

| ID | 条目（位置 + 问题） | 建议改法 | 体量 | 状态 | 来源 |
|---|---|---|---|---|---|
| `HA-01` ✅ | `HAMasterMetaManager.java:160-192,303-339` 心跳无条件写 Raft；磁盘全量、统计累加器也上链。批量已由 CELEBORN-2429 完成 | 磁盘/状态变化才复制完整心跳，否则只更新 Leader `lastHeartbeat` 并定期 checkpoint；统计量 Leader 本地聚合 | 3-5d | 需讨论 | §7.1 |
| `HA-02` ✅ | `RatisResource.scala` 无管理员校验，`celeborn.master.http.auth.supportedSchemes` 默认 `Nil`；`/ratis/peer/remove`、`/election/pause` 对网络可达者开放 | 运维端点加管理员角色校验；pause/resume 要求目标为 Leader；失败返回非 200 | 1-2d | 需讨论 | §7.2 |
| `HA-03` ✅ | `StateMachine.java:266-295` 用 `File.renameTo`（非原子，跨挂载点必失败）；`saveMD5File` 在 rename 之前，失败留孤儿 md5 | `Files.move(ATOMIC_MOVE)`；md5 在 rename 成功后写；失败重试 + metric | 1d | 待提 ⭐ | §7.3 |
| `HA-04` ✅ | `HARaftServer.java:223-262` 每个写请求在 RPC 线程同步 `.get()` 等多数派提交，无队列/聚合 | 提交异步化 + 聚合；心跳类与 RequestSlots 分离 | 3-5d | 需讨论 | §7.4 |
| `HA-05` ✅ | `StateMachine.java:95-96,189-190` apply 单线程串行，心跳 burst 与 RequestSlots 互相排队（保序是刻意设计） | 按 worker host 分区 striped executor，单 worker 保序 | 3-5d | 需讨论 | §7.5 |
| `HA-06` ✅ | `HARaftServer.java:513-523,542-588` Follower 靠请求驱动惰性刷新角色，`isLeader()` 同步 `getGroupInfo()` 阻塞 RPC 线程 | Follower 也定时刷新角色缓存；`isLeader` 命中缓存，刷新异步 | 1-2d | 待提 | §7.6 |
| `HA-07` ✅ | `AbstractMetaManager.java:430-535` 快照加载不校验 md5、无回退；解析中途异常后以半空状态继续运行 | 校验 md5、失败回退上一快照并告警；`cleanUpState` 推迟到解析成功后 | 1-2d | 待提 | §7.7 |
| `HA-08` ✅ | `AbstractMetaManager.java:397-422` 快照先 `toByteArray()` 再 `Files.write`，双份内存、无压缩、随规模线性膨胀 | `writeTo(OutputStream)` 流式写出 + 压缩 | 1-2d | 待提 | §7.8 |
| `HA-09` ✅ | `CelebornConf.scala:2796-2805` 优雅关闭默认 `false`，计划内重启要等 3~5s 选举 | 部署侧先开；长期评估改默认值 | 0.1d | 需讨论（改默认） | §7.10 |
| `HA-10` ✅ | `Resource.proto:57-78` 与 `TransportMessages.proto:1003-1022` 双 schema 逐项重复，新增操作两边同步 | 逐步统一到 `TransportMessages.proto`，删 `Resource.proto` 重复定义 | 3-5d | 需讨论 | §7.11 |
| `HA-11` ✅ | `StateMachine.java:228-237` `query` 覆写为死代码；所有读走 Leader 内存，未用 follower read | 删死代码；非关键诊断接口可评估 `readOnlyRequestType` stale read | 1-2d | 需讨论 | §7.12 |
| `HA-12` ✅ | `MasterClient.java:83-89` `decodeRequestId` 二次 `split("#")`、非法输入抛原始异常；`MetaHandler` 包 IOException 丢 cause | 一次 `indexOf/substring`；统一 `CelebornRuntimeException`；异常包装保留 cause | 0.5d | 待提 | §7.13 |
| `HA-13` 🧪 | `CelebornConf.scala:2977` purgeGap 默认 100 万条、`:3125` snapshot 阈值 20 万条偏大，Ratis closedSegments/LogEntry 堆积（生产 39MB/13 万条） | 调小阈值/purgeGap，压测找平衡；或改默认值并说明 | 0.5d + 压测 | 需讨论 | production 第五章 P0-1 |

> 已剔除：客户端 Leader hint 缓存（已存在），见 §14.2。

---

## 5. Worker

> 5.1 来源 `worker-storage-and-traffic-deep-dive.md` §8（2026-09 核查）；5.2 来源 `worker-memory-and-optimization.md`（🧪 生产 0.6.2.2）与 `production-performance-analysis.md`（🧪）。两节按 `file:line` 去重：**bufferQueue 无界**归 WK-11，**Flusher key / metricName** 归 WM-12，其余各表只出现一次。

### 5.1 存储与流控（WK）

| ID | 条目（位置 + 问题） | 建议改法 | 体量 | 状态 | 来源 |
|---|---|---|---|---|---|
| `WK-01` ✅ | `PushDataHandler.scala:304,399,640,796,1061` 应答路径在 Netty IO 线程 `Await.result(writePromise.future, Inf)`，副本慢时 EventLoop 被整条连接阻塞 | 改真异步：写完回调回 ACK（保 partition 内应答顺序）；或评估“本地写完即应答、副本失败走 HARD_SPLIT” | 3-5d | 需讨论 | §8.1 |
| `WK-02` ✅ | `StorageManager.scala:952-993,1002-1024` 心跳 `updateDiskInfos` 持 `this` + 逐目录 `writers` 锁求和；`userResourceConsumptionSnapshot` 在锁内 flatMap/groupBy 全量聚合 | writer 写入/关闭时原子累加每盘用量；快照浅拷贝后锁外聚合 | 1-2d | 待提 | §8.2 |
| `WK-03` ✅ | `PartitionDataWriter.java:121,144,197,214,218` write/evict/destroy/close 共用 `synchronized(this)`，evict 与 Netty 写线程互斥 | tier 切换用 `AtomicReference` + CAS，普通 write 只在切换瞬间独占 | 1-2d | 需讨论 | §8.3 |
| `WK-04` ✅ | `MemoryManager.java:257-264` 内存驱逐每次把全部 memory writer 拷进 ArrayList 再全量排序取头部，数万 writer 时排序制造 GC | 改 Top-K（PriorityQueue）或维护按大小有序索引 | 1d | 待提 | §8.4 |
| `WK-05` ✅ | `PartitionFilesSorter.java:272-275` Reducer 等排序完成用 50ms sleep 轮询两个 Set，平均多付 ~25ms | 每 fileId 一个 `CompletableFuture`，完成即唤醒 | 1d | 待提 | §8.5 |
| `WK-07` ✅ | `CelebornConf.scala:3941-3946` 本地盘 flush buffer 默认 256KiB，HDFS 默认 4MiB，本地每 MB 约 4 次 flush | 纯配置：SSD/NVMe 实测后提到 1~4MiB；或改默认并说明内存代价 | 0.5d + 压测 | 需讨论 | §8.7 |
| `WK-08` ✅ | `MemoryManager.java:58,122-123,355,383-386,421` `memoryPressureListeners` 增删持锁、遍历不持锁，理论 CME | 换 `CopyOnWriteArrayList` | 0.1d | 待提 | §8.8 |
| `WK-09` ✅ | `TierWriter.scala:63,77,224,253,300,454,676` write/take/return/flush/evict 共用一把 `flushLock`，单 writer 写路径串行点 | 拆 buffer 管理与 flush 提交两把锁，或读路径乐观锁 | 3-5d | 需讨论 | §8.9 |
| `WK-10` ✅ | `CongestionController.java:158-160,217-222` `avgConsumeSpeed` 实际取 `getPotentialProduceSpeed()`，命名/注释与语义不符；均分假设忽略用户消费能力差异 | 先修命名/文档；再评估按用户历史生产/消费比限速 | 1-2d | 需讨论 | §8.10 |
| `WK-11` ✅🧪 | `Flusher.scala:50,123-136` `bufferQueue` 无界且只增不减；`TierWriter.scala:429` 本地 flush 恒 `keepBuffer=true`。生产单 flusher 池 1.23GB 堆壳 | 队列设容量（如 `threadCount × 2~32`），超出 `release`；高水位时新 task 用 `keepBuffer=false` | 1d | 待提 | worker-storage §8.11；worker-memory F1；production 四章 P0-1 |
| `WK-12` ✅ | `PushDataHandler.scala:205-273` 单字节状态码每次新分配 `Array[Byte]+ByteBuffer`（10+ 处）；`:281,613` 每条 push `new WorkerInfo`；`:1106,1150-1152` 用 try-catch 区分新旧协议 | 状态码静态单例；直接用 `(host, port)`；消息前导字节显式区分版本 | 1d | 待提 | §8.12 |
| `WK-13` ✅ | `PartitionMetaHandler.scala:236-238` 每个 RegionFinish 同步 `indexChannel.write`，HDFS 场景高频小 IO | 聚合多个 region 的索引批量写，close 时兜底 | 1-2d | 待提 | §8.14 |
| `WK-14` ✅ | `Worker.scala:332-349` 3 个单线程 ScheduledExecutor + 1 个 HashedWheelTimer；`:745-758` TopN 用全量 `sortBy` | 合并共享调度池；TopN 改堆 | 1d | 待提 | §8.15 |
| `WK-15` ✅ | `StorageManager.scala:273-281,397` `getNextIndex` 在健康目录间纯轮询，忽略已维护的 `flushTimeMetrics/fetchTimeMetrics` | 加权轮询，权重取 flush/fetch 耗时倒数 | 1-2d | 需讨论 | §8.16 |

> `WK-06` 原条目（`ReadBufferDispatcher.release(refCnt)`）经核对为**有意语义**：当前代码注释明确“reader 失败时 read buffer 会有多个引用”，一次性释放全部引用是正确做法，不建议盲改；编号不回收，见 §14.1。  
> 已剔除：内存检查间隔 100ms、DCL 锁序死锁、flush 阈值迟滞、S3 6MB 分片等 12 项，见 §14.2。`Worker.scala:945-962` 的 `PushDataHandler` 重复代码（原 3.11 路线图）未列入，属重构而非可验证收益。

### 5.2 内存 / 生产实证（WM）

> 🧪 条目来自 0.6.2.2 生产 dump / 火焰图；类名已对 HEAD 核对，行号以来源文档为准，动工前建议在生产或 HEAD 复读一次。

| ID | 条目（位置 + 问题） | 建议改法 | 体量 | 状态 | 来源 |
|---|---|---|---|---|---|
| `WM-01` ✅🧪 | `ReadBufferDispatcher.java:172` 用 `PooledByteBufAllocator.buffer()` 分配 **堆内** buffer，却从 direct 读 buffer 配额（0.35×maxDirect）记账 → 堆外水位失真、堆内多一个潜在消耗者 | `buffer(...)` → `directBuffer(bufferSize, bufferSize)`（一行），灰度看 NettyMemory/fetch 延迟 | 0.5d | 待提 | worker-memory 二部分 §2 |
| `WM-02` ✅ | `MemoryManager.java:309-330` resume 条件只看总 memoryUsage；当 `directMemoryRatioToResume(0.2) < readBufferRatio(0.35)` 时读 buffer 驻留即可让 worker 永不 resume，现有校验只在 memoryFileStorage>0 时生效 | 启动校验 `resume > readBufferRatio + memFileStorage`；或文档强制约束 | 0.5d | 待提 | worker-memory §1.2 |
| `WM-03` ✅🧪 | `MemoryManager.java:340-399` + `StorageManager.scala:903-950` onTrim 全量遍历所有 writer（含 closed/空 buffer），无排序/限量，每个非空 buffer 都生成 FlushTask | 跳过 `isClosed()`；按 `readableBytes` 降序、达标即停；跳过 mini buffer | 1d | 待提 | worker-memory §3.1 |
| `WM-04` ✅🧪 | `Worker.scala:818-849` 整批 expiredShuffleKeys 作为**单个 Runnable** 提交给 64 线程 `cleanThreadPool`，批内串行；重启全量过期场景内存清理 ~110s | 按 shuffle 分片提交（~10 行） | 1d | 待提 | worker-memory §3.2 |
| `WM-05` ✅🧪 | `PartitionDataWriter.java:56,79`、`PartitionDataWriterContext.java:65`、`PartitionLocation.java:102-103,127` 等 per-writer 3.4KB；113K writer 驻留 | 8 项瘦身组合（writerString 延迟、shuffleKey intern、lazy bitmap、close 后置 null）；PartitionLocation 跟踪 #3825 | 3-5d | 需讨论 | worker-memory §4.1 |
| `WM-06` ✅🧪 | `TierWriter.scala:68,208` 构造即 `takeBuffer()`；writer 只有 commit/过期/内存压力三条出路，无 idle 检测，长尾 writer 占 direct | 首次 write 才取 buffer（~15-25 行）；加“最后写入超 N 秒 flush 归还 buffer” | 1-2d | 需讨论 | worker-memory §4.2 |
| `WM-07` ✅🧪 | `StorageManager.scala:315-331` graceful shutdown 每周期全量序列化 committedFileInfos（4.4K shuffle/11 万文件，数十 MB proto） | dirty-only 增量落盘（~20 行）；或继续调大 interval（纯配置） | 1-2d | 待提 | worker-memory §5.2 |
| `WM-08` ✅ | `MapPartitionDataReader.java:118-121` 每 credit stream `ByteBuffer.allocateDirect(indexBufferSize+16)`：非池化、不计 readBufferCounter，却计入 `usedDirectMemory` 参与 pause | 改池化/改 heap/计入 readBufferCounter | 0.5d | 待提 | worker-memory §6 |
| `WM-09` ✅ | `Worker.scala:750-757` 每个快照 user 都 `addGauge`，user 消失不 `removeGauge`；`WorkerInfo.scala:248-257` 对消失 key 置 0 而非删除 → 指标基数 = 历史累计任务数 | 快照消失的 user `removeGauge`（~10 行）；`WorkerInfo` 对消失 key `remove`（~5 行） | 0.5d | 待提 ⭐ | cluster-degradation 附录 C.3 |
| `WM-10` ✅🧪 | `Utils.scala:696-701` `splitShuffleKey` 用正则 `split("-")` + `dropRight.mkString` 重组 appId；push/fetch 每条至少 2 次，火焰图 4.23% | `lastIndexOf('-')` + 两次 `substring`；同改 `splitPartitionLocationUniqueId`、`splitAttemptKey` | 0.5d | 待提 | production 二章 P1-1 |
| `WM-11` ✅🧪 | `PushDataHandler.scala:1449-1457` `logTrace(s"""...""")` 无 `isTraceEnabled` 守卫，trace 关闭仍构造字符串并调 `getSplitThreshold/getCurrentFileInfo` | 加守卫或用 lazy 结构；顺带排查其他 logTrace | 0.5d | 待提 | production 二章 P1-2 |
| `WM-12` ✅🧪 | `Flusher.scala:77` 每 flush `s"Flusher-$this-${Random.nextInt()}"`（全局 Random CAS + 字符串）；`AbstractSource.scala:820-828` 空 labels 仍每次新建 metricName 字符串 | key 改固定 `Flusher-$this-$index`；`metricNameWithCustomizedLabels` 注册时缓存 | 0.5d | 待提 ⭐ | worker-memory §5.1；production 二章 P1-3 |
| `WM-13` ✅🧪 | `FlushTask.scala:59-78` 每次 flush 重建 `nioBuffers()` 数组；`incCounter`×2 无采样过滤；flush 路径 inclusive 18.67% | 评估复用 `nioBuffers` 结果；高频 counter 走 LongAdder 定期聚合 | 1-2d | 需讨论（IO 核心路径） | production 二章 P2-1 |
| `WM-14` ✅🧪 | `StorageManager.scala:59-60,542-562,625-665,741-755` `workingDirWriters` 无界；commit/close 后不移除 writer，只等 shuffle 过期；driver 卡死时无本地兜底 → 113K writer 驻留 | commit/close 后移出 writer；空目录清外层 entry；30 分钟定时任务加“最后写入时间”兜底 | 3-5d | 需讨论 | production 四章 P1-2；worker-memory F2 |
| `WM-15` ✅🧪 | `NettyUtils.java:171-178` 每个 allocator 同时建 heap+direct arena；`CelebornConf.scala:2012-2018` `numArenas` 为全局 key，master 无法单独配小；生产 120 个 HeapArena/244MB | `numArenas` 支持 per-module；生产按 IO 线程数配 16~32；删除无效的 `-Dio.netty.allocator.type` | 3-5d（代码）/0.1d（脚本） | 需讨论 | production 共性-2、四章 P1-1 |
| `WM-16` ✅🧪 | `WorkerSource.scala:141-151` 每条 push/fetch 都 `channel.id().asLongText()` + `splitShuffleKey` 取 appId，即使 appId 已在集合；火焰图 ~4.4% | 先 `contains` 判重；缓存 channel→appId；`splitShuffleKey` 见 WM-10 | 1-2d | 需讨论 | production 二章 P0-1 |
| `WM-17` ✅🧪 | `Flusher.scala:67,138-140,174-176` workingQueues 无界、offer 永不阻塞，慢盘不停止；congestion 默认关、全局水位兜底 → 单慢盘可拖垮 worker | 队列有界 + `addTask` 失败标记文件失败；队列深度接 per-disk 反压 | 3-5d | 需讨论 | production 四章 P0-1/P0-2 |

> 已剔除：`trimAllListeners` 丢触发、`MapPartitionDataReader` retain 泄漏、`commitFinishedChecker` 轮询等，见 §14.2。
> `CELEBORN-2301`（fetch 零拷贝拆分）上游已合入、生产 0.6.2.2 未含，属回 port 而非新 PR，见 §14.1。

---

## 6. Client

> 6.1 来源 `client-control-plane-deep-dive.md` §6（2026-09 核查）；6.2 来源 `client-data-plane-deep-dive.md` §8（2026-09 核查）。`performance-optimization-analysis.md` §5 中与本文重叠且前提成立的已并入，过时项见 §14.2。

### 6.1 控制面 LifecycleManager（LM）

| ID | 条目（位置 + 问题） | 建议改法 | 体量 | 状态 | 来源 |
|---|---|---|---|---|---|
| `LM-01` ✅ | `ReducePartitionCommitHandler.scala:272-276` StageEnd 前等 in-flight Commit 归零用 `Thread.sleep(1000)` 轮询，每个 Stage 多等 ~0.5s | in-flight 计数 + `CountDownLatch`/`Phaser`，归零即唤醒 | 1d | 待提 | §6.1 |
| `LM-02` ✅ | `LifecycleManager.scala:659-670`、`ReducePartitionCommitHandler.scala:489-526` 响应缓存 key 只有 shuffleId，但内容是“按当前 serdeVersion 序列化的字节”；混合版本会拿错 | key 改 `(shuffleId, serdeVersion)`；顺带封装统一 `RpcResponseCache` | 1-2d | 待提 | §6.2 |
| `LM-03` ✅ | `LifecycleManager.scala:584-610,1354-1389,1744-1803`、`CommitHandler.scala:336-421` 四处 `while + isCompleted + sleep(50)` 忙等 | 无重试的两处改 `Await.result`；有状态机的改 `onComplete` + 调度重试 | 1-2d | 待提 | §6.3 |
| `LM-04` ✅ | `LifecycleManager.scala:632-684,729-782` 全局 monitor 内做 `getLatestLocs` 全量遍历、缓存 loader 内响应序列化、并逐个回复等待 context；大 shuffle 阻塞所有注册 | 序列化移出锁；或按 shuffleId 分段加锁 | 3-5d | 需讨论 | §6.4 |
| `LM-05` ✅ | `ReducePartitionCommitHandler.scala:570-579` Reducer 等 StageEnd 固定 100ms 自旋，调用方含 `unregisterShuffle` | per-shuffle `CountDownLatch`/`CompletableFuture`，`setStageEnd` 统一唤醒 | 1d | 待提 | §6.5 |
| `LM-06` ✅ | `LifecycleManager.scala:1850-1881` `requestMasterRequestSlotsWithRetry` 名不副实：失败只立即再试一次、无退避 | 加退避有限重试；或改名 + 注释澄清 | 0.5d | 待提 | §6.6 |
| `LM-07` ✅ | `CommitHandler.scala:342` 双层 `while` 轮询内每次重复 `Utils.makeShuffleKey` 拼字符串 | 提到循环外 | 0.1d | 待提 | §6.7 |

> 已剔除：`offerAndReserveSlots` 四阶段并行、`ChangePartitionManager` 锁竞争、`handleMapperEnd` 逻辑重复等 4 项（前提不成立），见 §14.2。

### 6.2 数据面 ShuffleClient（SC）

| ID | 条目（位置 + 问题） | 建议改法 | 体量 | 状态 | 来源 |
|---|---|---|---|---|---|
| `SC-01` ✅ | `ShuffleClientImpl.java:232-234` retry 池固定 8 线程 + **无界队列**；任务体 50ms sleep 轮询 `reviveStatus`，闭包 retained 整个 batch byte[]；worker 宕机时失败风暴内存线性涨 | 等待与重推解耦（Revive 回调直接提交重推）；队列改有界，满则 `onFailure` | 3-5d | 需讨论 | §8.1 |
| `SC-02` ✅ | `ReviveManager.java:40-42,113-115` 固定 100ms tick，队列非空也不提前触发；等待方 50ms 轮询，端到端最坏 ~150ms+RPC | 队列非空立即调度；`ReviveRequest` 内置 latch，置状态即唤醒 | 3-5d | 需讨论（与 SC-01 同批） | §8.2 |
| `SC-03` ✅ | `CelebornInputStream.java:191,868,923` Exactly-Once 去重用 `Map<Integer, Set<Integer>>`，大 shuffle 数十万~百万条装箱节点；RoaringBitmap 已是本类依赖 | 改 `Map<Integer, RoaringBitmap>` | 1-2d | 待提 | §8.3 |
| `SC-04` ✅ | `CelebornInputStream.java:834` 每次 `fillBuffer` 分配从未读取的 `LocationPushFailedBatches`；`:175` 未使用 `RAND`；`ShuffleClientImpl.java:136` `splitting` 只 remove 不写 | 直接删除 | 0.1d | 待提 | §8.4 |
| `SC-05` ✅ | `DfsPartitionReader.java:259-261` 背压用 `while (results.size() >= max) Thread.sleep(50)` 忙等 | 换有界 `LinkedBlockingQueue(max)` + 阻塞 put | 1d | 待提 | §8.5 |
| `SC-06` ✅ | `DfsPartitionReader.java:265` 每 chunk 裸 `new byte[]`，上限默认 8MB，无池化 | 池化 `ByteBuf` 或复用缓冲 | 1-2d | 待提 | §8.6 |
| `SC-07` ✅ | `WorkerPartitionReader.java:102,204` vs `DfsPartitionReader.java:301` vs `LocalPartitionReader.java:230` 三处 poll 等待两处硬编码；且每次空 poll 也 `incReadTime`，污染读耗时 | 统一读 `clientFetchPollChunkWaitTime`；`incReadTime` 只在拿到 chunk 时计 | 0.5d | 待提 ⭐ | §8.7 |
| `SC-08` ✅ | `InFlightRequestTracker.java:157,222` `Thread.sleep(delta)` 轮询；`:147-151` 放行条件是“请求数达标 **OR** 字节数达标”，两维限流互相削弱（疑应为 AND） | 先向社区确认 OR/AND 语义；轮询改信号量 | 1-2d | 需讨论 | §8.8 |
| `SC-09` ✅ | `ShuffleClientImpl.java:1862-1872` 清理入口只有 `cleanupShuffle`，唯一调用方是 Spark；Flink/MR/Tez 的 `mapperEndMap/reducePartitionMap/reduceFileGroupsMap` 随 shuffleId 只增不清 | 各引擎补 shuffle 级清理钩子；或客户端加 shuffleId 空闲过期 | 3-5d | 需讨论 | §8.9 |
| `SC-10` ✅ | `ShuffleClientImpl.java:1078-1083` 单条 pushData 每 batch `new byte[16+length]` + arraycopy 全量拷贝；merged 路径已用 CompositeByteBuf | header + payload 改 composite buffer，验证下游处理 | 1-2d | 需讨论 | §8.10 |
| `SC-11` ✅ | `ShuffleClientImpl.java:2146-2185`、`ExceptionUtils.java:71-75` 失败归类依赖“异常消息以 StatusCode 枚举名开头”的非契约约定，改格式即静默落入兜底，影响 Revive/黑名单 | `CelebornIOException` 已带 StatusCode 就直接读字段，字符串匹配只做兜底；补单测锁格式 | 0.5d | 待提 | §8.11 |
| `SC-12` ✅ | `CelebornInputStream.java:456-527` vs `:529-590` “失败→重试计数→切 peer/原地/sleep”逻辑近乎逐行重复，两层循环共享计数 | 抽“失败后下一动作”决策函数；补失败路径测试 | 3-5d | 需讨论 | §8.12 |
| `SC-13` ✅ | `LocalPartitionReader.java:307-309` 返回 `Optional.empty()`（带 TODO）；本地直读不支持 checkpoint，重试从头读 | 复用 `PartitionReaderCheckpointMetadata` 补齐 | 1-2d | 待提 | §8.13 |
| `SC-14` ✅ | `DataBatches.java:51-56` 无参 `requireBatches()` 把 `batches` 置 null 且非 synchronized，并发 merge 时可能 NPE/静默丢 batch（Spark 单线程写路径下为潜伏债务） | 置空 list 而非 null，或整体同步 | 0.5d | 待提 | §8.14 |

> 已剔除：`WorkerPartitionReader` 无界队列 OOM、`fetchChunkMaxRetry` 语义、DFS 每次 next 开新流等 8 项，见 §14.2。

---

## 7. 网络与协议（NW）

> 来源：`network-and-protocol-deep-dive.md` §6（2026-09 核查，原 17 项保留 8 项）；其中 NW-03 已由 CELEBORN-2455 修复，本文列 7 项。

| ID | 条目（位置 + 问题） | 建议改法 | 体量 | 状态 | 来源 |
|---|---|---|---|---|---|
| `NW-01` ✅ | `NettyRpcEnv.scala:303-305` 控制消息仍用 Java 序列化包 `TransportMessage` 壳；V2 serde 快路径已实现（`:519-525`、`JavaSerializer.scala:119-125`）但全仓无消息用 `SerdeVersion.V2` | 默认切 V2 或加配置开关；HeartbeatFromWorker/RequestSlots 高频路径直接受益 | 3-5d | 需讨论（兼容性验证） | §6.1 |
| `NW-02` ✅ | `ControlMessages.scala` 1580 行：31 个 case class + ~530 行 `toTransportMessage` + ~500 行 `fromTransportMessage` 在同一 object | 按 Master/Worker/Client 拆文件；转换下沉 `PbSerDeUtils`，保留门面 | 3-5d | 需讨论 | §6.2 |
| `NW-04` ✅ | `TransportMessages.proto:248,707-719,731-732` 约 6 个 deprecated 字段 + 1 个兼容枚举；proto3 未设置零开销，成本是 schema 维护 | 大版本升级时清理 | 1d | 需讨论 | §6.4 |
| `NW-05` ✅ | `Message.java:126-182,189-254` 新增消息类型要改 4 处（enum、两个 switch、新类） | 注册表模式按 id 映射 decoder | 3-5d | 需讨论 | §6.5 |
| `NW-06` ✅ | `MessageDecoder.java:42-49` decode 无 try-catch，异常到末端 `exceptionCaught` 有 warn 但缺消息类型/buffer 状态上下文 | 补解码上下文日志 | 0.5d | 待提 | §6.6 |
| `NW-07` ✅ | `TransportContext.java:190-231` SSL/非 SSL 在 `initializePipeline` 内两处条件分叉、两套 encoder | 抽独立 SSL pipeline 构建方法 | 1d | 待提 | §6.7 |
| `NW-08` ✅ | `ControlMessages.scala:643-647,783-789,965-969` packed 已是事实默认且硬编码，`PbPartitionLocation` 非 packed 兼容路径是残留债务 | 大版本升级时删除非 packed 路径 | 1-2d | 需讨论 | §6.8 |

> `NW-03` 原条目（`Dispatcher.postMessage` 第一次多余 `synchronized`）已由 **CELEBORN-2455**（2026-10-10）修复，当前代码首行即 `endpoints.get(...)`；编号不回收，见 §14.1。  
> 已剔除：Outbox 无锁化、Inbox 锁改 Atomic、连接池取模热点等 9 项，见 §14.2。

---

## 8. Service / REST / 安全（SV）

> 来源：`service-and-security-deep-dive.md` §8（2026-09-06 核查）。与既有 Auth 文档重复的 A6（secret 明文）、D1（SASL 机制可配置）、SASL 明文文档项只在此引用，不展开。

| ID | 条目（位置 + 问题） | 建议改法 | 体量 | 状态 | 来源 |
|---|---|---|---|---|---|
| `SV-01` ✅ | `SSLFactory.java:326-327` 配置了 truststore 但文件不存在时，与“未配置”一样静默回退 `credulousTrustStoreManagers()`，TLS 认证意义丢失且无告警 | `trustStore != null && !exists()` 直接抛异常；仅未配置时回退并 WARN | 0.5d | 待提 | §8.3 |
| `SV-02` ✅ | `Master.scala:137-153`、`CelebornConf.scala:1650-1654` internal port 用无安全上下文的 RpcEnv、绑定主 host；且开 auth 强制开 internal port → 必然多一个免认证端口 | 文档/部署模板明确网络隔离；代码评估 internal SASL 或独立绑定地址 | 3-5d | 需讨论 | §8.4 |
| `SV-03` ✅ | `AuthenticationFilter.scala:169-179` catch 分支先 `remove()` 四个 thread-local，`finally` 审计记录 `user=null ip=null`；`RestAuditLogger.scala:26-28` `ThreadLocal<StringBuilder>` 从不 remove | 删除提前清理（外层 finally 已统一 remove）；审计后 `AUDIT_BUFFER.remove()` 或改局部 StringBuilder | 0.5d | 待提 | §8.5 |
| `SV-04` ✅ | `AuthenticationFilter.scala:161-163` 401 响应把原始 `Authorization` 头（含 Base64 凭证）写进错误消息 | 不回显头内容，至多保留 scheme 名 | 0.1d | 待提 | §8.6 |
| `SV-05` ✅ | `ConfResource.scala:138-181`、`HttpService.scala:132-142` 两个动态配置接口原样返回全部键值；静态 conf 已过 `Utils.redact` | 输出前统一 `Utils.redact` | 0.5d | 待提 ⭐ | §8.7 |
| `SV-06` ✅ | `FsConfigServiceImpl.java:79-80,53` 刷新时在 for 循环内逐条 `set`，system 配置原地修改 → 读者可见“半新半旧” | 循环结束一次性 `set`；SystemConfig 整体替换 | 0.5d | 待提 | §8.8 |
| `SV-07` ✅ | `BaseConfigServiceImpl.java:115-119` listener 逐个 `run()` 无 per-listener try/catch，一个异常中断后续；`listeners` 在调度启动后才赋值 | 每个 listener 包 try/catch；初始化移到调度前 | 0.5d | 待提 | §8.9 |
| `SV-08` ✅ | `SSLFactory.java:113-133` auto-SSL 只加密、不校验身份，配合 SV-01 的 credulous trust manager 无法防中间人；注释自认无法限制 executor 侧 | 限定 LM 侧生成证书；文档明确“仅加密非认证” | 1-2d | 需讨论 | §8.12 |
| `SV-09` ✅ | `SSLFactory.java:289` `setWantClientAuth(true)` 只请求不强制，mTLS 未用于身份认证 | 增加可选 `needClientAuth` 配置 | 1d | 需讨论 | §8.13 |
| `SV-10` ✅ | `HARaftServer.java:442-447` Ratis RPC type 非 GRPC 时只打一行 error 就禁用 TLS（返回 null），Netty-based 共识流量明文 | 不支持时启动 fail-fast，而非静默降级 | 0.5d | 待提 | §8.14 |
| `SV-11` ✅ | `DynamicConfigServiceFactory.java:28`、`DBSessionFactory.java:45` JVM 全局单例无角色隔离；HikariCP DataSource 创建后无 close 路径 | 单例按角色/conf 做 key；`shutdown()` 关闭 DataSource | 1-2d | 待提 | §8.15 |
| `SV-12` ✅ | `HttpService.scala:214-219` `while (state != STARTED) { logInfo; sleep(1000) }`，端口占用等启动失败无限等待 | 加超时并抛异常 | 0.5d | 待提 | §8.16 |
| `SV-13` ✅ | `CelebornSaslClient.java:72,109`、`CelebornSaslServer.java:62,86` `SaslException` 被包成 `IllegalArgumentException/RuntimeException`，调用方无法区分凭证错与协议错 | 定义 `CelebornSaslException`，保留 cause 链 | 0.5d | 待提 | §8.17 |
| `SV-14` ✅ | HTTP 管理面（BASIC/BEARER/SPNEGO）与控制面 SASL 两套身份体系割裂 | 属架构现状；部署文档给统一身份建议，长期收敛到 CELEBORN-1158 | 设计级 | 需讨论 | §8.18 |
| `SV-15` ✅ | `ApiBaseResource.scala:28-117` 旧 `/` text/plain 端点与 `/api/v1` JSON 双份维护，仅 `@Tag(Deprecated)` 非 `@Deprecated` | 大版本移除；至少加 `@Deprecated` | 1-2d | 需讨论 | §8.19 |
| `SV-16` ✅ | `ApiBaseResource.scala:56-57`（text/plain）vs `ConfResource.scala:39,64-68`（JSON）动态配置两套返回格式 | 随 SV-15 一并收敛 | 1d | 需讨论 | §8.20 |

> 已合入：`BasicAuthenticationHandler/BearerAuthenticationHandler` 的 `authenticationSupported` 类型检查（方向已修正）由 **CELEBORN-2456** 修复，见 §14.1。  
> 已映射：注册 secret 明文 → 既有 **A6**；SASL 机制硬编码 → **D1 / CELEBORN-1158**；SASL 只认证不加密的文档缺口 → 既有 Auth 文档项。

---

## 9. CLI（CLI）

> 来源：`cli-and-rest-api-analysis.md` §5（OpenAPI 一致性）与 §6（28 项缺陷/增强，含 P0~P3）。本节保留全部 28 项，不重排优先级。

| ID | 条目（位置 + 问题） | 建议改法 | 体量 | 状态 | 来源 |
|---|---|---|---|---|---|
| `CLI-01` ✅ | `master_rest_v1.yaml:1261-1263` `required: [worker]` 笔误，属性名是 `workers`；Swagger UI 显示必填错字段 | `worker`→`workers`，`-Pgenerate` 重新生成 | 0.1d | 待提 | §5.1/§6-1 |
| `CLI-02` ✅ | `MasterOptions.scala:48-53` paramLabel 提示 `DECOMMISSION_THEN_IDLE`，枚举常量是 `DECOMMISSIONTHENIDLE`，`valueOf` 必抛异常 | paramLabel 改为与枚举/yaml 一致 | 0.1d | 待提 ⭐ | §6-2 |
| `CLI-03` ✅ | `worker_rest_v1.yaml:750-761` `type` 默认 `NONE`，服务端落 `case _` 按当前默认退出类型（默认 Immediately）执行，非 no-op | 显式 `case "NONE"` 返回 no-op，非法值 400；或 yaml 去 `NONE`/default 改 required | 0.5d | 待提 | §5.2/§6-3 |
| `CLI-04` ✅ | 全局无 `--output-format`；输出是 `println(model.toString())` 伪 JSON（`HandleResponse.java:119-124`），无法喂 `jq`，无表格 | 加 `pretty/json/table`；json 用 Jackson 序列化 model | 1-2d | 需讨论 | §6-4 |
| `CLI-05` ✅ | `CelebornCli.scala:40-43` 创建两个 `CommandLine`，设置 case-insensitive 的第一个被丢弃 | 用设置好的 `cmd` 执行 | 0.1d | 待提 | §6-5 |
| `CLI-06` ✅ | `CliVersionProvider.scala:47-48` `--version` 字符串无 `s` 插值，`$CELEBORN_HOME` 原样输出 | 加 `s` 插值并用 `${CELEBORN_HOME}` | 0.1d | 待提 ⭐ | §6-6 |
| `CLI-07` ✅ | `BaseCommand.scala:49-50` `split(':')` 无 limit，value 含冒号或缺冒号时 `Array(k,v)` 抛 `MatchError` | `split(':', 2)` + 长度校验抛 `ParameterException` | 0.1d | 待提 | §6-7 |
| `CLI-08` ✅ | `WorkerSubcommandImpl.scala:67-68` `TypeEnum.valueOf` 无 `toUpperCase`、无 catch，非法输入抛 `IllegalArgumentException` | 用 `TypeEnum.fromValue`，与 master 侧异常处理对齐 | 0.1d | 待提 | §6-8 |
| `CLI-09` ✅ | `MasterSubcommandImpl.scala:189-191` 缺 `--worker-ids` 却提示 “Host list must be provided” | 改提示文案 | 0.1d | 待提 | §6-9 |
| `CLI-10` ✅ | `MasterSubcommandImpl.scala:201-210` `toWorkerId` `split(":")` 后直接取下标 + `toInt`，格式错误抛越界/数字异常 | 校验 5 段并抛 `ParameterException` | 0.5d | 待提 | §6-10 |
| `CLI-11` ✅ | `MasterSubcommand.scala:56` Master HA alias 多 host 时只取第一个，无 failover | 遍历地址直到成功，或查当前 leader | 1d | 待提 | §6-11 |
| `CLI-12` ✅ | `WorkerSubcommand.scala:43-52` worker 子命令不认 `--cluster`，缺 `--hostport` 直接抛异常 | 与 `MasterSubcommand` 对齐，复用 `CliConfigManager` | 1d | 待提 | §6-12 |
| `CLI-13` ✅ | `CliLogging.scala:32-38` 颜色无结尾 RESET，可能污染后续终端输出 | 末尾补 `${RESET}` | 0.1d | 待提 | §6-13 |
| `CLI-14` ✅ | `MasterSubcommand.scala:50,67-72`、`WorkerSubcommandImpl.scala:61-65` `ApiClient` 每次新建；三个状态查询各发一次相同请求 | `apiClient` 改 lazy val；共享一次响应 | 0.5d | 待提 | §6-14 |
| `CLI-15` ✅ | `CelebornCli.scala:39-44` 全模块无 `System.exit`，退出码语义未文档化 | 明确 0/1/2 并写入 `docs/celeborn_cli.md` | 0.5d | 待提 | §6-15 |
| `CLI-16` ✅ | `TestCelebornCliCommands.scala:394-406` 用 `System.setErr` 全局副作用，未在 finally 恢复 | finally 恢复 `System.err` | 0.1d | 待提 | §6-16 |
| `CLI-17` ✅ | `RatisApi` 的 master 10 个操作（leader 转移/选举暂停/peer 增删/snapshot 等）未暴露到 CLI | 加 `--ratis-*`；文件端点需 CLI 支持二进制输出 | 3-5d | 需讨论 | §3.3/§6-17 |
| `CLI-18` ✅ | `LoggerApi`（master/worker 各 2 操作）未暴露到 CLI | `--show-loggers` / `--set-logger <name>=<level>` | 1-2d | 需讨论 | §6-18 |
| `CLI-19` ✅ | metrics prometheus/json 端点未入 OpenAPI，SDK/CLI 无法查询 | `--show-metrics` 直接 GET `/metrics/*` | 1d | 需讨论 | §6-19 |
| `CLI-20` ✅ | `CheckQuota` 无 REST/CLI，需 yaml→生成→resource→CLI 全链路 | 新增 `GET /api/v1/quota/...` + `--show-quota` | 3-5d | 需讨论 | §4.3/§6-20 |
| `CLI-21` ✅ | `PbCheckWorkersAvailable` 与 `/ping` 可合并为健康探针，但 OpenAPI 无 | 新增 `/api/v1/health` + `--check-workers-available` | 1-2d | 需讨论 | §5.4/§6-21 |
| `CLI-22` ✅ | `ApplicationLost` 只能走 RPC，CLI 无法清理僵尸 app | `master --application-lost <appId>` | 1-2d | 需讨论 | §6-22 |
| `CLI-23` ✅ | 无 `--output-file` 落盘（Uniffle 有 `-f`，注意其取值 bug） | 新增选项 | 0.5d | 需讨论 | §6-23 |
| `CLI-24` ✅ | 无环境变量回退 `CELEBORN_CLI_HOST` / `CELEBORN_CLI_AUTH_HEADER` | 新增回退 | 0.5d | 需讨论 | §6-24 |
| `CLI-25` ✅ | 批量操作（如 `--exclude-worker`）不支持文件输入 | 支持一行一个 workerId | 1d | 需讨论 | §6-25 |
| `CLI-26` ✅ | 无 `--quiet/--verbose` 日志级别开关 | 新增 | 0.5d | 需讨论 | §6-26 |
| `CLI-27` ✅ | flag-based dispatch 导致 `run()` 长 `if` 链 | 重构为 picocli 嵌套子命令（注意兼容） | 3-5d | 需讨论 | §6-27 |
| `CLI-28` ✅ | 仅 HTTP，`setBasePath("http://...")` 写死；HTTP 禁用时无法管理集群 | 支持 HTTPS scheme；评估 RPC fallback | 3-5d | 需讨论 | §6-28 |

> 另见 §5 之 5.5：两个 yaml 重复定义十余个 schema、错误响应缺 schema、`servers` 写死 localhost、无 CI 校验生成代码同步 —— 归入 CLI 批次，状态“需讨论”，不单列 ID（属同一 OpenAPI 治理项）。

---

## 10. 引擎客户端（EN）

> 来源：`engine-clients-deep-dive.md` §6（2026-08，未逐条回读代码，标 ⚠️ 的需先复核）。Flink 8 项、MapReduce/Tez 10 项。标 ⚠️ 的“待提”项，开 JIRA 前先按当前 Flink/Hadoop/Tez 版本复核。

| ID | 条目（位置 + 问题） | 建议改法 | 体量 | 状态 | 来源 |
|---|---|---|---|---|---|
| `EN-01` ⚠️ | `FlinkShuffleClientImpl` 静态 `_instance` 靠 timestamp 比较感知 Driver 变化，对测试/多租户 classloader 不友好 | 改为按 job 生命周期管理实例，去掉全局可变单例 | 3-5d | 需讨论 | §6.1-1 |
| `EN-02` ⚠️ | `pushDataHandShake` 失败原因未用于 revive（代码内 `// TODO revive with handshake failure reason`） | 把握手失败原因映射到 Revive 决策 | 1-2d | 待提 | §6.1-2 |
| `EN-03` ⚠️ | segment 可见场景下空 location 直接抛 `PartitionUnRetryAbleException`，应重试 | 按可重试错误处理，走 revive/重试 | 1-2d | 待提 | §6.1-2 |
| `EN-04` ⚠️ | tiered consumer reader 打开失败不重试 | 加有限重试/退避 | 1-2d | 待提 | §6.1-2 |
| `EN-05` ⚠️ | `MAX_BYTES_PER_SEGMENT = 8MB`、result partition 7/8 排序 + 1/8 传输比例硬编码 | 提为配置并给默认值 | 1-2d | 需讨论 | §6.1-3 |
| `EN-06` ⚠️ | `CelebornFlinkShimV120` 反射访问 `resultPartitionBytes` 等私有字段，跨 Flink 版本脆弱 | 尽量走公开 API；按版本收敛 shim | 3-5d | 需讨论 | §6.1-4 |
| `EN-07` ⚠️ | 多处 `new UserIdentifier("default","default")`，未与 Celeborn 租户体系打通 | 从 Flink 配置/环境注入租户身份 | 1-2d | 需讨论 | §6.1-5 |
| `EN-08` ⚠️ | `RemoteShuffleEnvironment` 同时维护 Celeborn 与 Netty 两套 result partition，fallback/状态追踪复杂 | 抽 fallback 状态机，补测试 | 设计级 | 需讨论 | §6.1-6 |
| `EN-09` ⚠️ | MR/Tez `CelebornDagAppMaster` 等大量反射读写父类私有字段，跨 Hadoop/Tez 版本脆弱 | 按版本隔离 shim，补兼容测试 | 3-5d | 需讨论 | §6.2-1 |
| `EN-10` ⚠️ | MR/Tez 把 Celeborn block 包成合成 `MapOutput/FetchedInput`，reducer 再缓冲/合并已外部存储的数据 | 评估直通路径，避免二次拷贝 | 设计级 | 需讨论 | §6.2-2 |
| `EN-11` ⚠️ | `CelebornShuffleFetcher.writeShuffle()` / `CelebornTezBypassWriter.write(MapOutput)` 对 disk `MapOutput` 抛异常，迫使提升 reducer 内存 | 支持 disk-backed 有序合并 | 3-5d | 需讨论 | §6.2-3 |
| `EN-12` ⚠️ | `CelebornUnorderedPartitionedKVOutput.mapId`、`CelebornUnorderedKVOutput.mapId`、fetcher `uniqueMapId` 为静态可变，多 task JVM 可能冲突 | 改实例字段/线程安全分配 | 1-2d | 待提 | §6.2-4 |
| `EN-13` ⚠️ | 多处 `new CelebornTezWriter(shuffleId, mapId, mapId, attemptId, ...)`，签名第 3 参是 attemptNumber，实际把 `mapId` 传了进去 | 逐调用点核对并传 `attemptNumber`；补单测 | 0.5-1d | 待提 | §6.2-5 |
| `EN-14` ⚠️ | `CelebornUnorderedKVOutput` 的 `static broadcastOrOntToOne`、`static mapId` 在 session 模式有风险 | 改实例状态 | 1-2d | 待提 | §6.2-6 |
| `EN-15` ⚠️ | MR/Tez `MetricsCallback` 空实现，`incBytesRead/incReadTime` 未上报，丢失 shuffle 读指标 | 实现回调并接入 Tez/MR counters | 1-2d | 待提 | §6.2-7 |
| `EN-16` ⚠️ | `CelebornShuffleFetcher.fetchAndMerge()` catch `Exception` 后继续循环，持久错误下可能空转 | 区分可重试/致命，致命上抛 | 0.5d | 待提 | §6.2-8 |
| `EN-17` ⚠️ | `CelebornSortBasedPusher` 用 `HashMap<Integer, List<SerializedKV>>` + per-partition `AtomicInteger/Long`，稠密分区可用原生数组 | 分区号连续时用数组/开放寻址 | 1-2d | 需讨论 | §6.2-9 |
| `EN-18` ⚠️ | Tez 复用 `celeborn.client.mr.pushData.max`，无独立命名空间 | 新增 `celeborn.client.tez.*`，保留旧键别名 | 0.5d | 待提 | §6.2-10 |

> 本表来自 2026-08 的引擎走读，未像 5 篇 deep-dive 那样逐项复核；动工前先按当前 Flink/Hadoop/Tez 版本确认。`EN-13` 是明确的参数错位，优先验证。

---

## 11. Spark 链路与指标（SP）

> 来源：`celeborn-shuffle-pipeline.md` §4.6/§4.7。**第四部分基线是 `parallel-partition-write` 分支**（含自定义 Write/Read Times 页面）；本工作树没有该页面，故下表多数条目标“仅内部/分支上游化时处理”，不是当前上游 main 的可直接 PR。

| ID | 条目（位置 + 问题） | 建议改法 | 体量 | 状态 | 来源 |
|---|---|---|---|---|---|
| `SP-01` ⚠️ | 写侧无 mapperEnd RPC 时长，混在 writeTime 里无分解 | 独立计时并上报（自定义 source 或日志聚合） | 1-2d | 仅内部 | §4.7.1 |
| `SP-02` ⚠️ | 写侧无 revive 次数/耗时，容错开销不可见 | 计数 + 耗时 | 1-2d | 仅内部 | §4.7.1 |
| `SP-03` ⚠️ | `integrityCheck.enabled=true` 时 CRC 在 task 线程 `DataPusher.java:156` 计算，未计时 | 加 nanoTime 计时 | 0.5d | 仅内部 | §4.7.1 |
| `SP-04` ⚠️ | push 失败只有 `lastPushFailureReason` 字符串，无按原因计数 | 按 `StatusCode` 分桶计数 | 1d | 仅内部 | §4.7.1 |
| `SP-05` ⚠️ | 无 batch 数/平均大小/giant record 计数，无法判断 64KiB 是否打满 | 加分布指标 | 1d | 仅内部 | §4.7.1 |
| `SP-06` ⚠️ | sort 模式无 sort/spill 时间 | 在 SortBasedShuffleWriter 计时 | 1d | 仅内部 | §4.7.1 |
| `SP-07` ⚠️ | 加密时间（crypto 启用时）无指标 | 在加解密处计时 | 1d | 仅内部 | §4.7.1 |
| `SP-08` ⚠️ | 读侧 `retryCount/peerSwitchCount/excludeCount` 已聚合（Listener:215-220）但未上页面 | 页面补展示（零采集成本） | 0.5d | 仅内部 | §4.7.2 |
| `SP-09` ⚠️ | 读中途切流/revive 的 per-location `openStream` 未计时，只有 reader 初始化 batch 级 | 在 `CelebornInputStream` 内计时 | 1d | 仅内部 | §4.7.2 |
| `SP-10` ⚠️ | partition 数/skip location 数（skipCount 已在日志 summary）未上报 | 加指标 | 0.5d | 仅内部 | §4.7.2 |
| `SP-11` ⚠️ | 写时间分解表把跨线程并行项并排展示，易误读为串行；`serialize=0` 可能是 fastWrite0 快路径 | 表加“不可相加”注解；serialize=0 加提示 | 0.1d | 仅内部 | §4.7.3 |
| `SP-12` ⚠️ | 缺“未归因时间”派生项 = writeTime − (queueWait + drain + giant + closeWrite) | 加派生指标 | 0.5d | 仅内部 | §4.7.3 |

> `ReadMetrics` 的 `deserializeTimeMs`/`copyTimeMs` 死字段、`fetchWaitTime` 口径拓宽等属该分支代码；本工作树无 `ReadStreamStats.java`，不单列。`incReadTime` 计空轮询的问题在 **SC-07**（当前 main 存在）。

---

## 12. 配置 / 文档 / 架构债（CF）

> 来源：`ARCHITECTURE_ANALYSIS.md` §6、`performance-optimization-analysis.md` §10。均为跨模块债务，动工前需拆成可评审的小 PR。

| ID | 条目（位置 + 问题） | 建议改法 | 体量 | 状态 | 来源 |
|---|---|---|---|---|---|
| `CF-01` ⚠️ | `CelebornConf.scala` ~7,078 行，配置项与 builder 集中在一个文件 | 按功能域拆 trait/object，保留 `CelebornConf` 门面；多 PR 迁移 | 设计级 | 需讨论 | ARCHITECTURE §6.1.1；performance §10 |
| `CF-02` ⚠️ | `Worker.scala` ~1137、`LifecycleManager.scala` ~2105、`ShuffleClientImpl.java` ~2201 行，职责混合；`Master.scala` 见 MA-10 | 按子系统提取 manager/handler，行为不变 | 设计级 | 需讨论 | ARCHITECTURE §6.1.1 |
| `CF-03` ⚠️ | 缺压力测试与混沌测试，集成测试依赖外部 Spark/Flink | 先补 1~2 个可重复的混沌用例（worker kill/慢盘/网络分区） | 设计级 | 需讨论 | ARCHITECTURE §6.1.2 |
| `CF-04` ⚠️ | 文档缺场景化指南、REST API 文档需更新、缺深度原理文档（`docs/monitoring.md` 的漂移已在既有 M5/M11/PR-4 覆盖） | 按“部署/调优/排障”三类补指南；REST 文档随 CLI 批次更新 | 3-5d | 需讨论 | ARCHITECTURE §6.1.3 |

> `celeborn-conf-functional-map.md` 是人工导读，**没有缺口清单**（0 条），已在附录 A 登记。

---

## 13. 设计提案 / CIP（DS）

> 这些不是“改几行”的 PR，需要 CIP / dev@ 讨论或较大规模实现。状态栏区分“已实现待上游化”“已开 JIRA”“内部验证中”。

| ID | 提案 | 现状与上游化路径 | 风险 / 前置 | 状态 | 来源 |
|---|---|---|---|---|---|
| `DS-01` ✅ | **Overlapping 压缩/解压**：写路径 pipeline 压缩 + 读路径异步解压，对齐 Uniffle `OverlappingCompressionDataPusher` / PR #2602 | 分 3 步：第一步把 `Zstd/Lz4 Compressor/Decompressor` 的 `checksum/compressedBuffer` 改为方法局部变量（可独立 PR）；第二步读路径 `DecompressionWorker` + 全局共享池；第三步写路径 `DeferredCompressedBlock` | 读路径信号量许可释放、partition 切换、attempt 过滤易出 bug（Uniffle 9 个 fix）；写路径收益需 benchmark | 需讨论 | overlapping §1/§9 |
| `DS-02` ✅ | **自适应分区写并行度** `adaptivePartitionWriteParallelism`：热点判定在 LM，executor 多处并行写，`additionalPartitions` additive 字段 | 已在本仓库分支 `parallel-partition-write` 实现 Phase 1.3（~1300 行含测试）；作为 CIP-20 / PR #3260 的延续提 CIP：CIP-20 是 stale bot 关闭、非技术否决 | 需补 AQE skew、replicate 2K slot 压测；LM 复杂度需在 CIP 说明 | 需讨论（CIP） | dynamic-partition §2/§13 |
| `DS-03` ⚠️ | **App 粒度 Congestion Control**：在 User 维度外增加 appId 维度水位与限速 | 仅在得物 `celeborn-dewu` 分支设计，本仓库无 `AppCongestionControlContext`；需内部验证后再评估上游 CIP | per-app 状态基数、内存上限、与 User 级 OR 语义 | 仅内部 | congestion-control 第二部分 |
| `DS-04` ⚠️ | **集群高峰自动降级**：复用拥塞信号，Client 按任务优先级三档响应 + SKYNET 粒度限速 | 内部立项 v3.0；**上游可先做 2 个前置修复**（WM-09），其余策略类留在内部 jar | 依赖 Spark 标签/SKYNET，非通用；上游只提前置正确性修复 | 仅内部（前置见 WM-09） | cluster-degradation §附录 C |
| `DS-05` ✅ | **CELEBORN-2465**：SQL query 结束时主动 unregister shuffle（opt-in `queryEndShuffleCleanup.enabled` + `SparkListenerSQLExecutionEnd`） | JIRA 已开，仅有提案与 Test Plan，无代码 | 需确认与 Spark GC/ContextCleaner 的重复清理、长会话兼容 | 已有 JIRA | issue-query-end 全篇 |
| `DS-06` ✅ | **CIP-23 / CELEBORN-2429**：Batched Heartbeat Raft Writes | 已合入（`HeartbeatAggregator.java` + suite 在当前树），HA-01 的“批量”部分关闭 | 无 | 已合入 | cip-master-ha-heartbeat-batch |

---

## 14. 状态台账与勿提

### 14.1 已提 / 已合入

| 编号 | 内容 | 状态 | 对本文影响 |
|---|---|---|---|
| CELEBORN-2456 | BASIC/BEARER `authenticationSupported` 类型检查 + fail-fast | 已合入（`BasicAuthenticationHandler.scala:43-49` 方向已修正） | SV 原 8.1 从清单移除 |
| CELEBORN-2455 | `Dispatcher.postMessage` 删除多余 `synchronized` | 已合入（2026-10-10） | NW-03 从清单移除 |
| CELEBORN-2392 | 并发注册防误过期（内层 Set 改 `newKeySet`） | 已合入（2026-10-29） | MA-03 仅剩 unregister 判空/原子性 |
| WK-06 澄清 | `ReadBufferDispatcher.release(refCnt)` 为有意语义：reader 失败时 buffer 有多个引用，需一次释放 | 已澄清，不建议改 | 从清单移除 |
| CELEBORN-2429 / CIP-23 | 心跳批量 raft entry | 已合入（`HeartbeatAggregator`） | HA-01 仅剩“状态变更才复制/统计本地化” |
| CELEBORN-2465 | SQL query end unregister shuffle | JIRA 已开，提案 | 见 DS-05 |
| CELEBORN-1158 | SASL 机制可配置 | open | SV 不重复；D1 引用 |
| CELEBORN-2301 | fetch 零拷贝拆分（header/FileRegion 分离） | 上游已合入；生产 0.6.2.2 未含 | 回 port 项，非新 PR |
| #3825 / CELEBORN-2442 | PartitionLocation 内存 -51~56% | open | WM-05 子项跟踪，不自研 |
| #3809 / CELEBORN-2428 | TierWriter buffer accounting | open | WM-06 回 port 注意顺序 |
| CELEBORN-2152 | push mergeBuffer | 已合入 0.6.2 | 生产配置项，见 §14.3 |

### 14.2 各文档“核查后剔除”汇总（勿重复提）

| 来源 | 数量 | 代表条目 | 详见 |
|---|---|---|---|
| master-control §7 附表 | 7 | rack-aware O(W²)、QuotaManager 全量聚合、tags 过滤在锁内 | 原文 §7 附表 |
| worker-storage §8 附表 | 12 | 内存检查间隔 100ms、DCL 锁序死锁、S3 6MB 分片、`isPaused` 非 volatile（已修） | 原文 §8 附表 |
| client-control §6 附表 | 4 | offerAndReserveSlots 全串行、ChangePartition 锁竞争、MapperEnd 逻辑重复 | 原文 §6 附表 |
| client-data §8 附表 | 8 | `WorkerPartitionReader` 无界队列 OOM、fetchChunkMaxRetry 语义、DFS 每次开新流 | 原文 §8 附表 |
| network §6 附表 | 9 | Outbox 无锁化、Inbox 锁改 Atomic、连接池取模热点 | 原文 §6 附表 |
| service §8 附表 | 4 | FsConfigServiceImpl synchronized 阻塞读、审计 setStatus 顺序、SSL 密码明文 | 原文 §8 附表 |
| master-ha §7 附表 | 1 | Leader hint 缓存（已存在） | 原文 §7 文末 |
| **合计** | **45** | | |

> 另：`performance-optimization-analysis.md` 中被上述 deep-dive 否掉的条目（§3.6.2、§3.9.5 等）不再单列，以 deep-dive 结论为准。

### 14.3 仅内部 / 不建议上游

| 类别 | 条目 | 原因 |
|---|---|---|
| 生产配参 | `worker.pushdata.mergeBuffer`、JVM 堆/堆外配比、`metrics.sample.rate`、scrape 间隔、`-Dio.netty.leakDetectionLevel=advanced`、Recycler 禁用 | 部署侧权衡，不宜改上游默认值；leakDetection 只应在排查时临时开 |
| 内部实现 | SKYNET_ID 身份、`GradedPushStrategy`、App 级拥塞控制 | 得物 fork 专属，依赖内部标签/治理 |
| 兼容性禁区 | QOP 默认提到 `auth-int`、secret 改 KDF/哈希、双 schema 立即删除 | 混版本握手失败或三端不兼容，需 CIP |
| 运维清理 | Arthas agent 残留、metric tag 字符串重复 | 环境/低收益，非代码 PR |
| 已 won't fix | pom `artifactId/version contains an expression` WARNING | Scala 交叉构建通行做法 |

---

## 15. 提交顺序（与既有文档合并）

| 顺序 | ID | 内容 | 来源 | 备注 |
|---|---|---|---|---|
| 1 | T1 | `ThreadUtils` 线程 dump 丢 owner Id | community-and-ci §2 | 既有首推 |
| 2 | M1~M3 | histogram label、`RpcMetricsTracker.close`、`getAllMetricsNum` | community-and-ci §3 | 既有首批 |
| 3 | A2 → A1 | replicate 端口 SASL → `checkAuth` fail-open | community-and-ci §4 | 顺序不能反 |
| 4 | **MA-03** | unregister 的 get→remove→compute 原子化并判空 | §3 | 0.5d |
| 5 | **WM-09** | per-user gauge / `WorkerInfo` key 生命周期 | §5.2 | 两文件 ~15 行 |
| 6 | **HA-03** | 快照 rename 原子性 + md5 顺序 | §4 | 1d |
| 7 | **SV-05** | 动态配置 REST 脱敏 | §8 | 安全小修 |
| 8 | **CLI-02 + CLI-06** | 枚举名、`--version` 插值 | §9 | 可同 PR |
| 9 | **WM-12** | Flusher key 固定 + metricName 缓存 | §5.2 | 0.5d |
| 10 | **SC-07** | `incReadTime` 只计实际拿到 chunk | §6.2 | 指标口径 |
| 11 | C2 / C3 | LifecycleManager 泄漏、Maven 缓存 | community-and-ci §5 | 消日志噪声 |
| 12 | DS-01 / DS-02 | 压缩重叠、自适应并行度 | §13 | 先 CIP / dev@ |

每个 PR 的统一动作：`CELEBORN-XXXX` Jira → `./dev/reformat` → 相关模块单测 → 配置变更时 `UPDATE=1 ./build/mvn test -pl common -Dtest=ConfigurationSuite`。

---

## 16. 维护约定

1. **新增**：先在本文件对应模块表加一行（ID 顺延），再把细节写回来源文档；本文件不承载长篇证据。
2. **状态变化**：只改“状态”列；已合入的移到 §14.1 并保留一行映射，正文行删除或降为引用。
3. **去重**：新条目先按 `file:line + 问题本质` 在本文与 `celeborn-community-and-ci.md` 搜一遍；命中则只补来源，不新增 ID。
4. **基线**：每季度按最新 `main` 复核 ⚠️ 与 🧪 行；行号漂移只更新来源引用，不在本文重写行号。
5. **优先级**：本文不定义 P0/P1/P2；需要优先级时引用 `performance-optimization-analysis.md` §11。

---

## 附录 A：覆盖矩阵（21 篇）

| # | 文档 | 挖掘章节 | 落点条目 | 落点 |
|---|---|---|---|---|
| 1 | ARCHITECTURE_ANALYSIS.md | §6 架构挑战 | 4 | CF-01~04 |
| 2 | celeborn-conf-functional-map.md | 全篇（导读） | 0 | 无缺口清单 |
| 3 | celeborn-shuffle-pipeline.md | §4.6/§4.7 | 12 | SP-01~12（+SC-07 去重） |
| 4 | cip-master-ha-heartbeat-batch.md | 全篇 | 1 | DS-06（已合入） |
| 5 | cli-and-rest-api-analysis.md | §5/§6 | 28 | CLI-01~28 |
| 6 | client-control-plane-deep-dive.md | §6 | 7 | LM-01~07 |
| 7 | client-data-plane-deep-dive.md | §8 | 14 | SC-01~14 |
| 8 | cluster-degradation-control-design.md | 附录 C | 2 | DS-04；WM-09（2 处前置合并为 1 行） |
| 9 | congestion-control-analysis-and-design.md | 第二部分 | 1 | DS-03 |
| 10 | dynamic-partition-write-parallelism-design.md | 全篇 | 1 | DS-02 |
| 11 | engine-clients-deep-dive.md | §6 | 18 | EN-01~18 |
| 12 | issue-query-end-shuffle-cleanup.md | 全篇 | 1 | DS-05 |
| 13 | master-control-plane-deep-dive.md | §7 | 10 | MA-01~10（TagsManager→T2） |
| 14 | master-ha-ratis-deep-dive.md | §7 | 12 | HA-01~12（slot 锁→MA-01） |
| 15 | network-and-protocol-deep-dive.md | §6 | 7 | NW-01~02、04~08（NW-03 已合入） |
| 16 | overlapping-compression-decompression-design.md | §1/§9 | 1（3 阶段） | DS-01 |
| 17 | performance-optimization-analysis.md | §11/§2.6/§3.11/§4.6/§8/§10 | 索引 | 作为去重基准；独有项并入 WM/CF/HA |
| 18 | production-performance-analysis.md | 二/四/五章 | 9 | WM-10~17、HA-13（WK-11 与 21 去重） |
| 19 | service-and-security-deep-dive.md | §8 | 16 | SV-01~16（8.1 已修；8.2/8.10/8.11 映射 A6/D1/文档） |
| 20 | worker-memory-and-optimization.md | 第一部分/第二部分 | 8 | WM-01~08 |
| 21 | worker-storage-and-traffic-deep-dive.md | §8 | 14 | WK-01~05、07~15（WK-06 已澄清；§8.13 并入 WM-12） |

**对账**：正文 166 条 = MA 10 + HA 13 + WK 14 + WM 17 + LM 7 + SC 14 + NW 7 + SV 16 + CLI 28 + EN 18 + SP 12 + CF 4 + DS 6。

## 附录 B：抽取方法与核验记录

- **来源分级**：A 级 = 2026-09 已逐项核查的 deep-dive §可优化点（master-control / master-ha / worker-storage / client-control / client-data / network / service）；B 级 = performance（2026-08）、production / worker-memory（🧪 0.6.2.x）、CLI / 引擎 / Spark pipeline（2026-08）、设计文档。A/B 冲突以 A 为准；A 明确剔除的 B 级条目不再复活。
- **去重键**：`file:line + 问题本质`；跨文档同一问题只保留一行，来源列全列；与既有文档重叠只做映射。
- **核验记录（2026-10-31）**：
  - `CELEBORN-2456`：`BasicAuthenticationHandler.scala` 已改为 `classOf[PasswdAuthenticationProvider].isAssignableFrom(...)`，确认已修。
  - `CELEBORN-2455`：`Dispatcher.postMessage` 首行已是无锁 `endpoints.get(...)`，确认 NW-03 已修（2026-10-10）。
  - `CELEBORN-2392`：`AbstractMetaManager` 内层已改 `newKeySet()`，MA-03 仅剩 unregister 判空/原子性（2026-10-29）。
  - `WK-06`：`ReadBufferDispatcher.recycle` 现有注释说明 reader 失败会多引用、需一次释放全部，原“改减一”建议不成立。
  - `CELEBORN-2429`：`HeartbeatAggregator.java` 与 `HeartbeatAggregatorSuiteJ.java` 存在，确认已合入。
  - per-user 两处：`Worker.scala:750-757` 仍对每个快照 user `addGauge` 且无移除；`WorkerInfo.scala:248-257` 仍对消失 key 置 0 —— 确认 WM-09 仍有效。
  - `adaptivePartitionWriteParallelism`：全仓 grep 无匹配，确认仅存在于 `parallel-partition-write` 分支。
  - `ReadStreamStats.java`：当前树不存在，确认 SP 章死字段属分支代码。
- **计数口径**：CLI 28、EN 18、WM 17 等按“一行一个可独立验证的问题”计；同一 PR 可覆盖多条，计数不代表 PR 数。

