# Celeborn 调研知识库 · 索引

> 本仓库根目录是 Apache Celeborn 的调研 / 分析工作区。**本文件是唯一入口**：按目的导航 + 文档地图 + 待办清单 + 维护约定。
> 基线：Apache Celeborn 0.7.0-SNAPSHOT（分支 `ai-1`）｜ 最后更新：2026-10-31
> 新增、合并或删除文档前，请先读 §3 维护约定。

---

## 0. 按目的导航

| 我想… | 去这里 |
|---|---|
| 快速了解 Celeborn 整体架构 | [ARCHITECTURE_ANALYSIS.md](ARCHITECTURE_ANALYSIS.md)（约 30 分钟） |
| 看某个模块**源码级**怎么跑 | §1.1 源码阅读系列（10 篇，按模块划分，均带行号） |
| 找能改的代码 / 能提社区的 PR | [performance-optimization-analysis.md](performance-optimization-analysis.md) §11 优先级矩阵 + [celeborn-community-and-ci.md](celeborn-community-and-ci.md) §2~§4 |
| 搞懂 Spark 侧读写**到底慢在哪** | [celeborn-shuffle-pipeline.md](celeborn-shuffle-pipeline.md)（流程图）+ [spark-shuffle-thread-model-and-metrics.md](spark-shuffle-thread-model-and-metrics.md)（线程模型与指标口径） |
| 查某个配置键 | [celeborn-conf-functional-map.md](celeborn-conf-functional-map.md)（人工精选导读）；全量权威见 `docs/configuration/` |
| 写设计 / 提案 / 立项 | §1.4 设计文档与提案（6 篇） |
| 看生产问题的实证证据 | [production-performance-analysis.md](production-performance-analysis.md)（火焰图/heapdump）、[worker-memory-and-optimization.md](worker-memory-and-optimization.md)（内存根因） |
| 跟进 CI 问题 / 优化 CI | [celeborn-community-and-ci.md](celeborn-community-and-ci.md) §5 |

---

## 1. 文档地图

> 「状态」含义：✅ 已对照代码核实 ｜ 📊 生产数据实证 ｜ 📝 提案/设计（未落地代码） ｜ 🔧 已实现/进行中 ｜ EN 英文文档

### 1.1 架构与源码阅读（10 篇）

| 文档 | 内容 | 基线 / 更新 |
|---|---|---|
| [ARCHITECTURE_ANALYSIS.md](ARCHITECTURE_ANALYSIS.md) | 系统架构总览：Master-Worker-Client 组件、数据流与容错、关键技术决策、架构挑战（已瘦身，模块细节指向各 deep-dive） | 0.7.0-SNAPSHOT / 2026-08-05 |
| [network-and-protocol-deep-dive.md](network-and-protocol-deep-dive.md) | 网络与 RPC 框架 + Shuffle 协议与消息格式：帧格式/编解码/零拷贝/Outbox/Dispatcher/proto/MessageType/SerdeVersion | 0.7.0-SNAPSHOT / 2026-08-05，§6 于 2026-09 复检 ✅ |
| [master-control-plane-deep-dive.md](master-control-plane-deep-dive.md) | Master 控制面（非 HA）：Worker 注册/心跳/超时剔除、requestSlots → SlotsAllocator 全链路（ROUNDROBIN / LOADAWARE 行号级走读）、应用生命周期、配额与 tags | 0.7.0-SNAPSHOT / 2026-08-20，§7 于 2026-09-06 增补 ✅ |
| [master-ha-ratis-deep-dive.md](master-ha-ratis-deep-dive.md) | Master HA（Ratis 共识）：写请求链路、快照、选举、双 schema | 0.7.0-SNAPSHOT / 2026-08-05 ✅ |
| [worker-storage-and-traffic-deep-dive.md](worker-storage-and-traffic-deep-dive.md) | Worker 模块：Push/Fetch/CommitFiles 处理链、StorageManager/FileWriter/Flusher、Reduce/MapPartition 文件布局、磁盘自检与 HARD_SPLIT、BackPressure 水位 + CongestionControl、多级存储 | 0.7.0-SNAPSHOT / 2026-08-20，2026-09-07 复核并新增 §8 ✅ |
| [client-control-plane-deep-dive.md](client-control-plane-deep-dive.md) | Client 控制平面（LifecycleManager）：offerAndReserveSlots、StageEnd、Revive、Commit | 0.7.0-SNAPSHOT / 2026-08-05，§5-§6 于 2026-09 修订 ✅ |
| [client-data-plane-deep-dive.md](client-data-plane-deep-dive.md) | Client 数据面（ShuffleClientImpl）：pushData/pushMergedData、Revive/BatchRevive、SOFT/HARD_SPLIT、fetchChunk 重试与切 replica、Exactly-Once 去重 | 0.7.0-SNAPSHOT / 2026-08-20 ✅ |
| [service-and-security-deep-dive.md](service-and-security-deep-dive.md) | Service 模块（HTTP/REST/动态配置）+ 安全机制（SASL/SSL/HTTP 认证） | 0.7.0-SNAPSHOT / 2026-08-05，§8 于 2026-09-06 核查 ✅ |
| [engine-clients-deep-dive.md](engine-clients-deep-dive.md) | 引擎客户端：Flink（标准 RSS + Tiered，1.16–2.3）、MapReduce / Tez、跨引擎对比 | 0.7.0-SNAPSHOT / 2026-08-05 ✅ |
| [cli-and-rest-api-analysis.md](cli-and-rest-api-analysis.md) | CLI 模块 + OpenAPI/REST 审计 + RPC→REST→CLI 三层暴露缺口 + 与 Uniffle CLI 对比 | 0.7.0-SNAPSHOT / 2026-08-05 ✅ |

### 1.2 性能与生产实证（3 篇）

| 文档 | 内容 | 基线 / 更新 |
|---|---|---|
| [performance-optimization-analysis.md](performance-optimization-analysis.md) | 性能优化静态代码分析总览（Master/Worker/common/Client/Spark/网络层）+ **§11 全量优先级矩阵（P0/P1/P2 的唯一权威视图）** | 0.7.0-SNAPSHOT / 2026-08-05 ✅ |
| [production-performance-analysis.md](production-performance-analysis.md) | 生产实证分析：Worker/Master CPU 火焰图 + Heapdump，含跨组件共性发现 | 生产 0.6.2.x，行号已对 0.7.0-SNAPSHOT 核实 / 2026-08-05 📊 |
| [worker-memory-and-optimization.md](worker-memory-and-optimization.md) | **Worker 高内存水位根因 + 优化清单**：第一部分根因（无界缓冲驻留 + writer 生命周期），第二部分增量优化（P0-P2、CELEBORN-2301 回 port） | 生产 0.6.2.2 / 2026-09-13 profile 📊 |

### 1.3 Spark 侧链路与指标（2 篇）

| 文档 | 内容 | 基线 / 更新 |
|---|---|---|
| [celeborn-shuffle-pipeline.md](celeborn-shuffle-pipeline.md) | 读写链路 Mermaid 流程图（14 张）：写侧拓扑/泳道/三级反压/CPU 争抢/耗时归属，读侧拓扑/泳道/失败重试/耗时归属，第三部分与 Spark 原生 SortShuffleWriter、BlockStoreShuffleReader 对比 | `celeborn-ai` 工作树 / 2026-08-05 |
| [spark-shuffle-thread-model-and-metrics.md](spark-shuffle-thread-model-and-metrics.md) | Spark task 侧线程模型（DataPusher / WorkerPartitionReader，行号级）+ 生产指标归因 + Spark UI Write/Read Times 逐字段审计与缺口清单（W1-W7/R1-R3/D1-D2） | `parallel-partition-write` 分支 / 2026-08-09~10 |

### 1.4 设计文档与提案（6 篇）

| 文档 | 内容 | 状态 |
|---|---|---|
| [dynamic-partition-write-parallelism-design.md](dynamic-partition-write-parallelism-design.md) | **自适应分区写并行度**（adaptivePartitionWriteParallelism）：CIP-20 / PR #3260 缺陷分析 + 本仓库落地设计（Phase 1–1.3） | 🔧 已实现（分支 `parallel-partition-write`） |
| [overlapping-compression-decompression-design.md](overlapping-compression-decompression-design.md) | Overlapping 压缩/解压：Compressor/Decompressor 线程安全审计、对比 Uniffle Codec 无状态单例、写路径 pipeline 压缩 + 读路径异步解压 | 📝 仅调研与设计，未改代码 |
| [congestion-control-analysis-and-design.md](congestion-control-analysis-and-design.md) | CongestionControl 现状深度调研（第一部分）+ App 粒度扩展设计（第二部分，基于得物内部版 celeborn-dewu） | 📝 第一部分 ✅，第二部分内部推进 |
| [cluster-degradation-control-design.md](cluster-degradation-control-design.md) | **技术立项：集群高峰期自动降级控制**：Worker 拥塞信号触发（3 行配置）+ Client 按任务属性三档分级响应，SKYNET_ID 任务粒度独立速率上限；含附录 A-E 技术细节 | 🔧 立项 v3.0（2026-10-27），生产观察期进行中 |
| [cip-master-ha-heartbeat-batch.md](cip-master-ha-heartbeat-batch.md) | **CIP 草案：Batched Heartbeat Raft Writes**（CELEBORN-2429）：HA 下心跳是最高频 raft 写（生产峰值 ~97 entries/s），提案批量化削减条目数 | 📝 Draft EN（分支 `CELEBORN-2429`） |
| [issue-query-end-shuffle-cleanup.md](issue-query-end-shuffle-cleanup.md) | **CELEBORN-2465 提案**：SQL query 结束时主动 unregister shuffle（opt-in 配置 + SQLExecutionEnd 监听） | 📝 仅提案，未落地 EN |

### 1.5 社区贡献与 CI（1 篇）

| 文档 | 内容 |
|---|---|
| [celeborn-community-and-ci.md](celeborn-community-and-ci.md) | 合并篇：代码 TODO 6 项推荐（34 处全量评估）+ Metrics/Grafana 20 个可提交点（P0 5 / P1 9 / P2 6）+ Auth/SASL 11 个 PR 候选 + CI 问题台账（P1-P11）与 5 项优化提案；含跨方向提交顺序 |

### 1.6 配置参考（1 篇）

| 文档 | 内容 |
|---|---|
| [celeborn-conf-functional-map.md](celeborn-conf-functional-map.md) | Celeborn 配置键功能地图（20 个功能域的人工精选导读；全量权威参考见 `docs/configuration/`） |

---

## 2. 待办

> 优先级（P0/P1/P2）只在 [performance-optimization-analysis.md](performance-optimization-analysis.md) §11 定义，本节与各文档只引用、不重复定义阈值。
> 细节与证据以来源文档为准；完成后勾选。

### 2.1 生产集群配置与优化（Worker 内存/资源）

来源：[worker-memory-and-optimization.md](worker-memory-and-optimization.md)（根因见第一部分，清单见第二部分）

**P0**

- [ ] 开启 `worker.pushdata.mergeBuffer`（纯配置，缓解小 push 钉 chunk 的堆外碎片；上游 CELEBORN-2152 已合入 0.6.2）
- [ ] 修正 `resume(0.2) < readBufferRatio(0.35)` 配置冲突（纯配置，消除「永远无法 resume」风险）
- [ ] `ReadBufferDispatcher` 误用 pooled heap buffer → 一行改 `directBuffer`（读 buffer 池从堆内挪回堆外配额体系；main 未修，可提 JIRA）

**P1**

- [ ] onTrim 加排序/限量/closed 剔除：泄压效率↑、小写 IO 放大↓（main 未做）
- [ ] cleanup 单 Runnable → 分片到 64 线程池：大批量过期清理吞吐数量级提升（main 未做）
- [ ] per-writer 轻量化组合拳（3.4KB→~1.5KB）：113K writer 场景省 ~200MB 堆，部分跟踪上游 PR #3825

**P2**（归组，逐项见第二部分 §4.2/§5/§6）：lazy takeBuffer + idle flush、metrics 写入侧固定 flusher key + metricName 缓存、committedFileInfos dirty-only 落盘、fetch/push 应答 header duplicate churn（~11% alloc）。

另：CELEBORN-2301（fetch 零拷贝）上游已合入、生产 0.6.2.2 未含 → [ ] 待回 port。

### 2.2 社区贡献：Metrics/Grafana

来源：[celeborn-community-and-ci.md](celeborn-community-and-ci.md) §3

**P0（确定性 bug，1~10 行改动，适合第一批 PR）**

- [ ] P0-1 `addHistogram` 用裸 name 注册进 Dropwizard，所有 label 变体共享同一 Histogram，且 `removeMetric` 永远删不掉（AbstractSource.scala:208-215，1 行修）
- [ ] P0-2 `RpcMetricsTracker.close()` 移除 gauge/timer 时缺 `NAME_LABEL`，key 不匹配 → 移除是空操作，dispatcher/endpoint 生命周期泄漏
- [ ] P0-3 `getAllMetricsNum` 漏统计 histogram，容量告警数值偏低（补 `namedHistogram.size()`）
- [ ] P0-4 Grafana 面板引用不存在的 `metrics_LocalFetchChunkTime_Max`，永久 No data → 改 `metrics_FetchLocalChunkTime_Max`
- [ ] P0-5 `docs/monitoring.md` 记录幽灵指标 `RatisApplyCompletedIndexDiff`（代码中只有 `RatisApplyCompletedIndex`）：删文档行 + 删面板（或真正实现 diff 指标）

**P1**（9 项，归组）：杠杆最高 —— 扩展 `dev/lint_grafana.py`（补 unit/description/gridPos 检查 + pin CI linter 版本）；其余含 3 组复制粘贴重复面板、RPC timer 单位 ns↔ms 显示差 10⁶、172/194 面板缺 role 过滤、命名三种风格并存、`MessageWithHeader` 裸字符串等，逐项见 §3.2。

**P2**（6 个设计级缺陷，归组）：`getMetrics` 破坏性读取、无界 in-flight timer map、metricsCleaner 单线程池泄漏、label 值未转义、client 无 MetricsSystem、Quota/S3/OSS 无指标 —— 均建议先发 dev@ 讨论再动手。

### 2.3 社区贡献：Auth/SASL（11 个 PR 候选）

来源：[celeborn-community-and-ci.md](celeborn-community-and-ci.md) §4。推荐起步顺序：PR-2 → PR-1 → PR-3。

- [ ] PR-2 在 worker replicate port 启用 SASL（Worker.scala:251-252 一行漏传 `getServerBootstraps`，端到端可复现的鉴权绕过）
- [ ] PR-1 修复 checkAuth 在 clientId 为 null 时 fail-open（整个按应用鉴权模型的根，17 处调用；依赖 PR-2 先行）
- [ ] PR-3 增加 auth metrics + 失败日志（纯增量、零兼容风险，运维刚需）

其余 8 项（PR-4~PR-11：补 auth 文档消除 7 个孤儿配置、SASL negotiation timeout 死字段、`SaslTimeoutException` 修或删、ApplicationLost 时清除 worker secret、auth 测试套件 15 项、registration secret 校验、SASL 机制可配置化 CELEBORN-1158、secret 查询移出 Netty event loop）逐项见 §4.2/§4.3。

### 2.4 社区贡献：代码 TODO（6 项推荐）

来源：[celeborn-community-and-ci.md](celeborn-community-and-ci.md) §2（其余 28 项暂缓见 §2.3）

- [ ] ⭐⭐⭐ 修复 `ThreadStackTrace.toString` 丢弃 `blockedByThreadId` 结果的真 bug（顺手兑现 TODO，十几行，<0.5 天，评审风险最低，建议第一发）
- [ ] ⭐⭐⭐ `TagsManager.getTagStore` 每次调用重建 ConcurrentHashMap（性能 + 双 store 一致性隐患，1~2 天；动手前先邮件列表确认方案 A/B）
- [ ] ⭐⭐⭐ 服务端校验 `AuthenticationInitiationRequest`（安全加固：版本/authEnabled/机制列表均未校验，1~2 天）
- [ ] ⭐⭐（归组）：清理 `RegistrationClientBootstrap` 过时 TODO（可与上一项同 PR）、Kerberos name rules 可配置化（`SpnegoAuthenticationHandler` 硬编码 `"DEFAULT"`）、`shuffleToCompletedMappers` 换原生 Int→Int 结构（备选，需 JMH 证明收益）

### 2.5 社区贡献：CI 优化

来源：[celeborn-community-and-ci.md](celeborn-community-and-ci.md) §5.4

- [ ] C1 MiniClusterFeature 固定 sleep → 条件等待（收益最大：每触集群 suite 省 15-20s，单次 CI 省 5-7 分钟）
- [ ] C2 修复 LifecycleManager 泄漏（改动最小：消 1/3 日志噪声（591×2 条 ERROR），适合作为首个贡献）
- [ ] C3 缓存 Maven 本体（CI 提速，~20 job × 25s~2min，有 cb086f2ca 先例背书，审议阻力最小）
- [ ] C4 修复 S3 e2e 测试在 CI 从未真正执行（7 个 s3 用例恒 CANCELED，正确性问题，建议先发 JIRA/邮件列表讨论）

次级候选（C5，见 §5.4）：Maven 并行构建（-T 1C）、ShuffleFallbackSuite 起停挪回 beforeAll/afterAll、UnregisterShuffleSuite eventually 窗口收紧、CommitFilesSuite 调小 retryWait、spark4 job 含 spark-3 模块单测。

来自 [celeborn-community-and-ci.md](celeborn-community-and-ci.md) §5.2（2026-10-29 Test-with-SBT 日志，job 绿色）：

- [ ] 🔴 ReusedExchangeSuite 期间 worker push server 出现 Netty ByteBuf LEAK（epoll 接收 buffer 未 release，疑似真 bug）：先加 `-Dio.netty.leakDetection.level=paranoid` 复跑该 suite 确认可复现性
- [ ] 升级弃用的 actions：`setup-java@v5`、checkout/cache 的 Node 24 兼容版本
- [ ] WorkerStatusTracker ERROR 消息以换行开头导致空首行（×14）：改单行格式，可作社区小 PR
- [ ] Jersey WADL WARNING ×39（每次起 HTTP 服务一条）：禁用 WadlFeature 或补 JAXB 依赖，消噪声

### 2.6 设计提案推进状态

| 提案 | 状态 | 下一步 |
|------|------|--------|
| [queryEndShuffleCleanup](issue-query-end-shuffle-cleanup.md)（CELEBORN-2465） | 仅提案，未落地代码 | 提 PR（opt-in 配置 + SQLExecutionEnd 监听） |
| [Overlapping 压缩/解压](overlapping-compression-decompression-design.md) | 调研与设计完成，无代码改动 | P0 ThreadSafety 改造起步 → P1 读路径 → P2 写路径 → P3 加密兼容/压测 |
| [自适应分区写并行度](dynamic-partition-write-parallelism-design.md) | Phase 1.3 已实现（分支 `parallel-partition-write`） | spark-it 集成测试 + 故障注入 + 真实集群灰度 |
| [CongestionControl App 粒度扩展](congestion-control-analysis-and-design.md) | 得物内部分支 `feature/app-level-congestion-control`（v0.6.2-based） | 内部推进 |
| [Batched Heartbeat Raft Writes](cip-master-ha-heartbeat-batch.md)（CELEBORN-2429） | CIP Draft（分支 `CELEBORN-2429`，默认关闭的纯增量设计） | 草案评审 → 实现 |

### 2.7 进行时：集群高峰期自动降级控制

来源：[cluster-degradation-control-design.md](cluster-degradation-control-design.md)（2026-10-27 立项）

- [ ] **生产观察期（→2026-11-13）**：只开 CongestionControl 开关、不配水位线（零限速风险），经现有 CongestionRelatives 大盘实测写入速率分布，产出水位线建议值（与开发并行）
- [ ] 开发（→11-06）→ 联调（→11-13）→ 测试（→11-20）→ 灰度（→12-04）→ 全量上线与调参（→12-18）
- Phase 2（磁盘水位直读 / master 中心化决策）为下一期演进，与本方案叠加

---

## 3. 文档维护约定

1. **新增**：先在上表对应分类登记（文档名 + 内容一句话 + 基线/日期），再写正文；文件名统一 kebab-case。
2. **头部必填**：每篇正文前 8 行内给出 `> 基线（版本/分支/commit）｜ 整理日期 ｜ 状态 ｜ 来源或合并说明`。
3. **合并优先于新增**：新内容若属于既有文档主题，改写原文档而非新建；合并后删除源文件。
4. **优先级单一来源**：P0/P1/P2 与阈值只在 `performance-optimization-analysis.md` §11 定义，其他文档引用不复述。
5. **行号必须带基线**：引用 `file:line` 时注明所在分支/commit；不同基线的文档禁止直接比对行号。
6. **语言**：面向社区提案的英文文档（`EN`）保持英文，其余用中文；术语与代码标识符保留原文。
