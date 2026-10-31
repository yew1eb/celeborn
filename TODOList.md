# Celeborn 调研知识库 · 索引

> 本仓库根目录是 Apache Celeborn 的调研 / 分析工作区。**本文件是唯一入口**：按目的导航 + 文档地图 + 维护约定。
> 基线：Apache Celeborn 0.7.0-SNAPSHOT（分支 `ai-1`）｜ 最后更新：2026-10-31
> 新增、合并或删除文档前，请先读 §2 维护约定。

---

## 0. 按目的导航

| 我想… | 去这里 |
|---|---|
| 快速了解 Celeborn 整体架构 | [ARCHITECTURE_ANALYSIS.md](ARCHITECTURE_ANALYSIS.md)（约 30 分钟） |
| 看某个模块**源码级**怎么跑 | §1.1 源码阅读系列（10 篇，按模块划分，均带行号） |
| 找能改的代码 / 能提社区的 PR | [performance-optimization-analysis.md](performance-optimization-analysis.md) §11 优先级矩阵 + [celeborn-community-and-ci.md](celeborn-community-and-ci.md) §2~§4 |
| 搞懂 Spark 侧读写**到底慢在哪** | [celeborn-shuffle-pipeline.md](celeborn-shuffle-pipeline.md)（流程图 + 第四部分线程模型与指标口径） |
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

### 1.3 Spark 侧链路与指标（1 篇）

| 文档 | 内容 | 基线 / 更新 |
|---|---|---|
| [celeborn-shuffle-pipeline.md](celeborn-shuffle-pipeline.md) | 读写链路 Mermaid 流程图（14 张）+ 线程模型与指标口径：写侧拓扑/泳道/三级反压/CPU 争抢/耗时归属，读侧拓扑/泳道/失败重试/耗时归属，第三部分与 Spark 原生 SortShuffleWriter、BlockStoreShuffleReader 对比，第四部分 DataPusher/WorkerPartitionReader 线程模型、生产指标归因、Write/Read Times 逐字段审计与缺口（W1-W7/R1-R3/D1-D2） | `celeborn-ai` 工作树 / 2026-08-05；第四部分基于 `parallel-partition-write` 分支 / 2026-08-09~10 |

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

## 2. 文档维护约定

1. **新增**：先在上表对应分类登记（文档名 + 内容一句话 + 基线/日期），再写正文；文件名统一 kebab-case。
2. **头部必填**：每篇正文前 8 行内给出 `> 基线（版本/分支/commit）｜ 整理日期 ｜ 状态 ｜ 来源或合并说明`。
3. **合并优先于新增**：新内容若属于既有文档主题，改写原文档而非新建；合并后删除源文件。
4. **优先级单一来源**：P0/P1/P2 与阈值只在 `performance-optimization-analysis.md` §11 定义，其他文档引用不复述。
5. **行号必须带基线**：引用 `file:line` 时注明所在分支/commit；不同基线的文档禁止直接比对行号。
6. **语言**：面向社区提案的英文文档（`EN`）保持英文，其余用中文；术语与代码标识符保留原文。
