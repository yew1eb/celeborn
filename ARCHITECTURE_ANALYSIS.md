# Apache Celeborn 架构深度分析

> 基于版本 0.7.0-SNAPSHOT，整理日期 2026-08-05。

## 目录
1. [项目概述](#1-项目概述)
2. [整体架构](#2-整体架构)
3. [核心模块详解](#3-核心模块详解)
4. [数据流分析](#4-数据流分析)
5. [关键技术决策](#5-关键技术决策)
6. [架构挑战与改进方向](#6-架构挑战与改进方向)

---

## 1. 项目概述

Apache Celeborn 是一个高性能、弹性的分布式 Shuffle 服务，为 Apache Spark、Flink 等计算引擎提供统一的中间数据管理能力。

### 1.1 核心特性
- **计算存储分离**：支持计算节点与存储节点的独立扩展
- **多引擎支持**：Spark (2.4/3.x/4.x)、Flink (1.16-2.2)、MapReduce、Tez
- **高可用性**：基于 Raft 协议的 Master HA 机制
- **多级存储**：本地磁盘、HDFS、S3、OSS、内存存储
- **动态配置**：支持系统级、租户级、用户级的动态配置

### 1.2 技术栈
| 组件 | 技术 | 版本 |
|------|------|------|
| 构建工具 | Maven / SBT | 3.9.16 / 1.9.4 |
| 语言 | Java / Scala | 8/11/17/21 / 2.12.18 |
| RPC 框架 | Netty + gRPC | 4.2.10.Final / 1.44.0 |
| 共识协议 | Apache Ratis | 3.2.2 |
| 序列化 | Protocol Buffers | 3.25.5 |
| 存储后端 | LevelDB / RocksDB | 1.8 / 9.10.0 |

---

## 2. 整体架构

### 2.1 系统架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Apache Celeborn Cluster                          │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                         Master 集群 (HA)                            │  │
│  │  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐        │  │
│  │  │    Master    │◀───▶│    Master    │◀───▶│    Master    │        │  │
│  │  │   (Leader)   │ Raft│  (Follower)  │ Raft│  (Follower)  │        │  │
│  │  └──────┬───────┘     └──────────────┘     └──────────────┘        │  │
│  │         │                                                          │  │
│  │         │  元数据管理：Worker 注册、Slot 分配、应用生命周期            │  │
│  │         ▼                                                          │  │
│  │  ┌─────────────────────────────────────────────────────────────┐   │  │
│  │  │              HAMasterMetaManager / SingleMasterMetaManager   │   │  │
│  │  │              - Worker 状态管理                                │   │  │
│  │  │              - Shuffle 注册与注销                             │   │  │
│  │  │              - 配额管理                                       │   │  │
│  │  └─────────────────────────────────────────────────────────────┘   │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                    │                                       │
│         ┌──────────────────────────┼──────────────────────────┐            │
│         │                          │                          │            │
│         ▼                          ▼                          ▼            │
│  ┌──────────────┐          ┌──────────────┐          ┌──────────────┐     │
│  │    Worker    │          │    Worker    │          │    Worker    │     │
│  │      1       │◀────────▶│      2       │◀────────▶│      3       │     │
│  └──────┬───────┘          └──────┬───────┘          └──────┬───────┘     │
│         │                         │                         │              │
│  ┌──────┴───────┐          ┌──────┴───────┐          ┌──────┴───────┐     │
│  │ - PushServer │          │ - PushServer │          │ - PushServer │     │
│  │ - FetchServer│          │ - FetchServer│          │ - FetchServer│     │
│  │ - Replicate  │          │ - Replicate  │          │ - Replicate  │     │
│  │ - StorageMgr │          │ - StorageMgr │          │ - StorageMgr │     │
│  │ - MemoryMgr  │          │ - MemoryMgr  │          │ - MemoryMgr  │     │
│  └──────────────┘          └──────────────┘          └──────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ▲
                                       │ gRPC/Netty RPC
┌──────────────────────────────────────┼─────────────────────────────────────┐
│                     Compute Engine     │                                     │
│  ┌─────────────────────────────────────┴─────────────────────────────────┐  │
│  │                           Client 层                                    │  │
│  │  ┌─────────────────────┐         ┌─────────────────────────────────┐  │  │
│  │  │  LifecycleManager   │         │        ShuffleClient            │  │  │
│  │  │  (Driver/JM 端)      │         │    (Executor/TaskManager 端)     │  │  │
│  │  │                     │         │                                 │  │  │
│  │  │ - 元数据管理         │◀───────▶│ - Push/Fetch 数据               │  │  │
│  │  │ - Slot 申请/释放     │         │ - 失败重试处理                   │  │  │
│  │  │ - Commit 协调       │         │ - 压缩/解压                      │  │  │
│  │  └─────────────────────┘         └─────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 模块依赖关系

```
                    ┌─────────────────┐
                    │     Master      │
                    └────────┬────────┘
                             │ RPC
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│    Worker     │◀──▶│     Common    │◀──▶│    Client     │
└───────┬───────┘    └───────┬───────┘    └───────┬───────┘
        │                    │                    │
        │            ┌───────┴───────┐            │
        │            │               │            │
        └───────────▶│    Service    │◀───────────┘
                     │  (HTTP/RPC)   │
                     └───────────────┘
```

---

## 3. 核心模块详解

### 3.0 模块总览

| 模块 | 职责 | 深入阅读 |
|------|------|---------|
| `common/` | 共享基础设施：配置系统（`CelebornConf`）、Netty RPC 框架、网络协议与控制消息定义、指标系统 | [network-and-protocol-deep-dive.md](./network-and-protocol-deep-dive.md) |
| `master/` | 集群元数据管理、Slot 分配、Worker 生命周期管理、基于 Ratis 的 HA | [master-control-plane-deep-dive.md](./master-control-plane-deep-dive.md)、[master-ha-ratis-deep-dive.md](./master-ha-ratis-deep-dive.md) |
| `worker/` | 数据存储与传输、Push/Fetch 处理、内存与拥塞控制、多级存储后端 | [worker-storage-and-traffic-deep-dive.md](./worker-storage-and-traffic-deep-dive.md) |
| `client/` | 引擎无关的 Client 核心：`LifecycleManager`（控制面）与 `ShuffleClient`（数据面） | [client-control-plane-deep-dive.md](./client-control-plane-deep-dive.md)、[client-data-plane-deep-dive.md](./client-data-plane-deep-dive.md) |
| `service/` | HTTP REST 服务基类、认证、动态配置服务 | [service-and-security-deep-dive.md](./service-and-security-deep-dive.md) |
| `client-spark/` `client-flink/` `client-mr/` `client-tez/` | 各计算引擎的 Shuffle 接入层 | — |

### 3.1 Common 模块 (基础层)

**职责**：提供所有模块共享的基础设施

- `common/network/`：Netty 网络框架封装（TransportContext、连接池、协议编解码、SASL/SSL）
- `common/rpc/`：RPC 框架（`RpcEnv`/`RpcEndpoint` 抽象与 Netty 实现）
- `common/protocol/`：控制消息定义（`ControlMessages.scala`）与 Protobuf（`TransportMessages.proto`）
- `CelebornConf.scala`：配置系统（约 7000 行）

**关键技术点**：
- **配置系统**：`CelebornConf` 使用类型安全的配置项定义，支持动态更新
- **RPC 框架**：基于 Netty 的异步 RPC，支持 SASL 认证和 SSL 加密
- **网络协议**：自定义协议支持 Push/Fetch/Control 三种消息类型

### 3.2 Master 模块 (控制层)

**职责**：集群元数据管理、资源调度、Worker 管理

- `Master.scala`：主服务入口（约 1600 行）
- `SlotsAllocator.java`：Slot 分配算法（ROUNDROBIN / LOADAWARE）
- `clustermeta/`：元数据管理，含 `ha/`（基于 Apache Ratis 的 `HARaftServer` 与状态机）
- `quota/`、`tags/`：配额与 Worker 标签管理

HA 与 Raft 的详细分析见 [master-ha-ratis-deep-dive.md](./master-ha-ratis-deep-dive.md)。

**核心功能**：
1. **Worker 生命周期管理**
   - 注册/心跳/离线检测
   - 磁盘状态监控
   - 负载均衡调度

2. **Slot 分配策略**
   ```java
   // 两种分配策略（common/protocol/SlotsAssignPolicy.java）
   public enum SlotsAssignPolicy {
     ROUNDROBIN,  // 轮询
     LOADAWARE;   // 负载感知
   }
   ```

3. **HA 机制**
   - 基于 Apache Ratis 实现 Raft 共识
   - Leader 选举和日志复制
   - 自动故障转移

### 3.3 Worker 模块 (数据层)

**职责**：数据存储、数据传输、本地资源管理

- `Worker.scala`：Worker 主服务（约 1100 行）
- `Controller.scala` / `PushDataHandler.scala` / `FetchHandler.scala`：RPC 与数据请求处理
- `storage/`：存储管理（`StorageManager`、`PartitionDataWriter`、Flusher、DeviceMonitor）
- `memory/`、`congestcontrol/`：内存管理与拥塞控制

**存储架构**：
```
┌─────────────────────────────────────────────────────────────┐
│                      StorageManager                         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│  │ Local Disk  │ │    HDFS     │ │  S3 / OSS   │           │
│  │             │ │             │ │             │           │
│  │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │           │
│  │ │Flusher  │ │ │ │Flusher  │ │ │ │Flusher  │ │           │
│  │ │Thread   │ │ │ │Thread   │ │ │ │Thread   │ │           │
│  │ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │           │
│  │             │ │             │ │             │           │
│  │ FileWriter  │ │ FileWriter  │ │ FileWriter  │           │
│  └─────────────┘ └─────────────┘ └─────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

### 3.4 Client 模块 (接入层)

**职责**：为计算引擎提供 Shuffle 接口

- `LifecycleManager.scala`：Driver/JobMaster 端控制面，管理 shuffle 元数据与 slot 分配（约 2100 行）
- `ChangePartitionManager.scala` / `CommitManager.scala` / `ReleasePartitionManager.scala` / `WorkerStatusTracker.scala`：分区变更、Commit 协调、分区释放、Worker 状态跟踪
- `client/src/main/java/org/apache/celeborn/client/`：`ShuffleClient` 接口与 `ShuffleClientImpl` 数据面实现（压缩、读写、失败重试）
- `commit/`：CommitHandler 体系（Reduce / MapPartition 两种实现）

详细分析见 [client-control-plane-deep-dive.md](./client-control-plane-deep-dive.md)。

**两种分区类型**：
1. **Reduce 分区**：传统 MapReduce 模型
2. **Map 分区**：支持 Flink 的 Pipeline Shuffle

### 3.5 Service 模块 (通用服务层)

**职责**：HTTP 服务、认证、动态配置

- `HttpService.scala` / `Service.scala`：HTTP 服务基类与服务接口
- `http/`：HTTP Server、认证处理与 REST API
- `service/config/`：动态配置服务（FS / DB 两种后端）

详细分析见 [service-and-security-deep-dive.md](./service-and-security-deep-dive.md)。

---

## 4. 数据流分析

### 4.1 Shuffle Write 流程

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
│ Executor │───▶│ShuffleClient │───▶│    Worker    │───▶│   Disk   │
└──────────┘    └──────────────┘    └──────────────┘    └──────────┘
     │                 │                   │
     │ 1. pushData()   │                   │
     │────────────────▶│                   │
     │                 │ 2. 路由到目标 Worker
     │                 │──────────────────▶│
     │                 │                   │ 3. 写入本地文件
     │                 │                   │─────────▶
     │                 │                   │
     │                 │ 4. 异步复制到 Replica
     │                 │◀──────────────────│
     │ 5. 返回确认     │                   │
     │◀────────────────│                   │
```

### 4.2 Shuffle Read 流程

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
│ Executor │───▶│ShuffleClient │───▶│    Worker    │───▶│   Disk   │
└──────────┘    └──────────────┘    └──────────────┘    └──────────┘
     │                 │                   │
     │ 1. 请求数据位置  │                   │
     │────────────────▶│                   │
     │                 │                   │
     │ 2. 返回 PartitionLocation           │
     │◀────────────────│                   │
     │                 │                   │
     │ 3. fetchChunk() │                   │
     │────────────────────────────────────▶│
     │                 │                   │ 4. 读取文件
     │                 │                   │─────────▶
     │ 5. 返回数据     │                   │
     │◀────────────────────────────────────│
```

### 4.3 容错机制（Fault Tolerant）

> 参考官方设计文档 `docs/developers/faulttolerant.md`。

**Push 失败 → Revive**

```
 PushData 失败（CPU 高负载 / 网络抖动 / JVM GC / Worker 失联）
      │
      ▼  不急于判定 Worker 离线，视为临时不可用
 ShuffleClientImpl.revive() → reviveBatch() 聚合批量请求
      │  RPC
      ▼
 LifecycleManager（ChangePartitionManager 批量处理）
      │  在不同 Worker 上申请新 (primary, replica) PartitionLocation
      ▼
 ShuffleClient 切换到新 location 续推（按 batch 粒度重推，数据不丢）

 PushMergedData 失败（all-or-nothing，整批同生共死）：
   拆包 → 逐 batch revive（新 location 可能散布多个 Worker）
        → 按目标 Worker 重新分组 → 拆成多个 PushMergedData 分别发送
```

**BatchRevive**：Worker 宕机时其上的所有 PartitionLocation 都要 revive，逐个 RPC 会形成打向 LifecycleManager 的 Revive 风暴；ShuffleClient 先把 revive 请求聚合，再批量发送。

**Fetch 失败重试**

```
 fetchChunk 失败
      │
      ▼  每个 replica 最多重试 celeborn.client.fetch.maxRetriesForEachReplica 次（默认 3）
 有副本 ──▶ 切换另一个 replica 重试（无副本则重试同一个）
      │
      ▼  超过上限
 放弃重试，抛出异常
```

**磁盘检查**

```
 Worker 周期性检查磁盘健康与用量（DeviceMonitor）
      │
      ├─ 健康检查失败 → 隔离磁盘，恢复健康前不再向其分配 slot
      │
      └─ 可用空间 < celeborn.worker.storage.disk.reserve.size（默认 5G）
           → 不再分配 slot
           → 对该磁盘上所有 PartitionLocation 触发 HARD_SPLIT，阻止文件继续增长
```

**Exactly-Once**

同一 batch 可能跨 PartitionLocation 分裂重复（ACK 丢失重推、primary 成功 replica 失败、推测执行重复推送），保证不丢不重：

```
 写侧：每个 batch 附加 (mapId, attemptId, batchId) 头部
       （ShuffleClientImpl.BATCH_HEADER_SIZE = 4×4 字节，batchId 在 map attempt 内唯一）
 LM  ：同一 partitionId 保留所有分裂的 PartitionLocation；
       每个分裂至少一个副本 commit 成功才开放读取；
       记录每个 mapId 最终成功的 attempt，只读该 attempt 的数据
 读侧：丢弃 batchId 已读过的数据 batch
```

### 4.4 Slots Allocation 流程

> 参考官方设计文档 `docs/developers/slotsallocation.md`；实现见 `master/src/main/java/org/apache/celeborn/service/deploy/master/SlotsAllocator.java`，入口 `Master.handleRequestSlots`（Master.scala:934）。

**分配链路**

```
 LifecycleManager ──RequestSlots RPC──▶ Master.handleRequestSlots
                                            │
                                            ▼
                               SlotsAllocator（celeborn.master.slot.assign.policy）
                                     ├─ ROUNDROBIN（默认）
                                     └─ LOADAWARE
                                            │
                                            ▼
                         Worker ReserveSlots：优先在 hinted 磁盘建 FileWriter，
                         该盘不可用时自选健康磁盘；显式指定 HDFS 的 slot 放 HDFS
```

**ROUNDROBIN**

```
 workers 视为数组，逐个 partition 轮转放置 1 个 slot，直至分完
 worker 有多块盘时，磁盘索引 = (单调递增序号 + 1) % diskCount
 开启复制时，replica 落在不同 Worker（可选 rackAware）
```

**LOADAWARE**

```
 1) 过滤：HEALTHY 且 actualUsableSpace > 0 的非 DFS 磁盘
    （availableTypes 仅 HDFS/S3/OSS、无可用盘或无可用 slot 时回退 ROUNDROBIN）
 2) 磁盘打分排序：
      avgFlushTime × flushTimeWeight（默认 0）
    + avgFetchTime × fetchTimeWeight（默认 1）
    + activeSlots  × activeSlotsWeight（默认 0）
 3) 按 numDiskGroups（默认 5）把磁盘分组
 4) 组间梯度：快组比相邻慢组多分 (1 + diskGroupGradient)（默认 0.1）倍
    例：G5 分 100 → G4:110 → G3:121 → G2:133 → G1:146
 5) 组内按 usableSlots 比例分摊：
      usableSlots = 可用空间 / 平均 partition 大小 − 已用 slots
```

相关配置：`celeborn.master.slot.assign.policy`、`celeborn.master.slot.assign.loadAware.numDiskGroups`、`.diskGroupGradient`、`.flushTimeWeight`、`.fetchTimeWeight`、`.activeSlotsWeight`、`celeborn.master.slot.assign.extraSlots`（默认 2）、`.maxWorkers` / `.minWorkers`。

### 4.5 Traffic Control 流程

> 参考官方设计文档 `docs/developers/trafficcontrol.md`；目标：不损伤性能的前提下防 Worker OOM 并兼顾公平，手段为 Back Pressure + Congestion Control。

**Back Pressure（基础流控，不可关闭）**

```
        MemoryManager 高频检查 direct memory 使用率
                          │
  ┌─────────────┐  ≥ 0.85  ▼
  │ NONE_PAUSED │ ──────▶ PUSH_PAUSED（暂停接收 ShuffleClient 数据，强制 flush）
  └─────────────┘              │ ≥ 0.95
      ▲                        ▼
      │              PUSH_AND_REPLICATE_PAUSED（同时暂停接收副本数据，强制 flush）
      │                        │
      └────── 使用率 < 0.70（Resume）◀──────────────┘

 水位配置：
   celeborn.worker.directMemoryRatioToPauseReceive   默认 0.85
   celeborn.worker.directMemoryRatioToPauseReplicate 默认 0.95
   celeborn.worker.directMemoryRatioToResume         默认 0.7
```

**Congestion Control（可选，`celeborn.worker.congestionControl.enabled`）**

```
 Worker 侧：TimeSlidingHub 滑窗内按 UserIdentifier 统计推送字节
      │
      ▼  超 High Watermark（diskBuffer / userProduceSpeed 两级水位）
 CongestionController 选出占用超过均值的用户，下发 CongestionControl 消息
      │
      ▼
 ShuffleClient 侧（类 TCP 控速）：
   Slow Start（低速起步、快速增长）─▶ 达阈值 ─▶ Congestion Avoidance（缓慢增长）
        ▲                                              │
        └──────────── 收到 CongestionControl ──────────┘
```

相关配置：`celeborn.worker.congestionControl.sample.time.window`、`.diskBuffer.low|high.watermark`、`.userProduceSpeed.low|high.watermark`、`.workerProduceSpeed.low|high.watermark`。现状调研与 App 粒度扩展设计见 [congestion-control-analysis-and-design.md](./congestion-control-analysis-and-design.md)。

---

## 5. 关键技术决策

### 5.1 为什么使用 Netty 而不是 gRPC？

**设计考虑**：
- **性能**：Netty 提供更细粒度的控制，支持零拷贝
- **灵活性**：自定义协议优化 Shuffle 场景
- **兼容性**：历史版本使用 Netty，保持向后兼容

Netty 协议分层、帧格式与编解码的详细分析见 [network-and-protocol-deep-dive.md](./network-and-protocol-deep-dive.md)。

### 5.2 存储后端设计

**多级存储架构**：

| 存储类型 | 使用场景 | 特点 |
|---------|---------|------|
| Local Disk | 默认存储 | 低延迟，高吞吐 |
| HDFS | 大数据量 | 高可靠，自动扩展 |
| S3/OSS | 云端部署 | 成本优化，无限容量 |
| Memory | 小数据加速 | 超低延迟 |

### 5.3 内存管理策略

```java
// MemoryManager 的三种服务状态（worker/memory/MemoryManager.java）
public enum ServingState {
  NONE_PAUSED,                 // 正常服务
  PUSH_PAUSED,                 // 暂停 Push
  PUSH_AND_REPLICATE_PAUSED    // 暂停 Push 和复制
}

// 内存使用分级
1. Read Buffer     - 数据读取缓冲
2. Sort Memory     - 排序操作内存
3. Disk Buffer     - 磁盘写入缓冲
4. Netty Memory    - 网络传输内存
```

### 5.4 配置管理设计

**三层配置体系**：
```
┌─────────────────────────────────────────────┐
│           Configuration Layers              │
├─────────────────────────────────────────────┤
│ 1. TENANT_USER  │ 用户级配置 (最高优先级)   │
├─────────────────────────────────────────────┤
│ 2. TENANT       │ 租户级配置               │
├─────────────────────────────────────────────┤
│ 3. SYSTEM       │ 系统默认配置             │
└─────────────────────────────────────────────┘
```

---

## 6. 架构挑战与改进方向

### 6.1 当前架构挑战

#### 6.1.1 代码复杂度

| 文件 | 行数 | 问题 |
|-----|------|------|
| Master.scala | ~1639 | 职责过多，需要拆分 |
| Worker.scala | ~1137 | 混合了太多子系统管理 |
| CelebornConf.scala | ~7078 | 配置过于集中，难以维护 |
| ShuffleClientImpl.java | ~2201 | 状态管理复杂 |
| LifecycleManager.scala | ~2105 | 控制面逻辑高度集中 |

#### 6.1.2 测试覆盖

- 单元测试相对完善
- 集成测试依赖外部组件（Spark/Flink）
- 缺少压力测试和混沌测试

#### 6.1.3 文档完整性

- 配置文档自动生成，但缺少场景化指南
- REST API 文档需要更新
- 缺少深度原理文档

## 7. 附录

### 7.1 关键配置文件

```
conf/
├── celeborn-defaults.conf          # 主配置
├── celeborn-env.sh                 # 环境变量
├── log4j2.xml                      # 日志配置
└── metrics.properties              # 指标配置
```

### 7.2 重要工具脚本

```
bin/
├── celeborn-class                  # 类加载器
└── celeborn-ratis                  # Ratis shell 工具

sbin/
├── start-master.sh                 # 启动 Master
├── start-worker.sh                 # 启动 Worker
└── stop-all.sh                     # 停止服务

dev/
└── reformat                        # 代码格式化
```

