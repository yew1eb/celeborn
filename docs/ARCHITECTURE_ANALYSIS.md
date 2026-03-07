# Apache Celeborn 架构深度分析

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
| 构建工具 | Maven / SBT | 3.9.12 / 1.9.4 |
| 语言 | Java / Scala | 8/11/17/21 / 2.12.18 |
| RPC 框架 | Netty + gRPC | 4.2.10.Final / 1.44.0 |
| 共识协议 | Apache Ratis | 3.2.1 |
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

### 3.1 Common 模块 (基础层)

**职责**：提供所有模块共享的基础设施

```
common/src/main/
├── java/org/apache/celeborn/
│   ├── client/                 # 客户端接口定义
│   │   ├── ShuffleClient.java
│   │   ├── ShuffleClientImpl.java
│   │   ├── compress/           # 压缩算法 (LZ4, Zstd)
│   │   ├── read/               # 数据读取
│   │   └── write/              # 数据写入
│   └── common/
│       ├── network/            # Netty 网络框架封装
│       │   ├── TransportContext.java
│       │   ├── client/         # 客户端连接管理
│       │   ├── server/         # 服务端处理器
│       │   ├── protocol/       # 通信协议定义
│       │   ├── sasl/           # SASL 认证
│       │   └── ssl/            # SSL/TLS 加密
│       ├── meta/               # 元数据对象
│       └── protocol/           # 协议常量定义
└── scala/org/apache/celeborn/
    ├── common/
    │   ├── CelebornConf.scala  # 配置系统 (5000+ 行)
    │   ├── rpc/                # RPC 框架
    │   │   ├── RpcEnv.scala    # RPC 环境
    │   │   ├── RpcEndpoint.scala
    │   │   └── netty/          # Netty RPC 实现
    │   ├── metrics/            # 指标系统
    │   ├── protocol/message/   # 控制消息定义
    │   └── util/               # 工具类
    └── reflect/                # 反射工具
```

**关键技术点**：
- **配置系统**：`CelebornConf` 使用类型安全的配置项定义，支持动态更新
- **RPC 框架**：基于 Netty 的异步 RPC，支持 SASL 认证和 SSL 加密
- **网络协议**：自定义协议支持 Push/Fetch/Control 三种消息类型

### 3.2 Master 模块 (控制层)

**职责**：集群元数据管理、资源调度、Worker 管理

```
master/src/main/scala/
└── org/apache/celeborn/service/deploy/master/
    ├── Master.scala                    # 主服务入口 (1000+ 行)
    ├── MasterArguments.scala           # 命令行参数
    ├── MasterSource.scala              # 指标定义
    ├── SlotsAllocator.scala            # Slot 分配算法
    ├── clustermeta/                    # 元数据管理
    │   ├── AbstractMetaManager.scala
    │   ├── SingleMasterMetaManager.scala
    │   └── ha/                         # HA 实现
    │       ├── HAMasterMetaManager.scala
    │       ├── HARaftServer.scala      # Raft 服务端
    │       └── StateMachine.scala      # 状态机
    ├── quota/                          # 配额管理
    │   ├── QuotaManager.scala
    │   └── QuotaStatus.scala
    └── tags/                           # Worker 标签管理
        └── TagsManager.scala
```

**核心功能**：
1. **Worker 生命周期管理**
   - 注册/心跳/离线检测
   - 磁盘状态监控
   - 负载均衡调度

2. **Slot 分配策略**
   ```scala
   // 两种分配策略
   object SlotsAssignPolicy extends Enumeration {
     val ROUND_ROBIN = Value  // 轮询
     val LOADAWARE = Value    // 负载感知
   }
   ```

3. **HA 机制**
   - 基于 Apache Ratis 实现 Raft 共识
   - Leader 选举和日志复制
   - 自动故障转移

### 3.3 Worker 模块 (数据层)

**职责**：数据存储、数据传输、本地资源管理

```
worker/src/main/scala/
└── org/apache/celeborn/service/deploy/worker/
    ├── Worker.scala                    # Worker 主服务 (1000+ 行)
    ├── Controller.scala                # RPC 请求处理器
    ├── PushDataHandler.scala           # Push 数据处理
    ├── FetchHandler.scala              # Fetch 数据处理
    ├── WorkerSource.scala              # 指标定义
    ├── congestcontrol/                 # 拥塞控制
    │   ├── CongestionController.scala
    │   └── TimeSlidingHub.scala
    ├── memory/                         # 内存管理
    │   ├── MemoryManager.scala
    │   ├── ReadBufferDispatcher.scala
    │   └── ChannelsLimiter.scala
    ├── storage/                        # 存储管理
    │   ├── StorageManager.scala        # 存储管理器
    │   ├── PartitionDataWriter.scala
    │   ├── PartitionDataReader.scala
    │   ├── Flusher.scala               # 刷盘策略
    │   └── DeviceMonitor.scala         # 磁盘监控
    └── shuffledb/                      # Shuffle DB
        ├── DB.scala                    # LevelDB/RocksDB 封装
        └── DBProvider.scala
```

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

```
client/src/main/scala/
└── org/apache/celeborn/client/
    ├── LifecycleManager.scala          # 生命周期管理
    ├── ChangePartitionManager.scala    # 分区变更处理
    ├── CommitManager.scala             # Commit 协调
    ├── ReleasePartitionManager.scala   # 分区释放
    ├── WorkerStatusTracker.scala       # Worker 状态跟踪
    └── commit/                         # Commit 处理器
        ├── CommitHandler.scala
        ├── MapPartitionCommitHandler.scala
        └── ReducePartitionCommitHandler.scala
```

**两种分区类型**：
1. **Reduce 分区**：传统 MapReduce 模型
2. **Map 分区**：支持 Flink 的 Pipeline Shuffle

### 3.5 Service 模块 (通用服务层)

**职责**：HTTP 服务、认证、动态配置

```
service/src/main/scala/
└── org/apache/celeborn/server/common/
    ├── HttpService.scala               # HTTP 服务基类
    ├── Service.scala                   # 服务接口
    ├── http/                           # HTTP 服务
    │   ├── HttpServer.scala
    │   ├── authentication/             # 认证处理
    │   └── api/                        # REST API
    └── service/
        ├── config/                     # 动态配置
        │   ├── ConfigService.scala
        │   ├── FsConfigServiceImpl.scala
        │   └── DbConfigServiceImpl.scala
        └── store/                      # 元数据存储
```

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

### 4.3 容错机制

```
┌────────────────────────────────────────────────────────────────┐
│                        容错策略                                 │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  ┌─────────────────┐      ┌─────────────────┐                 │
│  │   Push 失败     │      │   Fetch 失败    │                 │
│  └────────┬────────┘      └────────┬────────┘                 │
│           │                        │                          │
│           ▼                        ▼                          │
│  ┌─────────────────┐      ┌─────────────────┐                 │
│  │ 1. 重试到 Replica│      │ 1. 标记 Worker   │                 │
│  │ 2. 触发 Revive  │      │ 2. 切换 Replica  │                 │
│  │ 3. 重新分配 Slot│      │ 3. 报告 Master   │                 │
│  └─────────────────┘      └─────────────────┘                 │
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                    Worker 离线                           │  │
│  │  - Master 检测心跳超时                                   │  │
│  │  - 触发 WorkerLost 事件                                  │  │
│  │  - 重新分配该 Worker 的所有 Partition                    │  │
│  └─────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

---

## 5. 关键技术决策

### 5.1 为什么使用 Netty 而不是 gRPC？

**设计考虑**：
- **性能**：Netty 提供更细粒度的控制，支持零拷贝
- **灵活性**：自定义协议优化 Shuffle 场景
- **兼容性**：历史版本使用 Netty，保持向后兼容

**协议分层**：
```
┌─────────────────────────────────────────┐
│            Application Layer            │
│     (PushData / FetchChunk / RPC)      │
├─────────────────────────────────────────┤
│           Protocol Buffers              │
│         (Message Serialization)        │
├─────────────────────────────────────────┤
│              Netty Codec                │
│      (FrameDecoder / MessageEncoder)   │
├─────────────────────────────────────────┤
│           TCP / SSL Channel            │
└─────────────────────────────────────────┘
```

### 5.2 存储后端设计

**多级存储架构**：

| 存储类型 | 使用场景 | 特点 |
|---------|---------|------|
| Local Disk | 默认存储 | 低延迟，高吞吐 |
| HDFS | 大数据量 | 高可靠，自动扩展 |
| S3/OSS | 云端部署 | 成本优化，无限容量 |
| Memory | 小数据加速 | 超低延迟 |

### 5.3 内存管理策略

```scala
// MemoryManager 的三种服务状态
object ServingState extends Enumeration {
  val SERVING = Value                    // 正常服务
  val PUSH_PAUSED = Value                // 暂停 Push
  val PUSH_AND_REPLICATE_PAUSED = Value  // 暂停 Push 和复制
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
| Master.scala | ~1200 | 职责过多，需要拆分 |
| Worker.scala | ~1100 | 混合了太多子系统管理 |
| CelebornConf.scala | ~5000 | 配置过于集中，难以维护 |
| ShuffleClientImpl.java | ~800 | 状态管理复杂 |

#### 6.1.2 测试覆盖

- 单元测试相对完善
- 集成测试依赖外部组件（Spark/Flink）
- 缺少压力测试和混沌测试

#### 6.1.3 文档完整性

- 配置文档自动生成，但缺少场景化指南
- REST API 文档需要更新
- 缺少深度原理文档

### 6.2 改进方向

#### 6.2.1 短期改进 (3-6 个月)

1. **模块化重构**
   - 拆分 Master/Worker 的大类
   - 提取公共逻辑到 Service 层
   - 优化配置系统结构

2. **可观测性增强**
   - 完善 Metrics 指标
   - 增加分布式追踪支持
   - 优化日志结构化输出

3. **测试完善**
   - 增加模拟测试 (Mock-based)
   - 完善边界条件测试
   - 添加性能回归测试

#### 6.2.2 中期改进 (6-12 个月)

1. **存储层抽象**
   - 统一存储接口
   - 支持更多云存储
   - 实现分层存储策略

2. **性能优化**
   - 零拷贝优化
   - 内存池化
   - 批量操作优化

3. **多租户增强**
   - 资源隔离
   - 优先级调度
   - 配额实时监控

#### 6.2.3 长期演进 (12 个月+)

1. **云原生支持**
   - 服务网格集成
   - 自动扩缩容
   - 多集群联邦

2. **新协议支持**
   - HTTP/3 数据传输
   - RDMA 支持
   - 内核旁路技术

---

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
└── celeborn-config.sh              # 配置工具

sbin/
├── start-master.sh                 # 启动 Master
├── start-worker.sh                 # 启动 Worker
└── stop-all.sh                     # 停止服务

dev/
└── reformat                        # 代码格式化
```

### 7.3 参考资源

- **官方文档**: https://celeborn.apache.org/docs/
- **Jira**: https://issues.apache.org/jira/projects/CELEBORN
- **邮件列表**: dev@celeborn.apache.org
