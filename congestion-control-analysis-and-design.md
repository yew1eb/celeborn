# Celeborn Congestion Control 调研与 App 粒度扩展设计（合并版）

> **基于版本**：Apache Celeborn 0.7.0-SNAPSHOT（本仓库 `celeborn-ai`）。
> **整理日期**：2026-08-05。
> **来源**：本文档由两份文档合并而来——《Celeborn_CongestionControl_深度调研报告.md》（第一部分，现状源码讲解）与《Celeborn_AppCongestionControl_设计文档.md》（第二部分，App 粒度扩展设计方案）。
>
> **重要声明**：
> - 两份源文档均基于**得物内部版 celeborn-dewu**（`v0.6.2-based` 分支，开发分支 `feature/app-level-congestion-control`）。
> - **第一部分（现状调研）** 描述的功能在本仓库中存在，已对照 0.7.0-SNAPSHOT 源码逐条核实；与当前代码不符之处已修正并注明。
> - **第二部分（App 粒度设计）未落地于本仓库**。本仓库中不存在 `AppCongestionControlContext`/`AppTrafficQuota` 等类，该设计仅在得物内部分支上进行开发。

---

# 第一部分：CongestionControl 现状深度调研

本部分对 Apache Celeborn 的**拥塞控制（Congestion Control）**功能进行全面深入的分析，涵盖设计目标、架构设计、核心算法、配置参数、客户端集成、监控指标以及演进历史。

## 一、设计目标与背景

### 1.1 流量控制的整体架构

Celeborn Worker 的流量控制由两层机制组成：

| 机制 | 类型 | 能否禁用 | 作用层面 |
|------|------|----------|----------|
| **Back Pressure（背压）** | 被动触发，基于直接内存用量比 | 不能禁用 | 全局暂停 |
| **Congestion Control（拥塞控制）** | 主动抑制，基于速率和水位线 | 可选开启 | 用户级别差异化限速 |

### 1.2 设计目标

> **防止 Worker OOM，同时不牺牲性能；在保证性能的前提下实现公平性。**

- **Back Pressure** 是最后防线：当直接内存占用达到高阈值时，暂停接收数据并强制刷盘释放内存。
- **Congestion Control** 是主动预防机制：在内存压力出现时，识别并抑制消耗资源最多的用户，让低速用户不受影响，实现资源公平分配。

### 1.3 Back Pressure 三水位线（始终启用）

```
直接内存占用率
  0.95 ─── Pause Replicate：暂停接收 ShuffleClient 数据 + 主 Worker 副本数据，强制刷盘
  0.85 ─── Pause Receive：暂停接收 ShuffleClient 数据，强制刷盘
  0.70 ─── Resume：恢复接收（任一 Pause 触发后，需降至此阈值才恢复）
  0.00 ─── 正常
```

配置项：`celeborn.worker.directMemoryRatioToPauseReceive`（默认 0.85）、`celeborn.worker.directMemoryRatioToPauseReplicate`（默认 0.95）、`celeborn.worker.directMemoryRatioToResume`（默认 0.70）。已对照 `CelebornConf.scala:4303-4326` 核实。

---

## 二、Congestion Control 核心架构

### 2.1 代码目录结构

```
worker/src/main/java/.../worker/congestcontrol/
├── CongestionController.java        # 主控制器（单例，362 行）
├── UserCongestionControlContext.java # 单用户拥塞状态上下文（88 行）
├── BufferStatusHub.java              # 时间窗口字节统计（91 行）
├── TimeSlidingHub.java               # 通用时间滑动窗口（164 行）
└── UserBufferInfo.java               # 单用户缓冲区信息（41 行）

worker/src/test/java/.../worker/congestcontrol/
├── TestCongestionController.java     # 主要测试
└── TestTimeSlidingHub.java           # 滑动窗口测试

common/src/main/java/.../write/
└── SlowStartPushStrategy.java        # 客户端慢启动推送策略（180 行）
```

### 2.2 核心组件关系图

```
Worker
  └─ CongestionController (singleton)
       ├─ producedBufferStatusHub      ← 全局生产速率（所有用户汇总）
       ├─ consumedBufferStatusHub      ← 全局消费速率（数据刷盘）
       ├─ overHighWatermark: AtomicBoolean
       ├─ workerTrafficQuota           ← Worker 级水位线（来自 CelebornConf/动态配置）
       ├─ defaultUserQuota             ← 默认用户级水位线
       │
       ├─ userBufferStatuses: Map<UserIdentifier, UserBufferInfo>
       │       └─ UserBufferInfo
       │            ├─ timestamp       ← 最后活跃时间
       │            └─ bufferStatusHub ← 该用户的生产速率时间窗口
       │
       └─ userCongestionContextMap: Map<UserIdentifier, UserCongestionControlContext>
               └─ UserCongestionControlContext
                    ├─ congestionControlFlag: volatile boolean
                    ├─ userTrafficQuota       ← 该用户的个人水位线（支持动态配置）
                    ├─ userBufferInfo         ← 指向 userBufferStatuses 中的条目
                    └─ workerBufferStatusHub  ← 引用全局 producedBufferStatusHub
```

### 2.3 数据流向

```
ShuffleClient → PushDataHandler
                    │
                    ├─ fileWriter.getUserCongestionControlContext()
                    │        └─ UserCongestionControlContext
                    │
                    ├─ context.updateProduceBytes(numBytes)
                    │        ├─ 更新 UserBufferInfo（用户维度速率）
                    │        └─ 更新 producedBufferStatusHub（全局生产速率）
                    │
                    └─ isUserCongested(context)
                             └─ 返回 true → 响应状态码 PUSH_DATA_SUCCESS_PRIMARY_CONGESTED
                                           或 PUSH_DATA_SUCCESS_REPLICA_CONGESTED
```

---

## 三、核心算法详解

### 3.1 拥塞判断算法（CongestionController.isUserCongested）

```java
public boolean isUserCongested(UserCongestionControlContext ctx) {
    // 没有任何活跃用户，直接放行
    if (userBufferStatuses.isEmpty()) return false;

    long userProduceSpeed = getUserProduceSpeed(ctx.getUserBufferInfo());
    UserTrafficQuota quota = ctx.getUserTrafficQuota();

    // ① 全局超过高水位线时：抑制生产速率超过"平均潜在速率"的用户
    if (overHighWatermark.get()) {
        long avgSpeed = getPotentialProduceSpeed();  // 全局生产总速率 / 活跃用户数
        if (userProduceSpeed > avgSpeed) {
            return true;  // 直接返回拥塞，不更新 congestionControlFlag
        }
    }

    // ② 用户级水位线检查：独立于全局状态
    if (userProduceSpeed > quota.userProduceSpeedHighWatermark()) {
        ctx.onCongestionControl();   // 设置 congestionControlFlag = true
    } else if (ctx.inCongestionControl()
        && userProduceSpeed < quota.userProduceSpeedLowWatermark()) {
        ctx.offCongestionControl();  // 设置 congestionControlFlag = false（迟滞恢复）
    }

    return ctx.inCongestionControl();
}
```

已对照 `CongestionController.java:149-186` 核实，与源码一致。

**迟滞（Hysteresis）特性**：进入拥塞用高水位，退出拥塞用低水位，防止频繁抖动。

**两类触发路径**：

| 触发路径 | 条件 | 特点 |
|----------|------|------|
| 全局路径 | `overHighWatermark=true` 且用户速率 > 全局平均速率 | 即时生效，不修改 `congestionControlFlag` |
| 用户路径 | 用户速率超出用户级高水位线 | 有迟滞，修改 `congestionControlFlag` |

### 3.2 全局水位线检查（定时任务，默认每 10ms）

```java
protected void checkCongestion() {
    long pendingConsume = getTotalPendingBytes();         // MemoryManager.getMemoryUsage()
    long workerProduceSpeed = producedBufferStatusHub.avgBytesPerSec();

    // 同时低于低水位线 → 退出拥塞
    if (pendingConsume < workerTrafficQuota.diskBufferLowWatermark()
     && workerProduceSpeed < workerTrafficQuota.workerProduceSpeedLowWatermark()) {
        overHighWatermark.compareAndSet(true, false);
        return;
    }

    // 任一超过高水位线 → 进入拥塞，触发内存 trim
    if ((pendingConsume > workerTrafficQuota.diskBufferHighWatermark()
      || workerProduceSpeed > workerTrafficQuota.workerProduceSpeedHighWatermark())
     && overHighWatermark.compareAndSet(false, true)) {
        // log
    }

    if (overHighWatermark.get()) {
        trimMemoryUsage();   // MemoryManager.trimAllListeners()
    }
}
```

已对照 `CongestionController.java:264-287` 核实。

**注意**：退出拥塞需要**两个条件同时满足**（AND），进入拥塞只需**一个条件满足**（OR），这是非对称设计，确保保守退出。

### 3.3 平均潜在速率计算

```java
// 全局生产总速率 / 活跃用户数 = 每用户"公平份额"
public long getPotentialProduceSpeed() {
    if (userBufferStatuses.size() == 0) return 0;
    return producedBufferStatusHub.avgBytesPerSec() / userBufferStatuses.size();
}

// 全局消费总速率 / 活跃用户数（用于 Gauge 指标上报）
public long getPotentialConsumeSpeed() {
    if (userBufferStatuses.size() == 0) return 0;
    return consumedBufferStatusHub.avgBytesPerSec() / userBufferStatuses.size();
}
```

### 3.4 时间滑动窗口（TimeSlidingHub / BufferStatusHub）

**结构**：
- 内部维护 `LinkedBlockingDeque<Pair<Long, N>>`（时间戳 → 节点）
- 每个节点代表 **1 秒**的聚合数据（`intervalPerBucketInMills = 1000ms`）
- 窗口大小默认 10 秒，最多保留 10 个桶

**avgBytesPerSec 计算**：
```java
// totalBytes * 1000 / (bucketCount * 1000ms)
// = totalBytes / bucketCount    (bytes/sec)
return currentNumBytes * 1000 / ((long) sumInfo.getRight() * intervalPerBucketInMills);
```

**线程安全**：`sum()` 和 `add()` 均为 `synchronized` 方法；字节计数使用 `LongAdder`。

---

## 四、客户端慢启动策略（SlowStartPushStrategy）

客户端接收到拥塞控制信号后，通过类 TCP 的拥塞控制算法调整推送速率。

### 4.1 状态机

```
                     ┌─────────────────────────────┐
                     │         慢启动阶段            │
                     │  currentMaxReqsInFlight < 阈值│
                     │  每次成功 +1（指数增长，每RTT翻倍）│
                     └──────────┬──────────────────┘
                                │ 达到 reqsInFlightBlockThreshold
                                ▼
                     ┌─────────────────────────────┐
                     │        拥塞避免阶段           │
                     │  每 RTT +1（线性增长）         │
                     └──────────┬──────────────────┘
                                │ 收到拥塞控制信号
                                ▼
                     ┌─────────────────────────────┐
                     │          拥塞响应             │
                     │  currentMaxReqsInFlight /= 2 │
                     │  重设阈值为当前值              │
                     │  若已为1，continueCongestedNum++ │
                     └──────────┬──────────────────┘
                                │ 恢复推送
                                ▼
                           回到慢启动阶段
```

### 4.2 Sleep 退避策略

```java
protected long getSleepTime(CongestControlContext context) {
    int currentMaxReqs = context.getCurrentMaxReqsInFlight();

    // 已达最大并发，无需等待
    if (currentMaxReqs >= maxInFlightPerWorker) return 0;

    long sleepInterval = initialSleepMills - 60L * currentMaxReqs;

    // 当 currentMaxReqs = 1（极度拥塞）时，每次持续拥塞额外加 1 秒
    if (currentMaxReqs == 1) {
        return Math.min(sleepInterval + continueCongestedNumber * 1000L, maxSleepMills);
    }

    return Math.max(sleepInterval, 0);
}
```

默认参数：
- `initialSleepMills`：从 `celeborn.client.push.slowStart.initialSleepTime` 读取（默认 500ms）
- `maxSleepMills`：从 `celeborn.client.push.slowStart.maxSleepTime` 读取（默认 2s）
- `maxInFlightPerWorker`：从 `celeborn.client.push.maxReqsInFlight.perWorker` 读取

> **修正说明**：原调研报告称读取 `celeborn.client.push.maxReqsInFlight`，该配置在 0.7.0 中已拆分为 `celeborn.client.push.maxReqsInFlight.total` 与 `celeborn.client.push.maxReqsInFlight.perWorker`；`SlowStartPushStrategy.java:111` 实际调用 `conf.clientPushMaxReqsInFlightPerWorker()`。

---

## 五、配置参数全览

### 5.1 Worker 侧配置

以下配置项均已对照 `CelebornConf.scala:4383-4471` 核实。

| 配置项 | 默认值 | 说明 | 引入版本 |
|--------|--------|------|----------|
| `celeborn.worker.congestionControl.enabled` | `false` | 是否启用拥塞控制 | 0.3.0 |
| `celeborn.worker.congestionControl.check.interval` | `10ms` | 全局水位线检查间隔 | 0.3.2 |
| `celeborn.worker.congestionControl.sample.time.window` | `10s` | 速率统计时间窗口 | 0.3.0 |
| `celeborn.worker.congestionControl.diskBuffer.high.watermark` | `Long.MAX_VALUE` | 磁盘缓冲区高水位（Bytes） | 0.3.0 |
| `celeborn.worker.congestionControl.diskBuffer.low.watermark` | `Long.MAX_VALUE` | 磁盘缓冲区低水位（Bytes） | 0.3.0 |
| `celeborn.worker.congestionControl.userProduceSpeed.high.watermark` | `Long.MAX_VALUE` | 用户生产速率高水位（Bytes/sec） | 0.6.0 |
| `celeborn.worker.congestionControl.userProduceSpeed.low.watermark` | `Long.MAX_VALUE` | 用户生产速率低水位（Bytes/sec） | 0.6.0 |
| `celeborn.worker.congestionControl.workerProduceSpeed.high.watermark` | `Long.MAX_VALUE` | Worker 生产速率高水位（Bytes/sec） | 0.6.0 |
| `celeborn.worker.congestionControl.workerProduceSpeed.low.watermark` | `Long.MAX_VALUE` | Worker 生产速率低水位（Bytes/sec） | 0.6.0 |
| `celeborn.worker.congestionControl.user.inactive.interval` | `10min` | 用户不活跃超时（超时后清理） | 0.3.0 |

> **注意**：所有水位线默认值为 `Long.MAX_VALUE`，即默认不触发对应的拥塞条件。至少需要配置磁盘缓冲水位线才能使拥塞控制生效。

**废弃配置映射**（0.3.0 → 0.6.0 重命名）：

| 旧配置（已废弃） | 新配置 |
|-----------------|--------|
| `celeborn.worker.congestionControl.low.watermark` | `diskBuffer.low.watermark` |
| `celeborn.worker.congestionControl.high.watermark` | `diskBuffer.high.watermark` |

### 5.2 客户端配置

| 配置项 | 说明 |
|--------|------|
| `celeborn.client.push.limit.strategy` | 推送策略：`SIMPLE`（默认）或 `SLOWSTART` |
| `celeborn.client.push.maxReqsInFlight.perWorker` | 单 Worker 最大并发请求数（0.7.0 起拆分；另有 `celeborn.client.push.maxReqsInFlight.total` 控制总并发，旧键 `celeborn.client.push.maxReqsInFlight` 已不在本仓库定义中） |
| `celeborn.client.push.slowStart.initialSleepTime` | 慢启动初始睡眠时间（默认 500ms） |
| `celeborn.client.push.slowStart.maxSleepTime` | 最大睡眠时间上限（默认 2s） |

### 5.3 动态配置支持

Worker 级配置（`workerTrafficQuota`）和用户级配置（`userTrafficQuota`）均支持通过 `ConfigService` 动态更新，无需重启：

```java
// 监听配置变化
configService.registerListenerOnConfigUpdate(this::updateQuota);

// updateQuota 实现
private void updateQuota() {
    workerTrafficQuota = configService.getSystemConfigFromCache().getWorkerTrafficQuota();
    for (UserCongestionControlContext ctx : userCongestionContextMap.values()) {
        ctx.updateUserTrafficQuota(
            configService.getTenantUserConfigFromCache(user.tenantId(), user.name())
                .getUserTrafficQuota());
    }
}
```

**配置优先级（高→低）**：租户用户级 → 租户级 → 系统级 → 静态配置

---

## 六、监控指标

### 6.1 Worker 侧 Metrics（WorkerSource）

以下指标名已对照 `WorkerSource.scala` 核实。

| Metric 名称 | 类型 | 含义 |
|------------|------|------|
| `PotentialConsumeSpeed` | Gauge | 全局消费速率 / 活跃用户数（bytes/sec） |
| `WorkerConsumeSpeed` | Gauge | Worker 全局消费速率（bytes/sec） |
| `UserProduceSpeed` | Gauge（带用户标签） | 各用户的生产速率（bytes/sec） |
| `IsHighWorkload` | Gauge（0/1） | Worker 是否处于高负载状态 |
| `PausePushDataStatus` | Gauge | 是否处于 Pause Receive 状态 |
| `PausePushDataAndReplicateStatus` | Gauge | 是否处于 Pause Replicate 状态 |

### 6.2 IsHighWorkload 判断逻辑（CELEBORN-2118，2025-08 引入）

```scala
// Worker.scala（0.7.0 中为 pattern match 实现，逻辑等价）
highWorkload = (
    CongestionController.instance().isOverHighWatermark()  ||
    memoryManager.servingState == PUSH_AND_REPLICATE_PAUSED ||
    memoryManager.servingState == PUSH_PAUSED               ||
    activeConnectionCount >= activeConnectionMax
)
```

该指标暴露为 Prometheus 指标 `metrics_IsHighWorkload_Value`，用于 Grafana 大盘展示。当 Master 感知到 Worker `IsHighWorkload=1` 时（CELEBORN-2066），在排除 Worker 数量超限时优先排除高负载 Worker。

### 6.3 状态码（PushDataHandler 返回给客户端）

| 状态码 | 值（0.7.0-SNAPSHOT） | 含义 |
|--------|----|------|
| `PUSH_DATA_SUCCESS_PRIMARY_CONGESTED` | 30 | 数据写入成功，但主节点被拥塞控制 |
| `PUSH_DATA_SUCCESS_REPLICA_CONGESTED` | 31 | 数据写入成功，但副本被拥塞控制 |
| `SOFT_SPLIT` | 22 | 触发软分裂 |
| `HARD_SPLIT` | 21 | 触发硬分裂 |

> **修正说明**：原调研报告记载状态码值为 31/32/13/11（对应 0.6.x 版本）；本仓库 `StatusCode.java:54-68` 中实际值为上表所列（30/31/22/21），已按当前代码修正。

客户端收到 `PRIMARY_CONGESTED` 或 `REPLICA_CONGESTED` 后，调用 `SlowStartPushStrategy.onCongestControl()` 执行速率回退。

---

## 七、用户生命周期管理

### 7.1 用户创建

首次推送数据时，`getUserBuffer()` 和 `getUserCongestionContext()` 通过 `ConcurrentHashMap.computeIfAbsent()` 懒创建用户条目，并注册 `UserProduceSpeed` Gauge 指标。

### 7.2 用户清理（防内存泄漏）

```java
// 定时任务，间隔 = userInactiveTimeMills（默认 10min）
private void removeInactiveUsers() {
    long now = System.currentTimeMillis();
    for (UserBufferInfo info : userBufferStatuses) {
        if (now - info.getTimestamp() >= userInactiveTimeMills) {
            userBufferStatuses.remove(user);
            userCongestionContextMap.remove(user);
            workerSource.removeGauge(USER_PRODUCE_SPEED, user.toMap());  // 清理 Metric
        }
    }
}
```

### 7.3 状态流转

```
用户首次 Push
    └─→ 创建 UserBufferInfo + UserCongestionControlContext
              │
              ▼
    每次 Push 调用 updateProduceBytes()
              │
              ├─ 速率 > 高水位 → onCongestionControl() → flag=true
              ├─ 全局超水位 + 速率超均值 → 直接返回 true（不修改 flag）
              └─ 速率 < 低水位 + flag=true → offCongestionControl() → flag=false
              │
    超过 userInactiveTimeMills 无 Push
              └─→ removeInactiveUsers() 清理条目 + Metric
```

---

## 八、与 Master 的联动

### 8.1 Worker 黑名单（CELEBORN-1782）

当 Worker 进入拥塞控制（`overHighWatermark=true`）时，该 Worker 会被加入 Master 的 blacklist，避免新 Shuffle 任务继续分配到该 Worker，从根本上减少压力。

### 8.2 高负载 Worker 排除策略（CELEBORN-2066）

Master 在需要排除 Worker 时，优先排除 `IsHighWorkload=1` 的 Worker，以保护整体集群稳定性。

---

## 九、关键 Issue 与演进历史

| Issue | Commit | 描述 |
|-------|--------|------|
| CELEBORN-61/62 | `c924a4ff0` | 客户端慢启动、拥塞避免、拥塞控制的首次实现 |
| CELEBORN-63 | `bb5a4d218` | 新增 CONGESTED 相关状态码 |
| CELEBORN-207 | `f88f5fcf5` | 支持网络拥塞控制；修复开启副本时 Master 可能漏掉拥塞状态的问题 |
| CELEBORN-227 | `bff6e91e0` | 支持多种 Push 策略（SIMPLE/SLOWSTART） |
| CELEBORN-342 | `798ff90bb` | 修复拥塞控制中平均生产字节数计算错误 |
| CELEBORN-524 | `181c1bfcd` | 修复 CongestionControl 过于频繁调用 ChannelsLimiter.onTrim 导致 CPU 卡顿问题 |
| CELEBORN-777 | `52dcd3b5d` | 修复 getPotentialConsumeSpeed 除零错误 |
| CELEBORN-1089 | `320714bf2` | 将 overHighWatermark 检查抽离为独立定时线程 |
| CELEBORN-1472 | `d362d9f75` | 减少 userBufferStatuses 调用次数，降低锁竞争 |
| CELEBORN-1487 Phase1 | `1d44e5fbf` | 新增用户级和 Worker 级生产速率水位线控制 |
| CELEBORN-1487 Phase2 | `7c9a008a1` | CongestionController 支持动态配置更新 |
| CELEBORN-1654 | `fdff49453` | 修复 TestCongestionController#testUserMetrics 偶发失败 |
| CELEBORN-1782 | `cec88b2de` | 拥塞中的 Worker 加入黑名单 |
| CELEBORN-1794 | `406ceb64c` | 修复 TestCongestionController 偶发失败 |
| CELEBORN-1930 | `15ea5d366` | 修复 HARD_SPLIT 在 PushMergedData 中的拥塞控制 NPE 问题 |
| CELEBORN-2066 | `8effb753f` | 排除 Worker 时优先排除高负载 Worker |
| CELEBORN-2082 | `92edae22d` | 新增高负载 Worker 排除日志 |
| CELEBORN-2118 | `a9490d6e2` | 引入 `IsHighWorkload` 指标，统一监控 Worker 过载状态 |

---

## 十、测试覆盖

### TestCongestionController.java 核心测试场景

| 测试方法 | 验证内容 |
|----------|----------|
| `testSingleUser` | 单用户：pending bytes 超阈值时触发拥塞，低于低水位时退出 |
| `testMultipleUsers` | 多用户公平：高速用户被抑制，低速用户不受影响 |
| `testUserMetrics` | 用户 Gauge 指标的创建与清理生命周期 |
| `testUserLevelTrafficQuota` | 用户级独立水位线生效（与全局水位线解耦） |
| `testWorkerLevelTrafficQuota` | Worker 级生产速率水位线触发全局拥塞 |
| `testDynamicConfiguration` | 动态更新配置后拥塞行为变化 |
| `testUpdateProduceBytes` | 字节数更新同步写入用户级和全局 BufferStatusHub |

### 典型配置（测试中使用）

```java
diskBuffer.high.watermark  = 1000 bytes
diskBuffer.low.watermark   = 500  bytes
userProduceSpeed.high      = 20000 bytes/sec
userProduceSpeed.low       = 10000 bytes/sec
workerProduceSpeed.high    = 20000 bytes/sec
workerProduceSpeed.low     = 10000 bytes/sec
```

---

## 十一、关键文件速查

| 文件路径 | 说明 |
|----------|------|
| `worker/src/.../congestcontrol/CongestionController.java` | 主控制器，362 行 |
| `worker/src/.../congestcontrol/UserCongestionControlContext.java` | 用户拥塞状态，88 行 |
| `worker/src/.../congestcontrol/BufferStatusHub.java` | 字节速率统计，91 行 |
| `worker/src/.../congestcontrol/TimeSlidingHub.java` | 滑动窗口基类，164 行 |
| `worker/src/.../congestcontrol/UserBufferInfo.java` | 用户缓冲信息，41 行 |
| `common/src/.../write/SlowStartPushStrategy.java` | 客户端慢启动策略，180 行 |
| `worker/src/.../worker/WorkerSource.scala` | 指标定义（PotentialConsumeSpeed 等） |
| `worker/src/.../worker/Worker.scala` | IsHighWorkload 判断逻辑 |
| `worker/src/.../worker/PushDataHandler.scala` | 拥塞状态码返回 |
| `common/src/main/scala/.../CelebornConf.scala` | 全部配置项定义 |
| `docs/developers/trafficcontrol.md` | 官方流量控制设计文档 |

---

## 十二、现状小结

Celeborn 的 Congestion Control 通过以下几个关键设计实现了**高效、公平、可观测**的流量控制：

1. **双层触发**：全局水位线（磁盘缓冲/Worker 生产速率）+ 用户级水位线，互相独立，灵活组合。
2. **迟滞状态机**：高水位进入，低水位退出，防止拥塞状态频繁震荡。
3. **公平抑制算法**：全局超水位时，以"平均速率"为基准，仅抑制超额用户，低速用户不受影响。
4. **时间滑动窗口**：1 秒粒度的精确速率统计，窗口大小可配置（默认 10s）。
5. **TCP-like 客户端策略**：慢启动 → 拥塞避免 → 指数退避，精准控制并发推送量。
6. **动态配置**：Worker 级和用户级水位线均可通过 ConfigService 热更新，无需重启。
7. **完善监控**：`IsHighWorkload`、`PotentialConsumeSpeed`、`UserProduceSpeed` 等指标覆盖全链路状态。

---

# 第二部分：App 粒度 Congestion Control 扩展设计

> **注意**：本部分为得物内部版 celeborn-dewu 上的设计方案（开发分支 `feature/app-level-congestion-control`，基于 `v0.6.2-based`），**未落地于本仓库**；本仓库中不存在 `AppCongestionControlContext`。

## 一、背景与动机

### 1.1 现有方案的局限

现有 Congestion Control 以 **`UserIdentifier`（tenantId + name）** 为粒度进行流量控制：

- 同一用户（如 `prod_user`）名下所有 Spark App 共享同一个拥塞上下文和水位线
- 无法区分"一个大作业打满"还是"多个小作业累计超限"
- 无法对单个 Spark App 进行隔离限速，某个 App 的暴涨会影响同用户所有其他 App

### 1.2 目标

引入 **App 粒度（appId 维度）** 的 Congestion Control，实现：

1. 对单个 Spark App 的 Push 速率独立进行水位线判断和抑制
2. 用户内不同 App 之间相互隔离，不因某个 App 突发而影响同用户其他 App
3. 在全局（Worker 级）、用户（User 级）、应用（App 级）三个维度形成分层控制体系
4. 复用现有 TimeSlidingHub / BufferStatusHub 等基础设施，最小化改动面

---

## 二、现有架构

现有架构（ReserveSlots → PartitionDataWriter → UserCongestionControlContext → PushDataHandler 的 isUserCongested 检查）见**第一部分**；现有粒度对比为：Worker 级（`overHighWatermark`）、User 级（`userCongestionContextMap` + 用户水位线）已实现，App 级尚未实现。

---

## 三、设计方案

### 3.1 总体思路

在 `CongestionController` 中新增 App 维度的数据结构，与 User 维度并行：

- 新增 `AppCongestionControlContext`（类比 `UserCongestionControlContext`）
- `PartitionDataWriter` / `TierWriter` 同时持有 App 级上下文
- `isUserCongested` 扩展为同时检查 User 级 + App 级两路水位线
- 配置、指标、动态配置均对称扩展

### 3.2 新增数据结构

#### 3.2.1 AppCongestionControlContext（新建文件）

```
worker/src/main/java/.../congestcontrol/AppCongestionControlContext.java
```

```java
public class AppCongestionControlContext {

  private volatile boolean congestionControlFlag;
  private final UserBufferInfo appBufferInfo;          // 复用 UserBufferInfo
  private final BufferStatusHub workerBufferStatusHub; // 引用全局 producedBufferStatusHub
  private final String appId;
  private volatile AppTrafficQuota appTrafficQuota;    // App 级水位线

  public AppCongestionControlContext(
      AppTrafficQuota appTrafficQuota,
      BufferStatusHub workerBufferStatusHub,
      UserBufferInfo appBufferInfo,
      AbstractSource workerSource,
      String appId) {
    this.congestionControlFlag = false;
    this.appBufferInfo = appBufferInfo;
    this.appId = appId;
    this.workerBufferStatusHub = workerBufferStatusHub;
    this.appTrafficQuota = appTrafficQuota;
    // 注册 App 级生产速率指标，用 appId 作为标签
    workerSource.addGauge(
        WorkerSource.APP_PRODUCE_SPEED(),
        Map.of("appId", appId),
        () -> appBufferInfo.getBufferStatusHub().avgBytesPerSec());
  }

  public void onCongestionControl()  { congestionControlFlag = true; }
  public void offCongestionControl() { congestionControlFlag = false; }
  public boolean inCongestionControl() { return congestionControlFlag; }

  public void updateProduceBytes(long numBytes) {
    long timeNow = System.currentTimeMillis();
    BufferStatusHub.BufferStatusNode node = new BufferStatusHub.BufferStatusNode(numBytes);
    appBufferInfo.updateInfo(timeNow, node);
    // 注意：App 级不重复累加到 workerBufferStatusHub（User 级已累加）
  }

  public UserBufferInfo getAppBufferInfo() { return appBufferInfo; }
  public String getAppId() { return appId; }
  public AppTrafficQuota getAppTrafficQuota() { return appTrafficQuota; }
  public void updateAppTrafficQuota(AppTrafficQuota quota) { this.appTrafficQuota = quota; }
}
```

#### 3.2.2 AppTrafficQuota（新建文件）

```
common/src/main/scala/org/apache/celeborn/common/quota/AppTrafficQuota.scala
```

```scala
case class AppTrafficQuota(
    appProduceSpeedHighWatermark: Long,   // 绝对阈值：App 速率超过此值才触发条件1（默认 200MB/s）
    appProduceSpeedLowWatermark: Long,    // 迟滞退出阈值（默认 100MB/s）
    appCongestionRatio: Double            // 相对比例系数，条件2：appSpeed > avgAllAppsSpeed / ratio（默认 1.5）
)
```

**参数说明**：
- `appCongestionRatio=1.5` 表示：某 App 速率需超过全局 App 均值的 1/1.5 ≈ 66% 才触发条件 2
- ratio 越大，条件 2 越宽松（越难触发）；ratio 越小（趋近 1.0），越容易触发
- 两个条件**同时满足（AND）** 才拥塞，相比 User 级单条件更精准

### 3.3 CongestionController 改动

在现有 `CongestionController` 中新增 App 维度的 Map：

```java
// 新增字段
private final ConcurrentHashMap<String, UserBufferInfo> appBufferStatuses;
private final ConcurrentHashMap<String, AppCongestionControlContext> appCongestionContextMap;
private final AppTrafficQuota defaultAppQuota;
```

**核心判断：isAppCongested（双重条件 AND 语义）**

参考对比方案，App 级拥塞判断采用**更严格的双重条件**，两者同时满足才触发拥塞控制，避免因单一条件误杀正常 App：

```
条件 1（绝对阈值）：appProduceSpeed > appProduceSpeedHighWatermark
条件 2（相对比较）：appProduceSpeed > (所有App平均生产速率 / ratio)
```

两个条件均满足 → 拥塞；任一不满足 → 不触发（有迟滞退出）。

**逻辑说明**：
- **条件 1** 过滤掉整体速率本就很低的 App（即便相对高也不拦）
- **条件 2** 过滤掉"全局所有 App 普遍在高速跑"时的正常高速 App（即便超阈值也不拦）
- `ratio` 是放大系数（> 1），值越小越敏感，值越大越宽松；建议默认值为 `1.5`，即某 App 速率超过全局均值 1.5 倍才视为异常

```java
public boolean isAppCongested(AppCongestionControlContext appCtx) {
    if (appBufferStatuses.isEmpty()) return false;

    long appProduceSpeed = appCtx.getAppBufferInfo().getBufferStatusHub().avgBytesPerSec();
    AppTrafficQuota quota = appCtx.getAppTrafficQuota();

    // 计算所有 App 的平均生产速率（App 维度均值，区别于 User 维度均值）
    long avgAppProduceSpeed = getAvgAppProduceSpeed();

    boolean aboveAbsoluteThreshold =
        appProduceSpeed > quota.appProduceSpeedHighWatermark();          // 条件 1
    boolean aboveRelativeThreshold =
        avgAppProduceSpeed > 0 &&
        appProduceSpeed > avgAppProduceSpeed / quota.appCongestionRatio(); // 条件 2

    // 进入拥塞：两个条件同时满足（AND）
    if (aboveAbsoluteThreshold && aboveRelativeThreshold) {
        appCtx.onCongestionControl();
        if (logger.isDebugEnabled()) {
            logger.debug(
                "App {} congested: produceSpeed={}, threshold={}, avgSpeed/ratio={}/{}",
                appCtx.getAppId(), appProduceSpeed,
                quota.appProduceSpeedHighWatermark(),
                avgAppProduceSpeed, quota.appCongestionRatio());
        }
    } else if (appCtx.inCongestionControl()
        && appProduceSpeed < quota.appProduceSpeedLowWatermark()) {
        // 退出拥塞：低于低水位（迟滞恢复）
        appCtx.offCongestionControl();
    }
    return appCtx.inCongestionControl();
}

/**
 * 计算所有活跃 App 的平均生产速率：
 *   sum(appBufferStatusHub.avgBytesPerSec) / appCount
 *
 * 注意：与 User 级的 getPotentialProduceSpeed() 不同，
 * 这里以 App 数量为分母，统计的是 App 维度的均值。
 */
private long getAvgAppProduceSpeed() {
    int appCount = appBufferStatuses.size();
    if (appCount == 0) return 0;
    long totalSpeed = 0;
    for (UserBufferInfo bufInfo : appBufferStatuses.values()) {
        totalSpeed += bufInfo.getBufferStatusHub().avgBytesPerSec();
    }
    return totalSpeed / appCount;
}
```

**新增方法：getAppCongestionContext**

```java
public AppCongestionControlContext getAppCongestionContext(String appId) {
    return appCongestionContextMap.computeIfAbsent(appId, id -> {
        // 检查上限保护
        if (appBufferStatuses.size() >= maxTrackedApps) {
            logger.warn("Tracked apps count {} reached limit {}, skip app {} congestion tracking",
                appBufferStatuses.size(), maxTrackedApps, appId);
            return null;  // 返回 null，调用方需判空
        }
        UserBufferInfo appBufferInfo =
            appBufferStatuses.computeIfAbsent(id, k ->
                new UserBufferInfo(System.currentTimeMillis(),
                                   new BufferStatusHub(sampleTimeWindowSeconds)));
        AppTrafficQuota quota = (configService == null)
            ? defaultAppQuota
            : configService.getAppTrafficQuota(appId).orElse(defaultAppQuota);
        return new AppCongestionControlContext(
            quota, appBufferInfo, workerSource, appId);
    });
}
```

**removeInactiveApps（与 removeInactiveUsers 合并为一个定时任务）**：

```java
private void removeInactiveUsers() {
    // ... 原有 User 级清理逻辑不变 ...

    // 新增：同步清理 App 级不活跃条目
    long currentTimeMillis = System.currentTimeMillis();
    Iterator<Map.Entry<String, UserBufferInfo>> appIter = appBufferStatuses.entrySet().iterator();
    while (appIter.hasNext()) {
        Map.Entry<String, UserBufferInfo> next = appIter.next();
        String appId = next.getKey();
        if (currentTimeMillis - next.getValue().getTimestamp() >= userInactiveTimeMills) {
            appIter.remove();
            appCongestionContextMap.remove(appId);
            workerSource.removeGauge(WorkerSource.APP_PRODUCE_SPEED(), Map.of("appId", appId));
            workerSource.removeGauge(WorkerSource.APP_CONGESTION_STATUS(), Map.of("appId", appId));
            logger.info("App {} has been inactive, removed from app congestion tracking", appId);
        }
    }
}
```

### 3.4 PartitionDataWriter / TierWriter 改动

#### PartitionDataWriter

新增字段：

```java
private AppCongestionControlContext appCongestionControlContext;
```

在构造函数中（紧随 userCongestionControlContext 初始化之后）：

```java
if (CongestionController.instance() != null) {
  userCongestionControlContext =
      CongestionController.instance()
          .getUserCongestionContext(writerContext.getUserIdentifier());

  // 新增：初始化 App 级拥塞控制上下文
  appCongestionControlContext =
      CongestionController.instance()
          .getAppCongestionContext(writerContext.getAppId());
}
```

新增 getter：

```java
public AppCongestionControlContext getAppCongestionControlContext() {
  return appCongestionControlContext;
}
```

#### TierWriter（LocalTierWriter）

`writeInternal` 中，在更新 User 级字节数时**同步**更新 App 级：

```java
override def writeInternal(buf: ByteBuf): Unit = {
  val numBytes = buf.readableBytes()
  if (userCongestionControlContext != null)
    userCongestionControlContext.updateProduceBytes(numBytes)
  // 新增：同步更新 App 级速率统计
  if (appCongestionControlContext != null)
    appCongestionControlContext.updateProduceBytes(numBytes)
  // ...
}
```

### 3.5 PushDataHandler 改动

在现有的 `isUserCongested` 检查之后，增加 App 级检查：

```scala
// 改动前
val isCongested = congestionController.isUserCongested(
    fileWriter.getUserCongestionControlContext)

// 改动后（两路 OR）
val isCongested =
  congestionController.isUserCongested(
      fileWriter.getUserCongestionControlContext) ||
  congestionController.isAppCongested(
      fileWriter.getAppCongestionControlContext)
```

四个检查点（handlePushData 主副本、非主副本；handlePushMergedData 主副本、非主副本）均如此修改。

**返回的状态码不变**，客户端收到 `PUSH_DATA_SUCCESS_PRIMARY_CONGESTED` 或 `REPLICA_CONGESTED` 后同样触发慢启动回退。

### 3.6 配置参数新增

在 `CelebornConf.scala` 中新增以下配置：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.worker.congestionControl.app.enabled` | `false` | App 级拥塞控制子开关 |
| `celeborn.worker.congestionControl.appProduceSpeed.high.watermark` | `209715200`（200MB/s） | App 生产速率绝对高水位（Bytes/sec），条件 1 |
| `celeborn.worker.congestionControl.appProduceSpeed.low.watermark` | `104857600`（100MB/s） | App 生产速率低水位，迟滞退出阈值 |
| `celeborn.worker.congestionControl.appCongestion.ratio` | `1.5` | 相对判断系数，条件 2：appSpeed > avgAllAppsSpeed / ratio |
| `celeborn.worker.congestionControl.maxTrackedApps` | `10000` | Worker 上最大跟踪 App 数（防泄漏保护） |

**开关逻辑**：

```
celeborn.worker.congestionControl.enabled=true     # 总开关（原有，控制整个拥塞控制模块）
celeborn.worker.congestionControl.app.enabled=true # App 级子开关（新增）

# App 级控制生效条件：总开关 AND app.enabled 同时为 true
```

**设计原则**：
- `enabled=false` 时，整个拥塞控制模块（含 App 级）不初始化，行为与现在完全一致
- `enabled=true` + `app.enabled=false`（默认）：只有 User 级控制生效，升级不影响现有行为
- `enabled=true` + `app.enabled=true`：User 级 + App 级同时生效
- User 级和 App 级水位线独立配置，设为 `Long.MAX_VALUE` 即等于不触发对应层级的限速

**PartitionDataWriter 中的初始化逻辑**：

```java
if (CongestionController.instance() != null) {
  userCongestionControlContext =
      CongestionController.instance()
          .getUserCongestionContext(writerContext.getUserIdentifier());

  // App 级：需额外检查 app.enabled 子开关
  if (conf.workerCongestionControlAppEnabled()) {
    appCongestionControlContext =
        CongestionController.instance()
            .getAppCongestionContext(writerContext.getAppId());
  }
}
```

**可选扩展**：支持通过动态配置对单个 App 设置独立水位线（`ConfigService` 中按 appId 查询）。

### 3.7 监控指标新增

在 `WorkerSource.scala` 中新增：

```scala
val APP_PRODUCE_SPEED = "AppProduceSpeed"  // Gauge，带 appId 标签
```

类比 `USER_PRODUCE_SPEED`，每个活跃 App 独立上报，App 清理时对应 Gauge 一并移除。

---

## 四、内存影响分析与控制

### 4.1 每个 App 的内存占用估算

新增的 `AppCongestionControlContext` 及其依赖对象，每个活跃 App 新增内存：

| 对象 | 成员 | 内存估算 |
|------|------|----------|
| `AppCongestionControlContext` | 3 个引用 + 1 boolean + volatile | ~40 B |
| `UserBufferInfo`（复用类） | 1 long + 1 引用 | ~24 B |
| `BufferStatusHub`（核心）| `LinkedBlockingDeque` + `sumInfo` | 见下方 |
| `BufferStatusHub._deque` | 最多 `maxQueueSize` 个 `Pair<Long, N>` | 每桶 ~80 B |
| `BufferStatusNode`（每桶）| `LongAdder`（1 Cell = ~16 B）| ~40 B |

**`BufferStatusHub` 单实例内存上限**（默认 10s 窗口，最多 10 个桶）：
```
10桶 × (Pair对象~48B + Long包装~16B + BufferStatusNode~40B) = ~1040B ≈ 1 KB
```

**每个 App 合计**：
```
AppCongestionControlContext + UserBufferInfo + BufferStatusHub ≈ 1.1 KB
```

### 4.2 规模估算（Worker 视角）

| 场景 | 并发 App 数 | 新增堆内存 |
|------|------------|-----------|
| 小规模（测试/开发） | 50 | ~55 KB |
| 中等规模 | 500 | ~550 KB |
| 大规模生产 | 2000 | ~2.2 MB |
| 极端场景 | 5000 | ~5.5 MB |

**结论**：内存开销与 User 级（每 User ≈ 1.1 KB）完全对称，绝对量很小，即便并发 5000 个 App 也仅额外占用 ~5.5 MB 堆内存，**对 Worker 几乎无影响**。

### 4.3 防泄漏机制

活跃 App 条目如果不及时清理，会在 Worker 长时间运行时持续积累。需要完善以下机制：

#### 方案 A：基于"最后活跃时间"的定时清理（与 User 级对称，推荐）

扩展 `removeInactiveUsers()` 定时任务，同步清理超过 `userInactiveTimeMills`（默认 10min）未收到 PushData 的 App 条目：

```java
private void removeInactiveApps() {
    long currentTimeMillis = System.currentTimeMillis();
    Iterator<Map.Entry<String, UserBufferInfo>> iter =
        appBufferStatuses.entrySet().iterator();
    while (iter.hasNext()) {
        Map.Entry<String, UserBufferInfo> next = iter.next();
        String appId = next.getKey();
        UserBufferInfo bufInfo = next.getValue();
        if (currentTimeMillis - bufInfo.getTimestamp() >= userInactiveTimeMills) {
            iter.remove();  // 从 appBufferStatuses 移除
            appCongestionContextMap.remove(appId);
            workerSource.removeGauge(WorkerSource.APP_PRODUCE_SPEED(),
                Map.of("appId", appId));
            workerSource.removeGauge(WorkerSource.APP_CONGESTION_STATUS(),
                Map.of("appId", appId));
            logger.info("App {} has been inactive, removed from app congestion map", appId);
        }
    }
}
```

此方案与现有 User 级清理逻辑完全对称，无需引入新的定时线程。

#### 方案 B：监听 ApplicationLost 事件（主动清理，可选增强）

Worker 收到 Master 的 `ApplicationLost` 消息时，立即清理对应 App 的所有拥塞控制条目：

```java
// CongestionController 新增方法
public void onApplicationLost(String appId) {
    if (appBufferStatuses.remove(appId) != null) {
        appCongestionContextMap.remove(appId);
        workerSource.removeGauge(WorkerSource.APP_PRODUCE_SPEED(), Map.of("appId", appId));
        workerSource.removeGauge(WorkerSource.APP_CONGESTION_STATUS(), Map.of("appId", appId));
        logger.info("App {} lost, cleaned up congestion control context", appId);
    }
}
```

在 `Controller.scala` 的 `ApplicationLost` 处理路径中调用此方法，实现应用结束后立即回收，不等超时。

> **推荐策略**：方案 A 作为兜底保证（防止 ApplicationLost 消息丢失），方案 B 作为快速回收优化，两者同时实现。

### 4.4 最大 App 数量上限保护（可选配置项）

为防止极端情况（短时间创建海量 App），新增一个可选的软上限配置：

```
celeborn.worker.congestionControl.maxTrackedApps（默认 10000）
```

当 `appBufferStatuses.size() >= maxTrackedApps` 时，新 App **跳过 App 级拥塞控制初始化**（仍走 User 级控制），并打印 WARN 日志。这是纯防御性保护，正常场景不会触发。

---

## 五、三层控制体系全貌

```
PushData 到达 Worker
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│  层 1：Worker 级（Back Pressure，始终启用）                    │
│  直接内存占用 > Pause Receive 阈值 (0.85)                     │
│  → 暂停接收，强制刷盘                                         │
└──────────────────┬───────────────────────────────────────────┘
                   │ 正常
                   ▼
┌──────────────────────────────────────────────────────────────┐
│  层 2：Worker 级拥塞控制（需开启 enabled=true）               │
│  diskBuffer 或 workerProduceSpeed 超高水位                   │
│  → overHighWatermark = true                                  │
│  → 超过"全局平均速率"的用户被抑制                              │
└──────────────────┬───────────────────────────────────────────┘
                   │ overHighWatermark=false 或未超均值
                   ▼
┌──────────────────────────────────────────────────────────────┐
│  层 3：User 级拥塞控制（已有）                                 │
│  userProduceSpeed > userProduceSpeedHighWatermark            │
│  → isUserCongested = true                                    │
└──────────────────┬───────────────────────────────────────────┘
                   │ OR
                   ▼
┌──────────────────────────────────────────────────────────────┐
│  层 4：App 级拥塞控制（本设计新增）                           │
│  appProduceSpeed > appProduceSpeedHighWatermark              │
│  → isAppCongested = true                                     │
└──────────────────┬───────────────────────────────────────────┘
                   │
                   ▼
        返回 PUSH_DATA_SUCCESS_*_CONGESTED
        客户端触发慢启动回退
```

---

## 六、监控指标设计

### 6.1 现有 User 级指标（保持不变）

| 指标名 | 类型 | 标签 | 含义 |
|--------|------|------|------|
| `UserProduceSpeed` | Gauge | `tenantId`, `name` | 单用户生产速率（bytes/sec） |
| `PotentialConsumeSpeed` | Gauge | 无 | 全局消费速率 / 活跃用户数 |
| `WorkerConsumeSpeed` | Gauge | 无 | Worker 全局消费速率 |
| `IsHighWorkload` | Gauge（0/1） | 无 | Worker 是否高负载 |

### 6.2 新增 App 级指标

#### 速率指标

| 指标名 | 类型 | 标签 | 含义 | 注册位置 |
|--------|------|------|------|----------|
| `AppProduceSpeed` | Gauge | `appId` | 单 App 生产速率（bytes/sec） | `AppCongestionControlContext` 构造时 |
| `AppCongestionStatus` | Gauge（0/1） | `appId` | 该 App 当前是否处于拥塞控制中 | `AppCongestionControlContext` 构造时 |

**指标生命周期**：
- **创建**：`getAppCongestionContext(appId)` 首次触发时注册 Gauge
- **清理**：`removeInactiveApps()` 定时任务检测到 App 不活跃后调用 `workerSource.removeGauge("AppProduceSpeed", Map.of("appId", appId))` 及 `removeGauge("AppCongestionStatus", ...)`

#### 计数指标（新增）

| 指标名 | 类型 | 标签 | 含义 | 注册位置 |
|--------|------|------|------|----------|
| `AppCongestionCount` | Counter | `appId` | 该 App 被触发拥塞的累计次数 | `AppCongestionControlContext` 构造时 |

每次调用 `onCongestionControl()` 时，若当前 `congestionControlFlag=false`（即新进入拥塞），则 counter +1。

### 6.3 WorkerSource 改动

在 `WorkerSource.scala` 的 `object WorkerSource` 中新增常量：

```scala
// congestion control - app level（新增）
val APP_PRODUCE_SPEED   = "AppProduceSpeed"
val APP_CONGESTION_STATUS = "AppCongestionStatus"
val APP_CONGESTION_COUNT  = "AppCongestionCount"
```

### 6.4 指标注册代码示例

**AppCongestionControlContext 构造函数中：**

```java
// 生产速率 Gauge（动态标签，按 appId 区分）
workerSource.addGauge(
    WorkerSource.APP_PRODUCE_SPEED(),
    Map.of("appId", appId),
    () -> appBufferInfo.getBufferStatusHub().avgBytesPerSec());

// 拥塞状态 Gauge（0=正常，1=拥塞中）
workerSource.addGauge(
    WorkerSource.APP_CONGESTION_STATUS(),
    Map.of("appId", appId),
    () -> congestionControlFlag ? 1L : 0L);

// 拥塞次数 Counter
workerSource.addCounter(
    WorkerSource.APP_CONGESTION_COUNT(),
    Map.of("appId", appId));
```

**onCongestionControl() 修改：**

```java
public void onCongestionControl() {
    if (!congestionControlFlag) {
        congestionControlFlag = true;
        workerSource.incCounter(WorkerSource.APP_CONGESTION_COUNT(),
            Map.of("appId", appId));
    }
}
```

### 6.5 Prometheus 指标样例

```
# 各 App 实时生产速率
metrics_AppProduceSpeed_Value{appId="application_1772620567885_001234"} 15728640.0
metrics_AppProduceSpeed_Value{appId="application_1772620567885_005678"} 3145728.0

# 各 App 当前拥塞状态
metrics_AppCongestionStatus_Value{appId="application_1772620567885_001234"} 1.0
metrics_AppCongestionStatus_Value{appId="application_1772620567885_005678"} 0.0

# 各 App 历史被拥塞次数
metrics_AppCongestionCount_total{appId="application_1772620567885_001234"} 7.0
```

### 6.6 Grafana 建议大盘

| Panel | 查询 | 说明 |
|-------|------|------|
| App 生产速率 TOP N | `topk(10, metrics_AppProduceSpeed_Value)` | 找出速率最高的 App |
| 当前拥塞 App 列表 | `metrics_AppCongestionStatus_Value == 1` | 实时查看哪些 App 被限速 |
| App 拥塞触发频率 | `rate(metrics_AppCongestionCount_total[5m])` | 哪些 App 频繁被拥塞 |
| 用户 vs App 速率对比 | `UserProduceSpeed` vs `sum by(appId) (AppProduceSpeed)` | 验证 App 聚合 ≈ User 总量 |

---

## 七、关键设计决策说明

### 7.1 App 级速率统计不重复累加到 Worker 全局

`AppCongestionControlContext.updateProduceBytes()` 只更新 App 自己的 `appBufferInfo`，**不**再累加到 `producedBufferStatusHub`（全局）。原因：

- User 级 `updateProduceBytes()` 已经将字节累加到全局 `producedBufferStatusHub`
- 双重累加会导致全局速率虚高，破坏 Worker 层的水位线判断

### 7.2 App 级拥塞与 User 级拥塞的关系（OR 语义）

两者任一触发即返回拥塞：

- User 级限制的是该用户**整体**（所有 App 汇总）不能超速
- App 级限制的是单个 App **独立**不能超速
- 两者互不影响，共同保护系统稳定性

### 7.3 不引入 isAppCongested 到 Worker 全局水位线路径

全局 `overHighWatermark` 路径已有"超过均值即抑制"的逻辑，App 级控制主要补充在全局正常时、单个 App 仍然过速的场景，因此 App 级只在用户级水位线检查的"平行分支"中运行。

### 7.4 App 信息的获取路径

`PartitionDataWriterContext` 已经包含 `appId`（`getAppId()`），无需修改 RPC 协议，直接复用即可。

---

## 八、开发仓库与分支

- **目标仓库**：`celeborn-dewu`（外部基线工作树，得物内部定制版）
- **开发分支**：`feature/app-level-congestion-control`（已基于 `v0.6.2-based` 创建）

---

## 九、需改动的文件清单

| 文件 | 改动类型 | 说明 |
|------|----------|------|
| `worker/src/main/java/.../congestcontrol/AppCongestionControlContext.java` | 新建 | App 级拥塞上下文 |
| `common/src/main/scala/.../quota/AppTrafficQuota.scala` | 新建 | App 级水位线 case class |
| `worker/src/main/java/.../congestcontrol/CongestionController.java` | 修改 | 新增 `appBufferStatuses` / `appCongestionContextMap`、`isAppCongested`、`getAppCongestionContext`、App 清理逻辑 |
| `worker/src/main/java/.../storage/PartitionDataWriter.java` | 修改 | 持有并初始化 App 级上下文 |
| `worker/src/main/scala/.../storage/TierWriter.scala`（LocalTierWriter） | 修改 | `writeInternal` 同步更新 App 级速率 |
| `worker/src/main/scala/.../worker/PushDataHandler.scala` | 修改 | 四个检查点增加 `isAppCongested`（OR 语义） |
| `worker/src/main/scala/.../worker/WorkerSource.scala` | 修改 | 新增 `AppProduceSpeed` / `AppCongestionStatus` / `AppCongestionCount` 指标常量 |
| `common/src/main/scala/.../CelebornConf.scala` | 修改 | 新增 5 项 App 级配置 |
| `worker/src/test/java/.../congestcontrol/TestCongestionController.java` | 修改 | 新增 App 级测试用例 |

---

## 十、测试计划

| 测试方法 | 验证内容 |
|----------|----------|
| `testSingleAppCongestion` | 单 App 速率超高水位时被拥塞控制，低于低水位后恢复 |
| `testMultipleAppsUnderSameUser` | 同一 User 下，一个 App 过速仅该 App 被抑制，另一 App 不受影响 |
| `testAppAndUserCongestionIndependent` | User 级和 App 级拥塞独立触发，OR 语义验证 |
| `testAppMetricsLifecycle` | AppProduceSpeed Gauge 在 App 不活跃后自动清理 |
| `testAppDynamicConfiguration` | 动态更新 App 级水位线后拥塞行为变化 |

### 集成验证

1. 启用 `celeborn.worker.congestionControl.enabled=true`
2. 配置 `appProduceSpeed.high.watermark=20MB/s`、`appProduceSpeed.low.watermark=10MB/s`
3. 运行多个 Spark App 同时推送，确认：
   - 超速的 App 收到 CONGESTED 状态码并触发客户端慢启动
   - 未超速的同用户 App 正常推送
   - `metrics_AppProduceSpeed_Value{appId="..."}` 在 Prometheus 中正确上报
