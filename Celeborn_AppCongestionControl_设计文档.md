# Celeborn App 粒度 Congestion Control 设计文档

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

## 二、现有架构回顾

### 2.1 关键类与数据流

```
ReserveSlots(applicationId, shuffleId, userIdentifier)
    │
    └─ Controller.handleReserveSlots()
            │
            └─ StorageManager.createPartitionDataWriter(appId, shuffleId, userIdentifier, ...)
                    │
                    └─ PartitionDataWriterContext { appId, shuffleId, userIdentifier, shuffleKey }
                            │
                            └─ PartitionDataWriter / TierWriter 构造时：
                                    CongestionController.getUserCongestionContext(userIdentifier)
                                    → UserCongestionControlContext (以 UserIdentifier 为粒度)
```

**PushData 时：**
```
PushDataHandler.handlePushData()
    │
    ├─ fileWriter.getUserCongestionControlContext()   ← 返回 UserCongestionControlContext
    └─ congestionController.isUserCongested(ctx)     ← 判断是否拥塞
```

### 2.2 现有粒度对比

| 粒度 | Key | 当前实现 |
|------|-----|----------|
| Worker 级 | 全局 | `overHighWatermark`（磁盘缓冲/Worker 生产速率） |
| User 级 | `UserIdentifier` | `userCongestionContextMap` + `userProduceSpeedWatermark` |
| App 级 | `appId` | **❌ 尚未实现** |

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

### 6.1 App 级速率统计不重复累加到 Worker 全局

`AppCongestionControlContext.updateProduceBytes()` 只更新 App 自己的 `appBufferInfo`，**不**再累加到 `producedBufferStatusHub`（全局）。原因：

- User 级 `updateProduceBytes()` 已经将字节累加到全局 `producedBufferStatusHub`
- 双重累加会导致全局速率虚高，破坏 Worker 层的水位线判断

### 6.2 App 级拥塞与 User 级拥塞的关系（OR 语义）

两者任一触发即返回拥塞：

- User 级限制的是该用户**整体**（所有 App 汇总）不能超速
- App 级限制的是单个 App **独立**不能超速
- 两者互不影响，共同保护系统稳定性

### 6.3 不引入 isAppCongested 到 Worker 全局水位线路径

全局 `overHighWatermark` 路径已有"超过均值即抑制"的逻辑，App 级控制主要补充在全局正常时、单个 App 仍然过速的场景，因此 App 级只在用户级水位线检查的"平行分支"中运行。

### 6.4 App 信息的获取路径

`PartitionDataWriterContext` 已经包含 `appId`（`getAppId()`），无需修改 RPC 协议，直接复用即可。

---

## 八、开发仓库与分支

- **目标仓库**：`/Users/admin/Workspaces/celeborn-dewu`（得物内部定制版）
- **开发分支**：`feature/app-level-congestion-control`（已基于 `v0.6.2-based` 创建）

## 九、需改动的文件清单

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
