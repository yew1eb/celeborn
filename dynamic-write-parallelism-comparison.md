# 方案对比：parallel-partition-write（A）vs dynamic-write-parallelism（B）

> A = `~/Workspaces/celeborn-main` 分支 `parallel-partition-write`（当前主推，已提交 3 个 commit：Phase 1 / 1.1 / 1.2）
> B = `~/Workspaces/celeborn-main-4` 分支 `dynamic-write-parallelism`（单 commit `f41206fe3`，附 311 行设计文档）
> 对比基于两边实际代码逐行阅读，非仅设计文档。

## 0. 一句话定位

- **A**：以 **split 事件的 fillTime**（首报时刻 − 该 epoch 真实分配时刻）为判定信号，LM 侧 `PartitionHotnessTracker` 集中判定，executor 只上报原生 revive；升档步进 +1/窗口。
- **B**：以 **revive 频率**（30s 滑窗内"真实分配"次数 ≥ K(P)=ratio×P）为判定信号，driver 侧滑窗计数，executor 侧 `WriteLocationTracker`；升档倍增 P→2P。

两者目标相同（大/倾斜分区 1:N 并行写）、约束相同（零 worker 改动、开关默认关、向后兼容）、executor 路由相同（mapId 哈希取模）。**差异集中在判定信号、状态模型严谨度、实现完成度。**

## 1. 架构对比表

| 维度 | A（celeborn-main） | B（celeborn-main-4） |
|---|---|---|
| 判定信号 | fillTime = split 首报时刻 − epoch 真实 allocTime，< 60s 窗口 → 热点 | 30s 滑窗内"真实分配"次数 ≥ ceil(ratio×P) |
| 信号过滤 | **cause ∈ {SOFT_SPLIT, HARD_SPLIT} 且旧 location 的 worker 可用**；push 失败原则性不计量 | **无 cause 过滤**，所有真实分配都计数（含 push 失败、worker 故障引发的 revive） |
| HARD 模式适配 | R1 统一计量后支持 | 原生支持（其唯一原优势，已被 A 追平） |
| 升档步长 | +1/窗口（去抖 60s），封顶 4 | 倍增 P→2P，冷却 5s，封顶 8 |
| 检测延迟 | 写满 1 个 threshold（split 驱动固有） | 同左（revive 也由 split 触发），两者等价 |
| driver 状态 | HotState **稀疏**（仅 revive 过的 partition），已提取为独立可单测组件（227 行） | activeSiblingsMap **非稀疏**（registerShuffle 即全量建条目）+ 滑窗队列；逻辑散在 ChangePartitionManager/LifecycleManager 两处 |
| executor 状态 | LocationGroup 薄包装 + 懒加载 ParallelState（244 行），soft-retired 兜底排水 | WriteLocationTracker singleMap + 稀疏 siblingsMap（208 行），无退休语义 |
| proto | `PbChangeLocationPartitionInfo.additionalPartitions`（field 5），`partition` 保持 **max-epoch**（老 client 语义不变） | 同 message 加 `locations`（field 5），但 `partition` 回填 **locs.get(0)（最老 sibling）** |
| driver→executor 收敛 | 全集回复（max epoch + additional），executor mergeAll 按 epoch 排序收敛 | 全集回复（getActiveSiblings 累积列表），executor updateOnRevive 整体替换 |
| 快速切换 | retire + 预置 reviveStatus=SUCCESS，重推线程立即换活跃 location | excludeSibling 本地排除 + 重推时 selectForMapId 跳过，免 RPC |
| 测试 | 3 个套件 19 例（含去抖/上限/epoch 乱序/worker 守卫/cause 守卫） | 2 个套件（driver 决策 154 行 + tracker 182 行） |
| 验证状态 | 定向 47/47 绿；client 全量回归进行中 | 无全量回归证据 |

## 2. 判定算法对比（核心分歧）

### A 的 fillTime 法
- 每次 split 独立判定：`fillTime ≈ threshold / 单 location 聚合写速 < 60s` ⇒ 热点线 ≈ 17MB/s。**慢而稳的热点也能检出**（45s 写满一次即升档）。
- 免疫 mapper 数放大（首报去重）、免疫 epoch 乱序（每 epoch 对照自己的 allocTime）、免疫故障污染（双重守卫）。
- 代价：爬升慢（每 60s +1，到 4 需 ~3 分钟）——正是之前讨论过的"60s 滞后"问题。

### B 的 revive 频率法
- 挂载点选择正确（三层去重后的真实分配处计数，不被 mapper 数放大）——这部分设计分析很扎实。
- 负反馈收敛论证（P 升 → 单 sibling 涨速 ÷P → 频率降）在均匀散布下成立。
- 三个固有弱点：
  1. **滑窗计数对"慢而稳"热点不敏感**：30s 窗口内凑不够 P 次真实分配就永远清零不升（每次 split 间隔 40s 的热点在 B 下永远不升档，在 A 下首次即升）。
  2. **无 cause 过滤**：worker 故障/网络抖动引发的 revive 风暴与热点同构，直接误升档（A 的 workerAvailable 守卫正是为此）。
  3. 时间戳在 `reserveSlotsWithRetry` **之前**记录，分配失败也计数（轻微污染）。

## 3. B 的实现级缺陷（代码证据，非设计意图）

1. **SOFT_SPLIT 后旧 location 不被排除——护窗口目标落空（正确性级别）**
   - driver 侧 `removeActiveSibling`（`LifecycleManager.scala:185`）**无任何调用方**（grep 确认，仅定义+注释引用），活跃集合只增不减；
   - executor 侧 `excludeSibling` 只在 HARD_SPLIT 路径调用，SOFT_SPLIT 路径不排除；
   - 结果：SOFT_SPLIT 后 driver 回复的全集仍含已 split 的旧 location → `updateOnRevive` 整体替换后 mapper 继续写它 → 迅速 HARD_SPLIT。B 设计文档命门 2 自称"excludedLocations 写侧过滤"，**代码里不存在该机制**。
   - 对照 A：`onEpochRetired` 把退休 epoch 移出活跃集合 + executor `pick` 两遍扫描（非退休优先，soft-retired 仅兜底），语义闭环。
2. **single-value = locs.get(0)（最老 sibling）**：`ReviveManager` 用 singleMap 做 max-epoch 去重，single 回退到老 epoch 会使去重判据失真；proto `partition` 字段同样回填最老 sibling，混合版本下老 client 拿到过期 location。A 始终保持 max-epoch。
3. **driver 内存非稀疏**：`updateLatestPartitionLocations` 对 registerShuffle 的全量 location 都建 activeSiblingsMap 条目（B 的"稀疏"只在 executor 侧）。10 万 partition 的 shuffle 在 LM 多一层全量 map。A 的 HotState 真稀疏。

## 4. B 值得吸收的优点

1. **快速爬升**：首次 revive 即 P=1→2，之后窗口内达标即倍增、冷却仅 5s——对"护住 SOFT→HARD 升级窗口"的目标响应比 A 的 +1/60s 及时一个量级。可直接回应之前"60s 窗口滞后"的疑虑。
2. **设计文档的问题分析**：SOFT→HARD 黄金窗口（窗口宽度 = 剩余量/写速）、三层去重与频率挂载点的论证，写得比 A 的调研部分更透，值得吸收进 A 的文档。
3. executor 侧稀疏 + 懒创建的内存账目思路与 A 的 LocationGroup 薄包装异曲同工（互相印证方向正确）。

## 5. 结论

**A 在信号严谨度（cause/worker 双重守卫、慢热点可检出）、状态模型（稀疏、退休语义闭环）、协议兼容（max-epoch 保持）、可测性（独立 tracker + 19 例）上全面优于 B；B 唯一实质优势是爬升速度。** B 的三个实现缺陷（尤其 SOFT_SPLIT 不排除旧 location）使其当前形态达不到其自身设计目标，不建议转用 B。

## 6. 建议行动（三选一）

- **方案一（推荐）：A 为主线，吸收 B 的快速爬升**
  1. `PartitionHotnessTracker.onEpochRetired` 升档步进从 +1 改为按 fillTime 比例步进（如 fillTime < window/4 → desired×2，封顶 max；或简单倍增），保留首报去重/cause 守卫/worker 守卫不变；
  2. A 的设计文档补充：§3.5 对比表加 B 方案一行（含本文 §3 缺陷记录）、§1 补 SOFT→HARD 窗口分析；
  3. 新增步进升档的单测；
  4. 跑定向套件 + client 全量回归。
- **方案二：只交付本对比报告，代码不动**（维持已推送的 Phase 1.2 现状，快速爬升留待灰度数据后再定）。
- **方案三：转用 B 为主线**（不推荐，需先修 §3 三个缺陷，工作量大于吸收其优点）。
