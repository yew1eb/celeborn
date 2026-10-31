# Celeborn Metrics / Grafana 模块增强点调研（可提交社区）

> 调研对象：Celeborn 的 metrics 采集/暴露链路 + `assets/grafana` 大盘资产 + 相关文档与 CI。
> 问题：这个模块还有哪些需要增强、优化、完善的地方，哪些值得整理成 PR 提交给社区？
> 代码依据：外部基线工作树 `celeborn-main-1`（分支 `main`，commit `626a07cfc`），行号均已核实。
> 证据标记：**✅ = 已逐条回读源码确认**；无标记的来自子 agent 报告，落地前需再核一次。

## 一、背景与结论

metrics/grafana 是 Celeborn 上游**近期最活跃**的模块之一，且社区对"小而实"的 PR 接受度很高（近期已合入 `[CELEBORN-2463] Add ManuallyExcludedWorkerCount`、`[CELEBORN-2346] RequestSlots failure metrics`、`[CELEBORN-2316] metadata operation metrics`、`[CELEBORN-2078] Fix wrong grafana metrics units`、`[CELEBORN-2028] Setup GA for grafana dashboard`）。因此"修 bug / 修单位 / 补面板 / 补文档"这条路径是验证过的、低阻力的贡献方式。

本次调研产出 **20 个可提交点**，分三档：

- **P0（5 个）**：确定性 bug，改动 1~10 行，无设计争议，最适合作为第一批 PR。
- **P1（9 个）**：质量与一致性问题，其中"扩展 `dev/lint_grafana.py`"是杠杆最高的一项。
- **P2（6 个）**：设计级缺陷，建议先在 `dev@celeborn.apache.org` 发讨论再动手。

---

## 二、P0：确定性 bug（推荐先做）

| # | 问题 | 证据 | 修复 |
|---|---|---|---|
| 1 | `addHistogram` 算出了带 label 的 key，却用**裸 name** 注册进 Dropwizard，导致所有 label 变体共享同一个 Histogram；且 `removeMetric` 用带 label 的 key 删除，永远删不掉 | ✅ `common/src/main/scala/org/apache/celeborn/common/metrics/source/AbstractSource.scala:208-215`（对比 `addTimer:184`、`addCounter:199`、`addGauge:121` 都传 `metricNameWithLabel`） | 1 行：`metricRegistry.histogram(metricNameWithLabel, histogramSupplier)` |
| 2 | `RpcMetricsTracker.close()` 注册时带 `NAME_LABEL`、移除时**不带** → key 不匹配，**移除是空操作**，dispatcher/endpoint 每次生命周期泄漏一批 gauge/timer | ✅ `common/src/main/scala/org/apache/celeborn/common/rpc/RpcMetricsTracker.scala:64-66`（`addGauge(QUEUE_LENGTH, NAME_LABEL)`）vs `:71-73`（`removeGauge(QUEUE_LENGTH)` 无 label） | 三处 `removeGauge/removeTimer` 补上 `NAME_LABEL` |
| 3 | `getAllMetricsNum` 统计 timer/meter/gauge/counter，**漏掉 histogram**，容量告警数值偏低 | ✅ `AbstractSource.scala:638-645`（而 `getMetrics:649` 会输出 histogram） | 补 `namedHistogram.size()` |
| 4 | Grafana 面板引用**代码里不存在**的指标 `metrics_LocalFetchChunkTime_Max`，该面板永久 "No data" | ✅ `assets/grafana/celeborn-dashboard.json:6071,6076`；真实常量是 `FETCH_LOCAL_CHUNK_TIME = "FetchLocalChunkTime"`（`worker/.../WorkerSource.scala:176`），同组 Mean 面板是对的 | 改为 `metrics_FetchLocalChunkTime_Max` |
| 5 | `docs/monitoring.md` 记录了一个**幽灵指标** `RatisApplyCompletedIndexDiff`，代码里只有 `RatisApplyCompletedIndex`（`master/.../MasterSource.scala:82`），Grafana 还配了一个永远空的面板 | ✅ `docs/monitoring.md:124` + `celeborn-dashboard.json:2101`；全仓 grep 无该指标 | 删文档行 + 删面板（改动最小）；或在 master 上真正实现 diff 指标 |

### P0-1 细节

```scala
// AbstractSource.scala:207-215
def addHistogram(name: String, labels: Map[String, String]): Unit = {
  val metricNameWithLabel = metricNameWithCustomizedLabels(name, labels)   // 带 label 的 key 算出来了
  namedHistogram.putIfAbsent(
    metricNameWithLabel,
    NamedHistogram(
      name,
      metricRegistry.histogram(name, histogramSupplier),                   // ← 却用裸 name 注册
      labelsWithCustomizedLabels(labels)))
}
```

后果：给 `PartitionFileSize` 加 `disk` label 后，每个盘的 histogram 会读到同一份聚合数据；`removeMetric` 用 `metricNameWithLabel` 删一个从未注册过的 key，形成孤儿条目。目前被掩盖是因为仅有的两个 histogram（`WorkerSource.scala:113-114`：`FetchChunkTransferSize`、`PartitionFileSize`）都没带 label。

---

## 三、P1：质量与一致性问题

| # | 问题 | 证据 |
|---|---|---|
| 6 | `dev/lint_grafana.py` **只查 panel id 是否重复**，对单位/描述/重复标题/gridPos 冲突/time range/datasource 全不校验 → 下述缺陷可被随意再次混入 | ✅ 73 行脚本，唯一逻辑是 `traverse_ids` + `Counter` 去重；`.github/workflows/grafana.yml` 只跑它 + 外部 `dashboard-linter` |
| 7 | 3 组复制粘贴重复面板，把真正该画的指标挤掉了：`OssFlushSize_Count`×2（id 247/248）、`S3FlushSize_Count`×2（249/250）、`RegionStartFailCount_Count`×2（154/155）；而 `RegionFinishFailCount`、`OssFlushCount`、`S3FlushCount` **完全没有面板** | ✅ `WorkerSource.scala:201,233,235` 定义了这些常量 |
| 8 | RPC timer 面板单位写成 `ns`，但 `AbstractSource.scala:584,590` 所有 timer 都经 `reportNanosAsMills` 导出为**毫秒** → 显示差 10⁶ | `AbstractSource.scala:584,590`；面板 id 229/230/232/233 |
| 9 | 172/194 面板**不带 `role` 过滤**，而 `RegisteredShuffleCount`、`RunningApplicationCount` 等指标 master/worker 同名，图上会混在一起；大盘里也没有 `role` 模板变量 | ✅ `WorkerSource.scala:167` 与 `MasterSource.scala:61` 同名 |
| 10 | 194 个面板全是 `timeseries`，0 个 stat/table/heatmap；92 个缺 `unit`，164 个缺 `description`；`graphTooltip:0` 无共享十字线；JVM 大盘连 `refresh` 都没有 | `celeborn-dashboard.json`（schemaVersion 41，194 panels）；`celeborn-jvm-dashboard.json`（schemaVersion 37，17 panels，0 description） |
| 11 | `docs/monitoring.md:412` 称 Netty DirectArena 指标 "nor in Grafana dashboard"，但面板 id 158/164/173/171/201-216/254-259 已经画了 → 文档过时 | `docs/monitoring.md:412` |
| 12 | 命名三种风格并存：`ThreadPoolSource` 12 个 snake_case 字面量**无 companion object、无常量**（每个名字写两遍，共 24 处）；`ResourceConsumptionSource` 用 lowerCamelCase；其余用 UpperCamelCase | `ThreadPoolSource.scala:34-106`；`ResourceConsumptionSource.scala:29-35` |
| 13 | `MessageWithHeader.java` 用**裸字符串** `"FetchChunkTransferTime"` / `"FetchChunkTransferSize"` 而非 `WorkerSource` 常量 → 常量改名不会编译报错但指标静默失效 | `MessageWithHeader.java:134,138,139` |
| 14 | `ObservedDevice.scala:87` 把设备名和状态**拼进指标名**（`Device_<name>_<status>_Count`），每台机器一个指标名；同仓已有正确范式（`DeviceMonitor.scala:97-107` 用 `device` label、`Flusher.scala:103` 用 `mountpoint`） | `ObservedDevice.scala:87` |

### 为什么 P1-6 是杠杆最高的一项

`dev/lint_grafana.py` 现在跑一遍的输出是：

```
Linting: assets/grafana/celeborn-dashboard.json
Current max numeric id: 272
No duplicate id found.
```

也就是说：**当前大盘是"合规"的**，但它对上面 4/5/7/8/10 全部缺陷完全无感。给它加上"重复标题 / 缺 unit / 缺 description / gridPos 冲突 / 硬编码 time.from-to / pluginVersion 泄漏"等检查，等于把这一整类缺陷永久挡在 CI 外面 —— 这类"防止问题复发"的基础设施贡献在 Apache 项目里通常很受欢迎。

顺带：CI 里 `dashboard-linter` 是 `git clone` 上游 main 分支（`grafana.yml:52`）**未 pin 版本**，建议一起 pin。

---

## 四、P2：设计级缺陷（需先在 dev@ 讨论）

| # | 问题 | 证据 |
|---|---|---|
| 15 | **`getMetrics` 是破坏性读取**：clear timer 队列 + 对每个 timer `reservoir.reset()`。默认 `metrics.properties` 同时挂了 `PrometheusServlet` / `JsonServlet` / `LoggerSink` 三个消费者，谁先抓谁拿走；两个 Prometheus 副本抓同一个 worker 会分摊样本；`ResettableSlidingWindowReservoir` 的滑动窗口每次 scrape 被清空 → **导出的分位数无意义** | ✅ `AbstractSource.scala:649-663`（`getAndClearTimerMetrics()`）、`fillInnerMetricsSnapshot:684-685`（`reservoir.reset()`） |
| 16 | 无界 in-flight timer map：`namedTimers` 的 startTime map 只在 `size > 5000` 且**每 10 分钟**才清理一次，中间无任何上界；key 是文件路径 / UUID | ✅ `AbstractSource.scala:399-420` |
| 17 | 每个 Source 一个 `metricsCleaner` 单线程池，只在 `destroy()` 里关闭，而 `destroy()` 全仓只有 `NettyRpcEnv.scala:364` 一处调用 → 线程泄漏；线程名还硬编码成 `worker-metrics-cleaner`（master/client 上也叫这个） | ✅ `AbstractSource.scala:69-71,693` |
| 18 | label 值**未做转义**就拼进 Prometheus 文本输出，value 含 `"` 或 `\` 会产生非法输出，整次 scrape 失败 | `MetricLabels.scala:26-29` + `AbstractSource.scala:713-721` |
| 19 | 客户端（LifecycleManager）**完全没有 MetricsSystem**，`Role.CLIENT` 已定义但未用于指标；`RpcMetricsTracker` 的 histogram 只能靠 `dump()` 打日志 | ✅ grep：`client*/src/main` 中无 `MetricsSystem` |
| 20 | Quota 无任何指标；`ResourceConsumption` 只有 disk/hdfs 维度、**缺 S3/OSS**，而 `WorkerSource.scala:73-76` 已在统计 S3/OSS flush → 配额核算对 S3/OSS 是瞎的 | `ResourceConsumptionSource.scala:28-36`；`WorkerSource.scala:73-76` |

### P2-15 补充说明（最值得发邮件讨论的一条）

```scala
// AbstractSource.scala:649-663 —— 注释本身就写着 "it will clear the timeMetrics queue"
override def getMetrics: String = {
  var leftMetricsNum = metricsCapacity
  val sb = new mutable.StringBuilder
  leftMetricsNum = fillInnerMetricsSnapshot(getAndClearTimerMetrics(), leftMetricsNum, sb)
  ...
  // fillInnerMetricsSnapshot 内对每个 timer：t.timer...reservoir.asInstanceOf[ResettableSlidingWindowReservoir].reset()
}
```

这直接抵消了 `ResettableSlidingWindowReservoir`（配置 `celeborn.metrics.timer.slidingWindow.size`，默认 4096）的设计意图：分位数描述的不是滑动窗口，而是"上一次 scrape 到这次 scrape 之间"的任意区间。修复方向是改成非破坏性读取（缓存快照 + 短 TTL），但会影响 Prometheus 语义，建议先在邮件列表对齐。

---

## 五、建议的 PR 切分（按社区接受度排序）

| PR | 内容 | 说明 |
|---|---|---|
| **PR-1** | P0 的 1+2+3 | 纯代码 bug，1~10 行改动 + 单测。标题建议 `[CELEBORN-XXXX] Fix histogram registration ignoring labels and metric removal key mismatch` |
| **PR-2** | Grafana 面板修复 4+5+7 | 删/改幽灵与重复面板，补 `RegionFinishFailCount` / `OssFlushCount` / `S3FlushCount`，修 `LocalFetchChunkTime_Max` |
| **PR-3** | 扩展 `dev/lint_grafana.py` | 加重复标题、缺 `unit`、缺 `description`、gridPos 冲突、硬编码 `time.from/to`、`pluginVersion` 泄漏等检查；顺带 pin CI 里的 `dashboard-linter` 版本 |
| **PR-4** | 文档漂移 11 + 补 RPC namespace + `SlotsAllocated` 语义更正 | `docs/monitoring.md:238` 写 "Slots allocated in last hour"，实际是自进程启动累计（`WorkerSource.scala:64` 是纯 counter，只有 `ACTIVE_CONNECTION_COUNT` 会 remove） |
| **PR-5** | P2-15 破坏性 scrape | 需先在 `dev@` 发讨论 |

### PR-1 需要新增的单测

- `CelebornSourceSuite`：加两个**不同 label** 的 histogram，验证取值互相独立（现有 `CelebornSourceSuite.scala:32` 只测了无 label 的 `addHistogram(histogram)`，所以 P0-1 这个 bug 一直没被发现）。
- `RpcMetricsTracker`：`close()` 后断言 gauge/timer 被真正移除（现有测试完全没有覆盖 `close()`）。

---

## 六、验证方式

```bash
# 单测
build/mvn test -pl common -am -DwildcardSuites=org.apache.celeborn.common.metrics.source.CelebornSourceSuite

# Grafana lint
python3 dev/lint_grafana.py assets/grafana/celeborn-dashboard.json
./dashboard-linter/dashboard-linter lint assets/grafana/celeborn-dashboard.json --config assets/grafana/.lint --strict

# 端到端（已覆盖 /metrics/prometheus 与 /metrics/json 输出）
build/mvn test -pl service -am -DwildcardSuites=org.apache.celeborn.service.deploy.cluster.ApiBaseResourceSuite

# 手动 smoke
curl http://<worker>:<httpPort>/metrics/prometheus | grep -i <指标名>
```

> 注意：`assets/grafana/.lint` 里已排除 `panel-title-description-rule`（理由 "Some panels have a title but no description"），如果要强制补 description，需要同步去掉这条 exclusion。

---

## 七、待确认

1. 优先推进哪几类（A 代码 bug / B Grafana+lint / C 文档 / D 破坏性 scrape / E 覆盖缺口）？
2. 粒度：拆成多个独立 PR，还是打包成一个？
3. 是否只出调研报告、暂不动手？
