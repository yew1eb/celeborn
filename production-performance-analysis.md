# Celeborn 生产环境性能实证分析(CPU 火焰图 + Heapdump)

> 基于版本:0.7.0-SNAPSHOT(当前仓库);整理日期:2026-08-05。
> 本文档由以下四份生产实证分析文档合并而来:`MASTER_CPU_FLAMEGRAPH_ANALYSIS.md`、`WORKER_CPU_FLAMEGRAPH_ANALYSIS.md`、`celeborn-master-heapdump-优化点分析.md`、`celeborn-worker-heapdump-优化点分析.md`(合并后源文件已删除)。
>
> **时效性说明**:CPU 火焰图数据采集于 **2026-07-25**;heapdump 基于生产 **0.6.2.x(master)/ 0.6.2.3(worker)** 集群。这些实证数据的生产版本与当前仓库版本(0.7.0-SNAPSHOT)不同——文中所有类名、文件路径、配置键、行号引用已对照 0.7.0-SNAPSHOT 代码逐一核实并修正;生产实证数据本身(采样占比、MAT 对象计数、生产配置)保持原样。
>
> 产出形态:**纯分析报告,不改代码**。覆盖配置层 + 代码层。

## 目录

- [第一章 跨组件共性发现](#第一章-跨组件共性发现)
- [第二章 Worker CPU 火焰图分析(2026-07-25)](#第二章-worker-cpu-火焰图分析2026-07-25)
- [第三章 Master CPU 火焰图分析](#第三章-master-cpu-火焰图分析)
- [第四章 Worker 内存 Heapdump 分析(生产 0.6.2.3)](#第四章-worker-内存-heapdump-分析生产-0623)
- [第五章 Master 内存 Heapdump 分析(生产 0.6.2.x)](#第五章-master-内存-heapdump-分析生产-062x)
- [附录:解析脚本与中间产物](#附录解析脚本与中间产物)
- [交叉引用](#交叉引用)

---

## 第一章 跨组件共性发现

四份实证分析中重复出现、与具体组件无关的发现,集中在此章;后续各章不再重复描述,只指向本章。

### 共性-1 Netty leakDetection 生产开 `advanced`,应降级(配置层,零代码风险)

**证据**:
- Worker CPU 火焰图:`java_lang_Throwable::fill_in_stack_trace` self 105 + `ResourceLeakDetector.track0` 30 + 相关 ≈ 173 samples(0.21% self,但每个被采样的 ByteBuf 都填一次完整栈,inclusive 影响更大)。调用栈:`AbstractByteBufAllocator.toLeakAwareBuffer → ResourceLeakDetector.track → track0 → DefaultResourceLeak.<init> → TraceRecord.<init> → Throwable.<init> → fillInStackTrace`。
- Master CPU 火焰图:本次采样窗口短、leak 采样率低,`fillInStackTrace` 未大量出现,但配置与 worker 完全一致;长期运行下同样会对 Ratis shaded Netty 的 ByteBuf 分配产生开销(Raft RPC 频繁分配 ByteBuf)。

**代码位置**(已对照 0.7.0-SNAPSHOT 核实):
- `charts/celeborn/values.yaml:109`(master `CELEBORN_MASTER_JAVA_OPTS`)、`:240`(worker `CELEBORN_WORKER_JAVA_OPTS`)— 均含 `-Dio.netty.leakDetectionLevel=advanced`
- `charts/celeborn/ci/values.yaml:44,87` — CI 同样设置
- 生产代码无任何 `ResourceLeakDetector.setLevel` 调用(仅测试 `SslClusterReadWriteLeakSuite` 用 PARANOID)

**根因**:`ADVANCED` 级别对采样到的 ByteBuf 调 `Throwable.fillInStackTrace` 记录完整分配栈。生产长期开启无意义——leak detection 是排查工具,不是常态监控。Netty 默认 `SIMPLE` 采样极低几乎无开销。

**优化方向**:把 master/worker JAVA_OPTS 里的 `-Dio.netty.leakDetectionLevel=advanced` 去掉(回退 SIMPLE)或改 `disabled`。仅在主动排查 ByteBuf 泄漏时临时开 `PARANOID`。

**预期收益**:worker ≈ 0.2-0.5% CPU,且减少 GC 压力(每次填栈分配栈帧对象),火焰图里 `ReferenceCountUtil.touch`(self 483)也会随之下降;master 无法量化(master 太空闲),但与 worker 同源,长期/高 Raft 流量下有正向收益。

### 共性-2 `numArenas` 是全局配置,无法 per-module 生效,且被 `Math.min` 钳制

**证据**:
- Master heapdump:生产配 `celeborn.network.memory.allocator.numArenas = 64`,但被 `NettyUtils.java:173` `Math.min(defaultNumHeapArena(), numCores)` 钳制到 48,配置未生效;master 是控制面无数据面 buffer,却持有 24 个 Celeborn 侧 HeapArena。
- Worker heapdump:120 个 `PoolArena$HeapArena` 占 244MB(堆的 37%),1,668 个 `PoolChunk` 占 279MB,1g 堆下偏高。

**根因**(已核实):
- `CelebornConf.scala:2012-2018` `celeborn.network.memory.allocator.numArenas` 是**全局 key**,不带 `<module>` 前缀,无法为 master 单独配小(会影响 worker 数据面)。transport module fallback(`CelebornConf.scala:1709-1719`,`RPC_SERVICE_MODULE → RPC_MODULE` 等)不覆盖 numArenas 维度。
- `CelebornConf.scala:620-622` `networkAllocatorArenas()` 默认 `Math.max(availableProcessors, 2)`;Netty `defaultNumHeapArena()` 默认 `2*cores`。
- `NettyUtils.java:171-178` 构造 `PooledByteBufAllocator` **同时建 heap 和 direct 两个 arena 池**,即使 `preferDirect=true`;heap arena 不可缺省,因为 RPC header 显式用 heap 编码(`MessageEncoder.java:84` `heapBuffer()`、`SslMessageEncoder.java:81`)。
- 附带误导项:`sbin/start-master.sh:34`、`sbin/start-worker.sh:39`、`sbin/restart-worker.sh:39` 的 `-Dio.netty.allocator.type=pooled` 对 Celeborn 显式构造的 allocator 无效,可移除。

**0.7.0 版本差异(新事实)**:0.7.0 新增 `celeborn.network.memory.allocator.type`(`CelebornConf.scala:1986-2001`),支持 `POOLED`(默认)/`UNPOOLED`/`ADAPTIVE`,为 heapdump 章 P1-1 类问题提供了新的配参选项(UNPOOLED 回收更激进但 CPU 更高,ADAPTIVE 需灰度验证)。

**优化方向**:
1. 代码层让 `numArenas` 支持 per-module(给 master 配更小值);或让 master 走独立 transport module 并改 `NettyUtils` 支持 numArenas 走 module fallback。
2. 配参优先(零代码):设 `numArenas` 到与实际 IO 线程数对齐的较低值(16~32),而非 CPU 核数。
3. 评估 `celeborn.network.memory.allocator.allowCache=true`(当前默认 false,`CelebornConf.scala:1977-1984`)能否加速 chunk 回收(需压测权衡)。

### 共性-3 Arthas 诊断工具残留

**证据**:Master heapdump Top Components 中 `ArthasClassloader` 7.4MB + NioEventLoopGroup;worker heapdump 中 `com.taobao.arthas` 的 `NioEventLoopGroup` 16MB + `ArthasClassloader` 19MB。arthas agent 加载 jar 还会贡献 Finalizer/ZipFile 堆积(见第五章 P2-2)。

**根因**:生产挂载 arthas 诊断工具,非 Celeborn 本身。

**优化方向**:诊断完成后下线 arthas。

---

## 第二章 Worker CPU 火焰图分析(2026-07-25)

### Context

生产环境 celeborn-worker 在高负载下 CPU 占用偏高。用 async-profiler 抓取 CPU 火焰图 `10.238.44.229_20260725-091843.html`(**数据日期 2026-07-25**),总采样 **80,611 samples**,结合代码定位 Celeborn 自身可优化的热点(而非 Netty/JDK/native syscall 这些不可改的部分)。每项给出:调用栈证据(inclusive/self 占比)→ 代码位置(文件:行号)→ 根因 → 优化方向 → 预期收益。

**解析方法(可信度说明)**:火焰图为 async-profiler 的 canvas 压缩格式(`cpool` 常量池 + `n()/u()/f()` DFS 栈序列)。`f(key, level, left, width, ...)` 中 `key>>>3` 为 cpool 下标、`width` 为该子树 inclusive 采样数;`u()` 下一层(子帧)、`n()` 同层(兄弟)。用 true stack-reconstruction 重建调用树后统计 self/inclusive,**self 总和 = 80,611 = total(cover 100%)**,验证重建正确。多线程合并图若改用 left 几何判断父子会失效(self 虚高至 795%),故采用栈语义重建。解析脚本见附录。

### 证据总览:CPU 全景(谁在烧 CPU)

按子系统聚合 self 采样(占 80,611 总采样的百分比):

| 子系统 | self 占比 | 说明 |
|--------|----------|------|
| native syscall I/O(writev/recv/send/pread) | 54.45% | 不可改,worker 本质 IO 密集 |
| Netty buffer 管理(CompositeByteBuf/PoolArena) | 10.79% | 部分可通过配置/代码缓解 |
| JDK concurrent(ConcurrentHashMap 等) | 8.02% | 部分来自 Celeborn metrics 热路径 |
| Netty channel/RPC pipeline | 3.55% | 不可改 |
| **Celeborn 自身代码(self)** | **5.02%** | 本章核心目标 |
| other(vtable/arraycopy/String/异常栈等) | 19.63% | 部分与 Celeborn 代码相关 |

**结论**:Celeborn 自身代码 self 只占 5%,但有若干"每条消息都做一次"的低效点,叠加后在万级 QPS 下显著,且部分能顺带削减 Netty/JDK 开销。优先级按"可削减量 × 实现成本 × 确定性"排序。

关键 Celeborn 帧 inclusive 占比(决定优先级):

| 帧 | inclusive | self | 性质 |
|----|-----------|------|------|
| `AbstractSource.sample`(Flusher 线程) | **44.7%** | 0.001% | 包裹 `LocalFlushTask.flush`,主体是不可改磁盘 IO,可优化的是框架固定开销 |
| `Flusher$$anon$1.run` | 22.45% | — | Flusher 线程总入口 |
| `LocalFlushTask.flush` | 18.67% | — | 主体 `fileChannel.write`=`__writev`;`nioBuffers()` 重建 + `incCounter`×2 可优化 |
| `WorkerSource.recordAppActiveConnection` | **4.36%** | 0.097% | **几乎全是可削减的纯 Java 开销,每条 push/fetch 触发,默认开启** |
| `Utils$.splitShuffleKey` | **4.23%** | — | 纯字符串开销,每条消息 2 次 |
| `MessageWithHeader.transferTo` | 29.31% | — | fetch 回包路径,header 必拷贝 + body ByteBuf 拷贝 |
| `PushDataHandler.handleCore` | 23.50% | 1.13% | push 主处理 |
| Netty `ResourceLeakDetector`(leak detection) | — | 0.21%(+上下游) | **生产开 `advanced`,应降级 → 见[共性-1](#共性-1-netty-leakdetection-生产开-advanced应降级配置层零代码风险)** |

> 注:`recordAppActiveConnection`(4.36%)与 `splitShuffleKey`(4.23%)inclusive 接近,因前者内部调用后者,两者高度重叠。合并看:**每条消息的"app-level metrics 记账"链路 ≈ 4.4% CPU,且默认开启**——这是最大的可削减项。

### 优化点清单(按优先级)

#### P0-1 关闭/降级 worker app-level metrics(配置层,立竿见影)

**证据**:`WorkerSource.recordAppActiveConnection` inclusive **4.36%**,被每条 `PushData`/`PushMergedData`/`OpenStream`/`ReadAddCredit`/`NotifyRequiredSegment`/`handleChunkFetchRequest` 调用。

**代码位置**:
- `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/WorkerSource.scala:141-151` — `recordAppActiveConnection`
- `common/src/main/scala/org/apache/celeborn/common/CelebornConf.scala:6070-6078` — `METRICS_WORKER_APP_LEVEL_ENABLED` **默认 `true`**

**根因**:默认开启时,每条消息执行:
1. `appActiveConnections.get(channel.id().asLongText())` — `ConcurrentHashMap.get` + Netty `asLongText()` 新建 String
2. `Utils.splitShuffleKey(shuffleKey)._1`(`WorkerSource.scala:144`)— `String.split("-")`(正则 + `String[]`)+ `dropRight(1).mkString("-")`(数组 + StringBuilder + 新 String),**却只用 `._1`**
3. `applicationIds.contains(applicationId)` — 即使 appId 早已在集合里(常态),前面两步也照做

火焰图对应:`ConcurrentHashMap.get`(self 2553,其中 1046 走向 recordAppActiveConnection)、`String.split`(245)、`mkString`(154)。

**优化方向**:
- **配置层(推荐先做)**:生产环境若不需要 per-app 连接数指标,设 `celeborn.metrics.worker.appLevel.enabled=false`,直接砍掉这 4.4%。配置项 doc 已警告 appId 是高基数 label。
- **代码层(若需保留指标)**:把 `contains` 检查前置——用 `channel.id()` 维度先判重,或缓存"该 channel 已记录的 appId 集合"避免每条消息 split;`splitShuffleKey` 改 `lastIndexOf('-')` 切片(见 P1-1)。`contains` 在常态命中时本就为 true,split 是纯浪费。

**预期收益**:配置关闭 ≈ 4.4% CPU;代码优化保留指标 ≈ 2-3%。

#### P0-2 Netty leak detection 降级

→ 见[共性-1](#共性-1-netty-leakdetection-生产开-advanced应降级配置层零代码风险)(worker 侧收益 ≈ 0.2-0.5% CPU + 降 GC 压力)。

#### P1-1 `Utils.splitShuffleKey` 用 `lastIndexOf` 替代正则 split(代码层)

**证据**:`Utils$.splitShuffleKey` inclusive **4.23%**,每条 push/fetch 至少 2 次(`PushDataHandler.scala:992` 的 `checkAuth` + `WorkerSource.scala:144` 的 `recordAppActiveConnection`),merged/openStream 路径更多。

**代码位置**:`common/src/main/scala/org/apache/celeborn/common/util/Utils.scala:696-701`
```scala
def splitShuffleKey(shuffleKey: String): (String, Int) = {
  val splits = shuffleKey.split("-")              // 正则编译 + String[] 分配
  val appId = splits.dropRight(1).mkString("-")   // 数组拷贝 + StringBuilder + 新 String
  val shuffleId = splits.last.toInt
  (appId, shuffleId)
}
```

**根因**:`makeShuffleKey`(`Utils.scala:692`)就是 `s"$applicationId-$shuffleId"`,shuffleId 是最后一段。用 `String.split("-")`(正则)+ `mkString` 反向重组 appId 是 O(n) 分配多次对象,完全没必要。同文件 `splitPartitionLocationUniqueId`(`Utils.scala:703`)、`splitAttemptKey`(`Utils.scala:726`)同模式。

**优化方向**:改为 `lastIndexOf('-')` 一次定位,`substring` 切片:
```scala
def splitShuffleKey(shuffleKey: String): (String, Int) = {
  val idx = shuffleKey.lastIndexOf('-')
  (shuffleKey.substring(0, idx), shuffleKey.substring(idx + 1).toInt)
}
```
零正则、零中间数组、零 StringBuilder。同理改另两个 split 方法。

**调用方(全量,证实热点扩散,已对照 0.7.0-SNAPSHOT 核实)**:`PushDataHandler.scala:992`、`FetchHandler.scala:151,367`、`WorkerSource.scala:144`、`DeviceInfo.scala:136,145,160`、`Master.scala:751`、`Controller.scala:167`、`Worker.scala:829`、`StorageManager.scala:693,764,805,1011`。

**预期收益**:`String.split`(245)+`mkString`(154)≈ 400 samples 自身,加上 `dropRight`/`String[]` 分配与 GC,实际削减 ≈ 0.5-1% CPU,且降低 young GC 频率。改动低风险、纯函数易测试。

#### P1-2 `checkDiskFullAndSplit` 的 `logTrace` 加 `isTraceEnabled` 守卫(代码层)

**证据**:位于 push 热路径每条消息(`PushDataHandler.scala:257` `handlePushData`、`:590` `handlePushMergedData` 每 partition 一次)。

**代码位置**:`worker/src/main/scala/org/apache/celeborn/service/deploy/worker/PushDataHandler.scala:1449-1457`
```scala
logTrace(
  s"""
     |CheckDiskFullAndSplit in
     |diskFull:$diskFull,
     |partitionSplitMinimumSize:$partitionSplitMinimumSize,
     |splitThreshold:${fileWriter.getSplitThreshold},
     |fileLength:${fileWriter.getCurrentFileInfo.getFileLength}
     |fileName:${fileWriter.getCurrentFileInfo.getFilePath}
     |""".stripMargin)
```

**根因**:Scala `s"""...""".stripMargin` 插值在调用 `logTrace` 前就完成字符串构造(包括调用 `fileWriter.getSplitThreshold`、`getCurrentFileInfo.getFileLength` 等),即使 trace 级别关闭也会执行全部求值再被 log4j 丢弃。此处**无 `log.isTraceEnabled` 守卫**。

**优化方向**:加 `if (log.isTraceEnabled)` 守卫,或用 lazy 结构。Celeborn 代码里其他 logTrace 多数也无守卫,可一并排查。

> **版本修正**:原报告另指出 `:1474-1482` 有一段同类 hardSplit 的 `logTrace`;在 0.7.0-SNAPSHOT 中该段已改为 `logInfo` 且只在 `splitStatus == HARD_SPLIT` 分支内触发(`PushDataHandler.scala:1473-1483`),不再是每条消息的开销,但 info 级别下每次 hardSplit 都会真实打日志,可关注。

**预期收益**:trace 关闭时省掉每条消息的字符串构造 + 方法调用,≈ 0.1-0.3% CPU,降分配。

#### P1-3 metrics timer key 用 requestId 而非字符串拼接(代码层)

**证据**:`RpcResponseCallbackWithTimer.<init>` self 0.64%、`onSuccess` self 4.7%(inclusive),每条 push/merged 触发。

**代码位置**(已对照 0.7.0-SNAPSHOT 核实):
- `PushDataHandler.scala:165` `val key = s"${pushData.requestId}"`(merged 路径 `:1008` 同模式;原报告引用的 `:464` 已漂移)
- `PushDataHandler.scala:866-883` `RpcResponseCallbackWithTimer`:构造时 `startTimer`、`onSuccess`/`onFailure` 时 `stopTimer`
- `common/src/main/java/org/apache/celeborn/common/network/protocol/MessageWithHeader.java:133` `key = UUID.randomUUID().toString()`(每个 fetch chunk)
- `common/src/main/scala/org/apache/celeborn/common/metrics/source/AbstractSource.scala:318-350` `doStartTimer`/`doStopTimer`:`metricNameWithCustomizedLabels` 拼 + `namedTimers.get` + `pair._2.put/remove(key, nanoTime)`

**根因**:
1. `s"${pushData.requestId}"` 把 Long 拼成 String 当 timer key,纯无意义分配。
2. `stopTimer`(`AbstractSource.scala:310-316`)**无条件**调 `doStopTimer`,即使 `startTimer` 因 sample rate 被跳过也照做 `map.remove`——sample rate 调低对 stopTimer 无效。
3. `MessageWithHeader.transferTo` 每个 fetch chunk 生成 `UUID.randomUUID().toString()` 当 key。
4. `doStartTimer`/`doStopTimer`/`incCounter` 每次都 `metricNameWithCustomizedLabels`(空 labels 走 `metricsName + staticLabelsString` fast path 但仍新建 String;非空 labels 走 `MetricLabels.labelString` 的 map→toArray→sorted→mkString)。

**优化方向**:
- timer key 直接传 `requestId`(Long),`AbstractSource` 的 timer 内部 map 改用 `Long` key 或 `Long`→String 的轻量化;避免每消息字符串拼接。
- `metricNameWithCustomizedLabels`(`AbstractSource.scala:711-719`)对空 labels 可预计算并缓存(注册时算一次,查表用预计算 key),避免每次 `+`。
- `stopTimer` 加 `needSample()` 短路(与 `startTimer` 对称,`AbstractSource.scala:251`),sample rate 调低时才真正生效。
- `MessageWithHeader` 的 fetch chunk timer key 可复用单调递增 Long 而非 UUID。

**预期收益**:metric 记账 self 虽仅 0.29%,但去掉的字符串/UUID 分配会降低 GC 压力,inclusive 路径上 `String.<init>`(120)、`UUID` 相关、`mkString` 都会下降。≈ 0.3-0.5% CPU + 降分配。

#### P2-1 `LocalFlushTask.flush` 的 `nioBuffers()` 与 `incCounter` 优化(代码层)

**证据**:`LocalFlushTask.flush` inclusive **18.67%**(主体 `fileChannel.write`=`__writev` 不可改);`CompositeByteBuf.nioBufferCount` self 1224(1.52%)+ `addComponent0` self 1721(2.13%,与 returnBuffer 相关)。`AbstractSource.sample` 44.7% 包裹它。

**代码位置**:`worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/FlushTask.scala:59-78`
```scala
override def flush(copyBytes: Array[Byte]): Unit = {
  val readableBytes = buffer.readableBytes()
  val buffers = buffer.nioBuffers()        // 每次 flush 重建 ByteBuffer[]
  if (gatherApiEnabled) { ... fileChannel.write(buffers) ... }
  else { for (buffer <- buffers) ... }
  source.incCounter(WorkerSource.LOCAL_FLUSH_COUNT)        // 无 sample 过滤
  source.incCounter(WorkerSource.LOCAL_FLUSH_SIZE, readableBytes)
}
```
- `Flusher.scala:77` `key = s"Flusher-$this-${Random.nextInt()}"` 每次 flush 拼字符串 + Random 当 timer key
- `Flusher.scala:78` `workerSource.sample(getFlushTimeMetric(), key) { task.flush(...) }`

**根因**:
1. `buffer.nioBuffers()`(`FlushTask.scala:61`)每次 flush 把 CompositeByteBuf 的 components 重新组装成 `ByteBuffer[]`(内部遍历 + 计算 nioBufferCount + 分配数组)。Flusher 是 per-partition 高频。
2. `incCounter`×2(`FlushTask.scala:75-76`)无 sample 过滤,per-partition-per-flush 必触发。
3. `Flusher.scala:77` 的 timer key 每次拼接 + `Random.nextInt()`。

**优化方向**:
- `nioBuffers()` 结果若 gather 写一次写完,可考虑复用或减少重建(需评估 CompositeByteBuf 生命周期)。
- `incCounter` 对高频 flush counter 可走 `LongAdder` 累加、定期聚合,或加 sample 过滤。
- `Flusher` timer key 用线程局部单调 Long 替代字符串拼接。

**预期收益**:`nioBufferCount` 1.52% + `addComponent0` 2.13% 部分 + incCounter 开销,≈ 1-2% CPU(需谨慎,flush 是核心 IO 路径,改动需充分测试)。**此项需先验证 `nioBuffers()` 是否真可复用,不确定性强,列 P2**。

#### P2-2 `MessageWithHeader.transferTo` fetch chunk 路径(代码层,低优先)

**证据**:`MessageWithHeader.transferTo` inclusive 29.31%,`copyByteBuf` inclusive 9.12%。调用栈 `doWriteSingle → writeFileRegion → MessageWithHeader.transferTo → copyByteBuf`。

**代码位置**:`common/src/main/java/org/apache/celeborn/common/network/protocol/MessageWithHeader.java:116-182`(`transferTo` :116、`copyByteBuf` :158、`NIO_BUFFER_LIMIT=256KB` :56)

**根因**:body 是 `ByteBuf` 时走 `copyByteBuf`(内存拷贝到 socket),header 恒走 `copyByteBuf`。这是 fetch chunk 从内存 buffer 回包的设计路径,**非 bug**。`copyByteBuf` 内 `nioBufferCount()==1` 走单 buffer,CompositeByteBuf 走 `nioBuffers()` 数组遍历,按 256KB 分块。

**优化方向**:此路径开销主体是 `target.write(buffer)`(即 `__send` 系统调用,不可改)。可优化点已在 P1-3(UUID key)覆盖。`copyByteBuf` 本身是 Spark 遗留实现,逻辑合理,**不建议改动**。仅记录供参考。

**预期收益**:随 P1-3 的 UUID 优化附带下降,单独无显著收益。

### 不建议改动的项(已确认)

- **native syscall(writev/recv/send/pread)54.45%**:worker 本质 IO 密集,系统调用不可改。优化应面向"减少调用次数/批量",如已存在的 gather write(`gatherApiEnabled`)。
- **Netty channel/buffer pipeline 3.55%+10.79%**:Netty 内部,除非升级版本或调 `io.netty.*` 参数,不建议改 Celeborn 侧。
- **`MessageWithHeader.copyByteBuf` 本身**:Spark 遗留实现,逻辑合理,改动风险高于收益。
- **`WorkerPartitionLocationInfo.getLocation` 两次 CHM.get**:键是预存 String 无分配,两次哈希查找是必要逻辑,优化空间小。

### 配置层速查表(可立即在生产验证)

| 配置 / JVM 参数 | 当前 | 建议 | 对应优化项 | 预期 CPU 收益 |
|----------------|------|------|-----------|--------------|
| `celeborn.metrics.worker.appLevel.enabled` | `true`(默认,`CelebornConf.scala:6070-6078`) | `false`(若不需 per-app 指标) | P0-1 | ≈ 4.4% |
| `-Dio.netty.leakDetectionLevel` | `advanced`(charts) | 去掉/SIMPLE | P0-2(共性-1) | ≈ 0.2-0.5% |
| `celeborn.metrics.sample.rate` | `1.0`(默认,`CelebornConf.scala:5968-5969`) | `0.1`~`0.5`(若 timer 精度可降) | P1-3(部分,stopTimer 不受控) | ≈ 0.3% |

> 三项配置叠加预期可削减 **≈ 5% CPU**,且零代码风险。建议先在生产灰度验证 appLevel 关闭与 leak 降级。

### 代码层速查表(需改代码,按风险/收益排序)

| 优化项 | 文件:行号 | 风险 | 预期收益 |
|--------|----------|------|----------|
| P1-1 splitShuffleKey 用 lastIndexOf | `Utils.scala:696-701`(+703,726) | 低(纯函数,易测) | ≈ 0.5-1% |
| P1-2 logTrace 加 isTraceEnabled 守卫 | `PushDataHandler.scala:1449-1457` | 极低 | ≈ 0.1-0.3% |
| P1-3 timer key/metricName 预计算 | `PushDataHandler.scala:165,1008`、`MessageWithHeader.java:133`、`AbstractSource.scala:310-350,711-719` | 中(metrics 模块改动) | ≈ 0.3-0.5% |
| P2-1 flush nioBuffers/incCounter | `FlushTask.scala:59-78`、`Flusher.scala:77-78` | 中-高(IO 核心路径) | ≈ 1-2%(需验证) |

### 验证方法

本章为纯分析,无需运行验证。若后续实施某项,建议:
1. **配置项(P0)**:生产灰度一台 worker,改 `celeborn-defaults.conf` 或 JVM 参数,用同规格负载 + async-profiler 重新采样,对比 `recordAppActiveConnection`/`fillInStackTrace` 的 inclusive 占比是否下降。
2. **代码项(P1-1)**:`./build/mvn test -pl common -Dtest=UtilsSuite`(若有 splitShuffleKey 单测);新增/补充 `splitShuffleKey` 边界用例(appId 含 `-` 的情况,验证 `lastIndexOf` 切片正确)。
3. **代码项(P1-2/P1-3)**:`./build/mvn test -pl worker`;P1-3 涉及 metrics 需跑 `worker` 模块 metrics 相关 suite。
4. 所有代码改动提交前 `./dev/reformat` + `./build/mvn spotless:check`。
5. 复采火焰图后,用附录的 `/tmp/parse_flame.mjs` 解析脚本重新统计 self/inclusive,量化收益。

---

## 第三章 Master CPU 火焰图分析

### Context

继 worker 火焰图分析后,对 master 节点同样做 CPU 火焰图采样(与 worker 同日采集,**数据日期 2026-07-25**),确认 master 侧是否存在可优化热点,以及与 worker 共性问题是否同源。数据来源:生产环境 celeborn-master async-profiler CPU 火焰图 `10.252.162.101_master_output.html`,总采样 **158 samples**(对比 worker 80611,master 在采样窗口内 CPU 基本空闲)。解析方法与 worker 相同(true stack-reconstruction,self 总和 = 158 = total,cover 100% 验证正确),脚本见附录。

**最重要的结论先行**:master 在采样窗口内 CPU 占用极低(158 samples vs worker 80611,约 1/500),且处于 **JIT 预热期**(C2 编译器开销占 22.8%)。Celeborn 自身代码 self 仅 1.27%(2 samples)。因此 master **几乎没有值得改的代码热点**——大部分采样落在 JIT 编译、Ratis Raft 框架、Prometheus 抓取、GC 这些"非业务代码"上。本章主要价值在于:确认 master 无显著瓶颈 + 记录与 worker 共享的配置层优化(leak detection,见[共性-1](#共性-1-netty-leakdetection-生产开-advanced应降级配置层零代码风险))。

### 证据总览:CPU 全景(按子系统聚合 self)

| 子系统 | self 占比 | 说明 |
|--------|----------|------|
| JIT C2 编译(PhaseChaitin/PhaseIdealLoop/Compile::) | 22.78% | **预热期假象**,稳态消失。无需优化 |
| kernel(syscall/futex/memcpy/文件系统) | 20.25% | 不可改 |
| other(vtable/arraycopy/异常栈等) | 20.25% | 分散 |
| JDK 同步/线程(CompletableFuture/ConcurrentSkipListMap/Monitor/Safepoint) | 16.46% | 多来自 Ratis 异步 apply + GC safepoint |
| Ratis(Netty pipeline + Raft log) | 10.76% | master 核心工作,框架开销 |
| GC(PSPromotionManager/copy_to_survivor) | 6.96% | 预热期 + 小堆,稳态应更低 |
| protobuf(parseFrom/mergeFrom) | 1.27% | Raft 消息反序列化 |
| **Celeborn 自身代码** | **1.27%** | 极低,无显著热点 |

关键帧 inclusive 占比(多线程栈累加,>100% 正常):

| 帧 | inclusive | 性质 |
|----|-----------|------|
| Ratis Netty `invokeChannelRead` | 162.7% | Raft 复制网络入口(多 event loop 线程累加) |
| `NettyRpcService$InboundHandler.channelRead0` | 35.4% | Ratis RPC 入站 |
| `SegmentedRaftLogWorker.run` | 15.2% | Raft 日志落盘 worker |
| `RaftServerImpl.appendEntries` / `appendEntriesAsync` | 15.2% | Raft 追加日志 |
| `CompletableFuture.postComplete` | 10.1% | Ratis 异步 apply 链 |
| `ResourceProtos$ResourceRequest.parsePartialFrom` | 6.3% | protobuf 反序列化 |
| **`AbstractSource.getMetrics`(Prometheus scrape)** | **7.6%** | 指标抓取,master 最大 Celeborn 侧 inclusive |
| `StateMachine.applyTransaction` | 4.4% | Raft 事务 apply |
| `Compile::Compile`(C2) | 22.8% | JIT 预热 |

### 优化点清单

#### P0 Netty leak detection 降级(配置层,与 worker 共性)

→ 见[共性-1](#共性-1-netty-leakdetection-生产开-advanced应降级配置层零代码风险)。master 本次采样无法量化(master 太空闲),但与 worker 同源,长期/高 Raft 流量下有正向收益,且降 GC 压力,零代码风险。**这是 master 侧唯一建议执行的优化。**

#### P1 Prometheus scrape 路径的 metrics 字符串构造(代码层,低优先)

**证据**:`AbstractSource.getMetrics` inclusive **7.6%**,调用栈 `HttpUtils$$anon$1.doGet → PrometheusServlet → AbstractServlet.getMetricsSnapshot → AbstractSource.getMetrics → fillInnerMetricsSnapshot → getGaugeMetrics`。这是 Prometheus 周期性抓取指标时的瞬时开销,不是业务热路径。

**代码位置**(已对照 0.7.0-SNAPSHOT 核实):
- `service/src/main/scala/org/apache/celeborn/common/metrics/sink/AbstractServlet.scala:33-35` — `getMetricsSnapshot`:`sources.map(_.getMetrics).mkString`,每次 scrape 遍历所有 source
- `common/src/main/scala/org/apache/celeborn/common/metrics/source/AbstractSource.scala:647-661` — `getMetrics`:每次重建 StringBuilder,遍历 6 类 metric(timers/histograms/meters/gauges/counters),逐个拼字符串;**注释 645 行明确会清空 timeMetrics 队列**(`:682-683` `reset()` reservoir)
- `AbstractSource.scala:447-458` — `getGaugeMetrics`:每个 gauge `System.currentTimeMillis` + new StringBuilder + `normalizeKey` + `addMetricsWithPrometheusHelpType`
- `AbstractSource.scala:703-705` — `normalizeKey`:`key.replaceAll("[^a-zA-Z0-9]", "_")` **每次调用编译正则,无 Pattern 缓存**

**根因**:Prometheus 文本格式输出的固有开销——每次 scrape 把所有指标序列化成文本。`normalizeKey` 的 `replaceAll` 每次编译正则是可优化的点,但仅在 scrape 路径,master CPU 本就低。

**优化方向**(低优先,可选):
- `normalizeKey` 的正则预编译为 `Pattern` 常量(或缓存),避免每次 scrape 重复编译。
- 若指标数量大且 scrape 频繁,可考虑缓存 metrics name 的序列化结果(name 部分不变,只 value 变)。

**预期收益**:本次 7.6% 是 scrape 瞬时占比,绝对值极小(≈12 samples)。优化更多是代码整洁度收益,性能收益微乎其微。**不建议为此改动**,除非 scrape 频率被调得很高或指标爆炸式增长。

#### 记录项(不建议改动)

**Ratis Raft apply 路径(框架固有)**

**证据**:`StateMachine.applyTransaction` inclusive 4.4%、`SegmentedRaftLogWorker.run` 15.2%、`CompletableFuture.postComplete` 10.1%、protobuf `parsePartialFrom` 6.3%。

**代码位置**:`master/src/main/java/org/apache/celeborn/service/deploy/master/clustermeta/ha/StateMachine.java:177-208` — `applyTransaction` 用 `CompletableFuture.supplyAsync(() -> runCommand(request, trxLogIndex), executorService)` 异步执行(`:190`),`runCommand`(`:217`)→ `metaHandler.handleWriteRequest`。

**判断**:这是 Ratis Raft 复制的核心机制,`CompletableFuture` 异步链 + protobuf 序列化都是框架固有开销,**不在 Celeborn 可控范围**。单线程 executor(`StateMachine.java:96`,注释:为保证多 master 事务顺序一致)是正确性约束,不能改并发。无需优化。

**Worker 心跳处理(正常开销)**

**证据**:`AbstractMetaManager.updateWorkerHeartbeatMeta` 出现在 apply 链路。

**代码位置**:`master/src/main/java/org/apache/celeborn/service/deploy/master/clustermeta/AbstractMetaManager.java:282-350` — `updateWorkerHeartbeatMeta`:`synchronized(workersMap)` 更新 worker 信息 + `totalAvailableSlots` + `updateAvailableWorkers`。

**判断**:每个 worker 心跳(默认秒级)走一次 Raft apply。`synchronized(workersMap)` 是全局锁,但 worker 数量级通常不大(几十~几百),心跳频率低,不构成瓶颈。火焰图中 self 为 0,印证开销极小。无需优化。

**JIT 预热(假象)**

**证据**:`CompileBroker::compiler_thread_loop` inclusive 23.4%、`C2Compiler::compile_method` 22.8%、`PhaseChaitin::Register_Allocate` 10.1%、`PhaseIdealLoop` 系列大量出现。

**判断**:这是 JVM 在采样窗口内对新热点方法做 C2 编译(寄存器分配 `PhaseChaitin`、循环优化 `PhaseIdealLoop` 都是 C2 阶段)。**稳态运行后这部分消失**,不是代码问题。若想避免,可考虑 master 启动后预热(跑典型负载)或用 AOT/CDS,但对 master 这种低 CPU 节点意义不大。

**GC(预热期)**

**证据**:`PSPromotionManager::drain_stacks_depth` self 3.8%、`copy_to_survivor_space` 1.9%。

**判断**:ParallelGC 的 young GC,预热期对象分配多导致。稳态应下降。若长期 GC 明显,可调堆/GC 策略,但本次采样不足以支撑结论。

### 配置层/代码层速查表(master)

| 配置 / JVM 参数 | 当前 | 建议 | 对应项 | 说明 |
|----------------|------|------|--------|------|
| `-Dio.netty.leakDetectionLevel` | `advanced`(`charts/celeborn/values.yaml:109`) | 去掉/SIMPLE | P0(共性-1) | 与 worker 共性,长期有收益 |

| 优化项 | 文件:行号 | 风险 | 预期收益 | 建议 |
|--------|----------|------|----------|------|
| P1 normalizeKey 正则预编译 | `AbstractSource.scala:703-705` | 极低 | 极小(≈scrape 瞬时) | 可选,非必须 |

> master 无 worker 那样的 `appLevel metrics` 高频热路径(recordAppActiveConnection 是 worker 侧),故不涉及 worker 的 P0-1。`celeborn.metrics.sample.rate` 默认 1.0 在 master 侧影响也小(master 无 per-message 热路径)。

### 结论与建议

1. **master 无显著 CPU 瓶颈**。158 samples 的低采样 + JIT 预热期占比高,说明 master 在生产负载下 CPU 充裕,不是性能瓶颈点。这与 master 的职责(元数据管理 + Raft 复制 + slot 分配)一致——它是控制面,非数据面。
2. **唯一建议执行的优化**:与 worker 同步把 Netty leak detection 从 `advanced` 降级(共性-1),零代码风险,消除长期隐患。
3. **不建议为 master 单独做代码改动**。Prometheus scrape 的 `normalizeKey` 正则优化属于代码整洁度,性能收益微乎其微,改了也行不改也行。
4. **若要进一步分析 master**:需在**高负载场景**(如大规模 slot 申请、频繁 worker 心跳、大量 shuffle 注册)下重新采样,采集更长时间(目标 >10000 samples),才能区分稳态热点与预热假象。当前采样不足以支撑更深结论。

### 与 worker 报告的对照

| 维度 | worker | master |
|------|--------|--------|
| 采样量 | 80,611(高负载) | 158(基本空闲) |
| Celeborn 自身代码 self | 5.02% | 1.27% |
| 最大可优化项 | appLevel metrics 关闭(4.4%)+ leak 降级 | leak 降级(共性) |
| 是否数据面 | 是(push/fetch IO 密集) | 否(控制面) |
| 代码层优化价值 | 高(多个 per-message 热点) | 低(无热路径) |
| 共性配置优化 | leak detection 降级 | leak detection 降级 |

---

## 第四章 Worker 内存 Heapdump 分析(生产 0.6.2.3)

### Context

生产集群对 Celeborn worker **0.6.2.3** 做了 heapdump(本地分析时对齐 0.6.2 代码;本章行号引用已全部更新为 0.7.0-SNAPSHOT),MAT 解析出三份报告(`celeborn_Leak_Suspects.zip` / `celeborn_System_Overview.zip` / `celeborn_Top_Components.zip`)。堆转储原始文件 27.8GB,MAT 解析后 used heap **660MB**(3,706,172 对象)。本章结合 MAT 证据与代码,定位 worker 可优化的内存点,给出优先级、根因、验证方法与修复建议。本章为**分析与方案设计**,不含直接改代码(实际修复需另起实现 PR 并配合生产复测)。

### 证据总览:MAT 报告(660MB used heap)

| Problem Suspect | 占用 | 占比 | 核心对象 | 引用根 |
|---|---|---|---|---|
| #2 Netty HeapArena | 244MB | 37.05% | 120 个 `PoolArena$HeapArena` + 1,668 个 `PoolChunk`(279MB) | 120 个 `FastThreadLocalThread` / `PooledByteBufAllocator` |
| #1 LocalFlusher 队列 | 92MB | 13.93% | 1 个 `LocalFlusher@/data1-12` 的 `LinkedBlockingQueue` 堆积 57,751 个 `CompositeByteBuf` + 57,751 个 `Component[]`(85MB) | `LocalFlusher@1928312257-/data1-12` 线程,栈停在 `LinkedBlockingQueue.take()`(生产 0.6.2 对应 `Flusher.scala:72`;0.7.0-SNAPSHOT 为 `:76`) |
| #3 PartitionDataWriter 残留 | 74MB | 11.22% | 18,187 个 `PartitionDataWriter` + 18,187 个 `LocalTierWriter`(58MB) + 18,187 个 `DiskFileInfo`(11.7MB) | `workingDirWriters` 的 `ConcurrentHashMap$Node[262144]` |
| 其他 | — | — | `Recycler$LocalPool` 38MB、Arthas 19MB(→ [共性-3](#共性-3-arthas-诊断工具残留))、Duplicate String `10.238.44.229`×10391 | — |

关键事实:**16 个 `LocalFlusher`**(16 块磁盘 /data1../data16),但只有 `/data1-12` 单个 flusher 队列堆积 92MB → **单块慢盘**问题。`worker-file-sorter-scheduler` 线程只是被 MAT 选中的最短 GC root 路径,sorter 线程本身停在 `take()` 空闲等待,**不是泄漏源**。

### 优化点清单(按优先级)

#### P0-1: Flusher workingQueues 无界 + ByteBuf 释放依赖队列消费【最重要】

**证据**:Problem Suspect #1。单个 `/data1-12` flusher 队列堆积 57,751 个 FlushTask,每 ~256KB 数据产 1 个 task,折算约 14.3GB 待 flush 数据堆积在一个磁盘的队列里。`CompositeByteBuf` + `Component[]` 的 release 完全依赖 flush worker 线程 `take()` + `returnBuffer()`。

**根因**(已核实代码):
- `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/Flusher.scala:67` `workingQueues(index) = new LinkedBlockingQueue[FlushTask]()` — **无界**(默认 `Integer.MAX_VALUE`)。
- `Flusher.scala:50` `bufferQueue` 同样无界。
- `Flusher.scala:138-140` `addTask` 用 `offer(task, timeout, MILLISECONDS)`,但因队列无界,offer 永不阻塞 → **生产者无反压**。
- `Flusher.scala:97,123-135` `returnBuffer` 在 `keepBuffer=true`(`TierWriter.scala:427-431` LocalFlushTask 默认)时把 buffer 归还无界 `bufferQueue`;**只有 task 被 take 出来并 flush 完成才 release**,磁盘慢→堆积→ByteBuf 驻留。
- `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/TierWriter.scala:440` `buf.retain()` 后 component 生命周期延展到 flush 完成磁盘写入。

**修复建议**:
1. `workingQueues` 改有界,新增配置 `celeborn.worker.flusher.queue.capacity`(按磁盘类型区分 HDD/SSD)。`addTask` offer 失败/超时时标记该 file 失败(`notifier.setException`)而非无限堆积。
2. 将 `FLUSH_WORKING_QUEUE_SIZE` gauge(`Flusher.scala:103-106`,已有但仅观测;定义在 `WorkerSource.scala:228`)**接入反压回路**:队列深度超阈值时让 `LocalTierWriter.writeInternal`(`TierWriter.scala:432-449`)在 flush 前阻塞或触发该 file 的 split/失败,实现 **per-disk 隔离反压**。
3. 队列深度超阈值时,新 task 改用 `keepBuffer=false`(直接 release 而非归还 bufferQueue),优先释放内存而非复用。

**验证**:
- 生产先核查 `FlushWorkingQueueSize` metric(按 mountpoint)是否在 `/data1-12` 高涨。
- 修复后压测:对单盘注入 IO 慢(如 `cgroup` 限速 / `dmsetup` delay),验证该盘队列被限流、其他健康盘 push 不受影响、`usedHeapMemory` 不再上涨。

#### P0-2: per-disk 背压缺失,单慢盘可拖垮整个 worker

**证据**:Problem Suspect #1 的 `/data1-12` 单盘堆积 92MB,连带 Netty 直接内存上涨。

**根因**(已核实):
- `worker/src/main/java/org/apache/celeborn/service/deploy/worker/congestcontrol/CongestionController.java` 默认**关闭**(`celeborn.worker.congestionControl.enabled` 默认 `false`,`CelebornConf.scala:4383-4389`),watermark 默认 `Long.MaxValue`(`:4399-4415` 等)。
- 即使开启,`getTotalPendingBytes()` = `MemoryManager.getMemoryUsage()` = **Netty 已用直接内存 + sortMemory**(`MemoryManager.java:460-462`),是**全局聚合**,非 per-disk 队列长度。
- `diskBufferCounter`(`MemoryManager.java:72,447-451`)只在 `writeInternal` 时增、`returnBuffer` 时减(`Flusher.scala:123-135`),同样全局,无法定位单盘。
- 单慢盘队列无界增长 → 撑高全局 Netty 直接内存 → 触发全局 `pausePushDataThreshold`(`MemoryManager.java:310-323`)→ **所有盘 push 被暂停**,健康盘被殃及。
- `LocalFlusher.notifyError`(`Flusher.scala:174-176`)收到错误也"Won't stop LocalFlusher","慢而不失败"的盘不会触发 `DeviceMonitor` 错误路径。

**修复建议**:
1. `diskBufferCounter` / `workingQueueSize` 按 mountPoint 分桶,单盘超阈值只对该盘反压。
2. 基于 `flushTimeMetric`(`Flusher.scala:84-87` 已有 `TimeWindow`)的滑动窗口检测慢盘,自动降级或摘除(类似 `DeviceMonitor` 的 `HIGH_DISK_USAGE`)。
3. 生产短期可先开启 congestion control 并配置合理 watermark(非 Long.MaxValue)。

**验证**:同 P0-1 的单盘限速压测,观察健康盘吞吐不受影响。

#### P1-1: Netty HeapArena 数过多(120 个)+ 无法消除 heap arena

**证据**:Problem Suspect #2 / Top Consumers。120 个 `HeapArena` 占 244MB(37%),1,668 个 `PoolChunk` 占 279MB,1g 堆下偏高。

**根因**(已核实):numArenas 全局 key、无法 per-module、`Math.min` 钳制、heap+direct 双池、header 用 heapBuffer 的机制见[共性-2](#共性-2-numarenas-是全局配置无法-per-module-生效且被-mathmin-钳制)。worker 侧特有:
- MAT 实测 120 个 HeapArena → 节点 availableProcessors≥120 或生产显式配 `numArenas=120`(需核实)。
- `allowCache` 默认 `false`(`CelebornConf.scala:1977-1984`)→ 无 thread-local cache,chunk 走共享池更难回收,PoolChunk 半满未释放(1,668 个)。

**修复建议**:
1. **配参优先**(零代码):设 `celeborn.network.memory.allocator.numArenas` 到与实际 IO 线程数对齐的较低值(16~32),而非 CPU 核数。
2. 评估开启 `celeborn.network.memory.allocator.allowCache=true`(当前默认 false)是否能加速 chunk 回收(需压测权衡)。
3. 代码层面:`NettyUtils.java:171-178` 支持 `preferDirect=true` 时 heapArena 传 0(但需先把 `MessageEncoder.java:84` header 改 direct 或 Unpooled,避免 NPE/退化)。
4. 0.7.0 可考虑 `celeborn.network.memory.allocator.type=ADAPTIVE/UNPOOLED`(`CelebornConf.scala:1986-2001`,0.7.0 新增),灰度验证。
5. 清理冗余:`sbin/start-worker.sh:39` 的 `-Dio.netty.allocator.type=pooled` 无效(见[共性-2](#共性-2-numarenas-是全局配置无法-per-module-生效且被-mathmin-钳制))。

**验证**:
- 生产开 `celeborn.network.memory.allocator.verbose.metric=true`(`CelebornConf.scala:2039-2040`),看 `numHeapArenas` / `pinnedHeapMemory` / `usedHeapMemory`(`common/src/main/java/org/apache/celeborn/common/network/util/NettyMemoryMetrics.java`)实际值。
- 调小 numArenas 后复测 MAT,确认 HeapArena 占比下降、push/fetch 吞吐不退化。

#### P1-2: `workingDirWriters` 无界 + cleanup 路径不完整,僵尸 writer 残留

**证据**:Problem Suspect #3。18,187 个 `PartitionDataWriter`(连 `LocalTierWriter` + `DiskFileInfo`)驻留,`workingDirWriters` 的内层 ConcurrentHashMap table 容量扩到 262144(2^18,单向不缩)。

**根因**(已核实,行号已更新为 0.7.0-SNAPSHOT):
- `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/StorageManager.scala:59-60` `workingDirWriters` 是 `ConcurrentHashMap[File, ConcurrentHashMap[String, PartitionDataWriter]]`,**无界、无 TTL、无 LRU**。注释(第57行)称"fileInfos and partitionDataWriters are one to one mapping"。
- **唯一写入点** `StorageManager.scala:542-562` `registerDiskFilePartitionWriter`(由 `TierWriter.scala:412` LocalTierWriter 构造调用;`:611` DfsTierWriter 同)。
- **移除点** `StorageManager.scala:625-665` `cleanFileInternal`,**只 remove 内层 entry,外层 `workingDir` entry 永不移除**(即便内层空)。
- cleanup 触发**完全依赖 Master 心跳下发 `expiredShuffleKeys`**(`Worker.scala:535-536` 入队、`:818-845` `cleanup` → `StorageManager.cleanupExpiredShuffleKey`(`StorageManager.scala:677`))。Master 在 `appHeartbeatTimeoutMs` 超时或 `ShuffleUnregister` 才标记过期(`Master.scala:708,749-769`),HA 切换期间 `getAppTimeoutDeadline` 还跳过检查。
- **commit/destroy 路径不从 map 移除**:`Controller.scala:372` `commitFiles` 只 `fileWriter.close()`(`:405`)+ `notifyFileInfoCommitted`,不 remove。writer 要等整个 shuffle 过期才被移除。
- **僵尸场景**:LifecycleManager 未发 unregister、driver 卡死但心跳线程活着、client 异常断开未走 finalize → shuffleKey 永不进 expiredShuffleKeys → writer 永久驻留。worker **无任何本地时间兜底**(`StorageManager.scala:741-755` 的 30 分钟定时任务只删磁盘空目录,不清理内存 map)。

**修复建议**:
1. `fileWriter.close()` / `notifyFileInfoCommitted` 后即从 `workingDirWriters` 移除该 writer(而非等整个 shuffle 过期)。
2. 内层 `writers.isEmpty` 时 `workingDirWriters.remove(workingDir)`(清外层空 entry,避免 table 膨胀)。
3. worker 本地增加基于"最后写入时间"的独立过期兜底:在 `StorageManager.scala:741` 的 30 分钟定时任务里扫描 `workingDirWriters`/`diskFileInfos`,移除长期未活跃且已 close 的 entry,不依赖 Master 心跳。
4. `cleanFileInternal` 移除内层 entry 后,若内层空则移除外层。

**验证**:复现 driver 卡死/未 unregister 场景,观察 writer 数量是否随时间收敛而非单调增长;MAT 复测 `PartitionDataWriter` 实例数下降。

#### P2-1: Netty Recycler 未禁用(38MB)

**证据**:Top Consumers。68 个 `Recycler$LocalPool` 占 38MB,Netty 4.2 默认对象池。

**根因**:全仓库无 `-Dio.netty.recycler.maxCapacity.default=0`(`BufferRecycler.java` 是 Celeborn 自身的,无关)。Netty 默认池化 `PooledByteBuf`/`Component` 等。

**修复建议**:`sbin/start-worker.sh` 加 `-Dio.netty.recycler.maxCapacity.default=0` 禁用(代价:对象分配开销略增)。相对 244MB HeapArena 是次要矛盾,排在 P0/P1 之后。

#### P2-2: metric tag / host string 重复(Duplicate Strings)

**证据**:Top Components。`10.238.44.229`(worker host IP)重复 10,391 次(8234+2157),`application_*` Spark app attempt ID 重复数百次,版本串 `2.5.4.*` 上百次。总量 ~2MB,不大但暴露 tag 复用问题。

**根因**:metric label(role=worker, host=ip, app=attemptId)每次注册 gauge 都新建字符串;`2.5.4.*` 疑似 protobuf 相关重复。

**修复建议**:WorkerSource 注册 metric 时对 host/role 等 tag 用 intern 或常量复用;app attemptId 作为 tag 时考虑短哈希。优先级低。

#### P2-3: Arthas 残留(19MB)

→ 见[共性-3](#共性-3-arthas-诊断工具残留)。

### 修复优先级与依赖

```
P0-1 (Flusher 队列有界+反压)  ──┐
P0-2 (per-disk 背压)          ──┼──> 解决 CompositeByteBuf/PoolChunk 堆积主因,复测后 P1-1 的 PoolChunk 279MB 应同步下降
P1-1 (HeapArena 配参/代码)    ──┘
P1-2 (workingDirWriters 清理)  ──> 独立,解决 writer 残留
P2-* (Recycler/tag/arthas)    ──> 收尾,低优先
```

**先验证后改代码**:生产优先核查 `FlushWorkingQueueSize`(per mountpoint)、`numHeapArenas`/`pinnedHeapMemory`、`PartitionDataWriter` 实例数三个 metric 的实际值,确认主因后再决定改代码范围。配参类(P0-1 的 capacity、P0-2 的 congestion watermark、P1-1 的 numArenas)可先于代码上线验证。

### 关键文件索引

| 优化点 | 文件:行号(0.7.0-SNAPSHOT) |
|---|---|
| P0-1 Flusher 队列无界 | `worker/src/main/scala/.../storage/Flusher.scala:50,67,97,103-106,123-140` |
| P0-1 flush 触发/retain | `worker/src/main/scala/.../storage/TierWriter.scala:427-449` |
| P0-2 congestion 默认关 | `worker/src/main/java/.../congestcontrol/CongestionController.java`;`CelebornConf.scala:4383-4468` |
| P0-2 全局内存水位 | `worker/src/main/java/.../memory/MemoryManager.java:310-323,447-462` |
| P0-2 慢盘不停止 | `worker/src/main/scala/.../storage/Flusher.scala:174-176` |
| P1-1 allocator 构造 | `common/src/main/java/.../network/util/NettyUtils.java:171-178` |
| P1-1 numArenas 默认 | `common/src/main/scala/.../CelebornConf.scala:620-622,2012-2018` |
| P1-1 header heapBuffer | `common/src/main/java/.../network/protocol/MessageEncoder.java:84` |
| P1-2 workingDirWriters | `worker/src/main/scala/.../storage/StorageManager.scala:59-60,542-562,625-665,677-736,741-755` |
| P1-2 cleanup 触发 | `worker/src/main/scala/.../Worker.scala:535-536,818-845` |
| P1-2 commit 不移除 | `worker/src/main/scala/.../Controller.scala:372,405` |
| P2-1 Recycler | `sbin/start-worker.sh:39` 附近 |
| 监控 metric | `common/src/main/java/.../network/util/NettyMemoryMetrics.java`;`worker/.../WorkerSource.scala:228,255` |

### 端到端验证方法

1. **metric 先行**:生产开 `celeborn.network.memory.allocator.verbose.metric=true`,采集 `FlushWorkingQueueSize`(per mountpoint)、`numHeapArenas`/`usedHeapMemory`/`pinnedHeapMemory`、`DiskBuffer`,定位主因是 flush 堆积还是 arena 配置。
2. **复现压测**:对单盘注入 IO 慢(`cgroup` blkio 限速 或 `dmsetup delay`),验证 P0-1/P0-2 修复后该盘队列被限流、健康盘不受影响、堆内存不上涨。
3. **僵尸 writer 复现**:构造 driver 卡死/未 unregister 场景,验证 P1-2 修复后 `PartitionDataWriter` 实例数随时间收敛。
4. **MAT 复测**:修复后重新 heapdump,确认 `CompositeByteBuf`/`PoolChunk`/`HeapArena`/`PartitionDataWriter` 占比下降。
5. **回归**:`./build/mvn -pl worker test` + 集成测试 `./build/mvn -Pspark-3.5 -pl tests/spark-it test`;改了 `CelebornConf` 需 `UPDATE=1 build/mvn test -pl common -Dtest=ConfigurationSuite` 重生成文档;提交前 `./dev/reformat`。

---

## 第五章 Master 内存 Heapdump 分析(生产 0.6.2.x)

### Context

生产集群对 Celeborn master **0.6.2.x** 做了 heapdump,MAT 解析出三份报告(`master_celeborn_Leak_Suspects.zip` / `master_celeborn_Top_Consumers.zip` / `master_celeborn_Top_Components.zip`)。MAT 解析后 used heap **~204MB**(1,922,811 对象,13,885 类),远小于 worker 的 660MB。本章结合 MAT 证据、**生产 System Properties**、代码与 ratis 3.2.2(`pom.xml:102`)反编译验证,定位 master 可优化的内存点。

生产 master 关键配置(来自 MAT System Properties,为生产事实;括号内为 0.7.0-SNAPSHOT 对应情况):
- HA 3 节点:`10.252.162.17/101/150`,ratis port 9872
- `celeborn.master.ha.ratis.raft.server.snapshot.auto.trigger.enabled = true`,**threshold = 200000**(与 0.7.0-SNAPSHOT 默认值一致,`CelebornConf.scala:3062-3078`)
- `celeborn.network.memory.allocator.numArenas = 64`(但实际生效 48,被钳制 → [共性-2](#共性-2-numarenas-是全局配置无法-per-module-生效且被-mathmin-钳制))
- `celeborn.rpc.io.serverThreads = 32`,`clientThreads = 32`(0.7.0-SNAPSHOT 已合并为单一 key `celeborn.rpc.io.threads`,`CelebornConf.scala:2055-2056`)
- `celeborn.master.slot.assign.policy = LOADAWARE`(`CelebornConf.scala:3097`)

### 证据总览:MAT 报告(~204MB used heap)

| Problem Suspect | 占用 | 占比 | 核心对象 | 引用根 |
|---|---|---|---|---|
| #1 Netty HeapArena | 101MB | 49.52% | 48 个 `PoolArena$HeapArena`(每个 4.2MB)+ 48 个 `PoolChunk` + 24 个大 `byte[]`(100.6MB,每个 ~4MB) | `PooledByteBufAllocator.DEFAULT`(Ratis shaded netty 静态单例) |
| #2 Ratis RaftLog 积压 | 52MB | 25.55% | `SegmentedRaftLogCache.closedSegments` 持有 5 个 `LogSegment`(每个 ~15MB);含 130,102 个 `LogEntryProto` + 65,061 个 `StateMachineLogEntryProto` + 221,057 个 `TermIndex$1` + 130,120 个 `byte[]` | `StateMachineUpdater` 线程,栈停在 `StateMachineUpdater.waitForCommit()`/`run()` |
| 其他 | — | — | `RetryCacheImpl` 3.5MB、Finalizer 13MB(5274 个,含 ZipFile 2316)、Arthas 7.4MB(→ [共性-3](#共性-3-arthas-诊断工具残留)) | — |

Class Histogram 印证:`byte[]` 148,095 个 131MB(含 Netty 24 个大块 + Ratis 13 万小块)、`LogEntryProto` 130,102 个、`StateMachineLogEntryProto` 65,061 个。

### 优化点清单(按优先级)

#### P0-1: Ratis closedSegments 堆积 —— snapshot 阈值过大 + purgeGap 二次拖延【最重要】

**证据**:Problem Suspect #2。5 个 LogSegment × 15MB = 39MB 未清理,含 13 万条日志条目。生产 `snapshot.auto.trigger.threshold = 200000`,而积压量未达 20 万 → snapshot 不触发 → `purgeUptoSnapshotIndex` 无机会生效 → closedSegments 无限增长。

**根因**(已核实代码 + 反编译 ratis 3.2.2 验证,行号为 0.7.0-SNAPSHOT):
- `CelebornConf.scala:3071-3078` `celeborn.master.ha.ratis.raft.server.snapshot.auto.trigger.threshold` 默认 **200000**(条目数,非字节)。语义:`lastAppliedIndex - snapshotIndex >= threshold` 才 takeSnapshot。Ratis 原生默认 400000,Celeborn 已降到 20 万但仍偏大。
- `CelebornConf.scala:2948-2955` `celeborn.master.ha.ratis.raft.server.log.purge.gap` 默认 **1000000**(100 万条)。语义:snapshot 后,closedSegments 还要等 `snapshotIndex` 落后 `lastAppliedIndex` 超过 100 万条才真正 purge。**这是比 snapshot 阈值大 5 倍的"二次拖延"**。
- `master/src/main/java/.../clustermeta/ha/HARaftServer.java:333` `setPurgeUptoSnapshotIndex(true)` 硬编码(正确方向,但依赖 snapshot 先触发);`:363-364` 透传 purgeGap;`:430-431` 透传 snapshot 阈值。
- `HARaftServer.java:410` `setSegmentCacheNumMax(2)` 硬编码(Ratis 默认 6)——但此值只限制 cache 影子,不限制 `LogSegmentList` 权威列表,无法阻止 closedSegments 堆积。

**修复建议**:
1. **调小 `autoSnapshotThreshold`** 到 50000~100000(`CelebornConf.scala:3071`)。让 snapshot 更频繁触发。
2. **调小 `purgeGap`** 到 10000~50000(`CelebornConf.scala:2948`)—— 这是**最直接针对 MAT 现象的旋钮**。当前 100 万远大于 snapshot 阈值,导致 snapshot 后仍长期不 purge。
3. 若 snapshot 变频繁后怕风暴,透传 `celeborn.ratis.server.snapshot.creation.gap`(ratis 默认 1024)适当调大。
4. 注意:snapshot 有 I/O 成本(全量序列化 workersMap),太频繁会拖慢 apply,需压测找平衡。

**验证**:
- 生产开 ratis metric,观察 `lastAppliedIndex` 与 `snapshotIndex` 差值随时间收敛(而非单调增长至 20 万)。
- MAT 复测 `SegmentedRaftLogCache` / `LogEntryProto` 实例数下降。

#### P0-2: Ratis shaded netty 的 PooledByteBufAllocator.DEFAULT 预分配 48 个 HeapArena【最高 ROI 单一改动】

**证据**:Problem Suspect #1。48 个 HeapArena(24 核 ×2)× 4.2MB = 101MB,占堆 49.5%。master 是控制面,无数据面 buffer,却预分配近一半堆。

**根因**(已核实 + 反编译 `NettyRpcService` 验证):
- **48 个 HeapArena 的主因不是 Celeborn,而是 Ratis**。`CelebornConf.scala:2866-2875` `celeborn.master.ha.ratis.raft.rpc.type` 默认 `"netty"`。
- Ratis 用自带 shaded netty(`org.apache.ratis.thirdparty.io.netty.*`),其 `NettyRpcService` 的 `ServerBootstrap` **未设 `ChannelOption.ALLOCATOR`** → 走 shaded netty 的 `PooledByteBufAllocator.DEFAULT` 静态单例,`defaultNumHeapArena() = 2*cores = 48`。
- 每个 arena 默认 pageSize=8192 + maxOrder=11 → chunk ~16MB,但实际每个 4.2MB(半满预分配),48 个共 ~96MB backing array。这与 MAT 的 24 个 4MB byte[] 完全吻合。
- Celeborn 自己只贡献 24 个 arena(`networkAllocatorArenas=max(cores,2)=24` 限制 + `networkShareMemoryAllocator=true` 全局共享 1 个 allocator)。
- 生产配 `numArenas=64` 但被 `NettyUtils.java:173` `Math.min` 钳制到 48 —— 配置对 Celeborn 侧都没生效(对 Ratis 侧更无影响,Ratis 走自己的 DEFAULT)。详见[共性-2](#共性-2-numarenas-是全局配置无法-per-module-生效且被-mathmin-钳制)。

**修复建议**:
1. **最高 ROI**:`celeborn.master.ha.ratis.raft.rpc.type=grpc`(`CelebornConf.scala:2866-2875`),Ratis 改用 gRPC,不再走 shaded netty DEFAULT,48 个 HeapArena 直接消失,释放 ~96MB。代价:gRPC thread/overhead 不同,需评估 leader 选举/RPC 延迟。
2. 若继续用 netty rpc:透传 ratis 属性限制其 allocator,但 Ratis shaded netty 没暴露 numHeapArenas 配置,只能靠 `-Dio.netty.allocator.numHeapArenas`(shaded netty 读自己的系统属性,需确认 Ratis 是否读取)—— 可靠性不如切 grpc。

**验证**:切 grpc 后 MAT 复测,`PoolArena$HeapArena` 从 48 降至 Celeborn 的 24(或更少),堆占用下降 ~96MB。

#### P1-1: apply 单线程 + workersMap 全局锁拖慢 apply,放大 snapshot 积压

**证据**:Problem Suspect #2 引用线程 `StateMachineUpdater` 栈停在 `waitForCommit()`(park 等待 commit)。apply 慢 → lastAppliedIndex 前进慢 → snapshot 阈值更难达到(闭环放大 P0-1)。

**根因**(已核实,行号为 0.7.0-SNAPSHOT):
- `master/src/main/java/.../ha/StateMachine.java:96` `ThreadUtils.newDaemonSingleThreadExecutor("master-state-machine-executor")` —— **apply 单线程串行**(设计有意,保证多副本一致,参考 Ozone)。
- `master/src/main/java/.../clustermeta/AbstractMetaManager.java` 共 **10 处 `synchronized(workersMap)`**(`:231,241,296,330,380,572,582,600,648,686`;0.7.0-SNAPSHOT 较生产 0.6.2 的 7 处有所增加)。worker 心跳(秒级)走 Raft apply,在全局锁上串行排队;`updateWorkerHeartbeatMeta`(`:282-350`)持锁做 `updateThenGetDiskInfos`、`totalAvailableSlots` 等。
- CPU 火焰图(第三章)已记录 applyTransaction inclusive 4.4%、CompletableFuture.postComplete 10.1%、protobuf parse 6.3%。

**修复建议**:
1. 引入 Striped Executor(按 worker host hash 分片),让不同 worker 的心跳 apply 并行,保持同 worker 内有序(参见 `performance-optimization-analysis.md` 静态分析中的同类建议)。
2. 缩小 `synchronized(workersMap)` 临界区,把 `updateThenGetDiskInfos`/`totalAvailableSlots` 等读操作移出锁或用读锁。

**验证**:压测高并发 worker 心跳,观察 apply 吞吐提升、`lastAppliedIndex` 前进速率、snapshot 触发频率。

#### P1-2: Celeborn 侧 numArenas 全局无法为 master 单独配小

**证据**:Celeborn 自己贡献 24 个 HeapArena。生产配 `numArenas=64` 未生效(被 `Math.min` 钳到 48)。

**根因与机制**:见[共性-2](#共性-2-numarenas-是全局配置无法-per-module-生效且被-mathmin-钳制)。master 侧特有:master RPC 消息都是小控制消息(`RegisterApplicationInfo`/`RequestSlots`/`PbBatchUnregisterShuffles` 等),无大 buffer,24 个 arena 仍偏多。

**修复建议**:
1. 代码层支持 per-module numArenas(见共性-2)。
2. 短期:master 堆调到 2g(`sbin/start-master.sh:27-28` `CELEBORN_MASTER_MEMORY` 默认 1g),或显式 `-XX:MaxDirectMemorySize=256m`。
3. 清理冗余:`sbin/start-master.sh:34` 的 `-Dio.netty.allocator.type=pooled` 无效(见共性-2)。

**验证**:改后 MAT 看 Celeborn 侧 arena 数下降;worker 侧压测确认不受影响。

#### P2-1: RetryCache expiry 600s 偏长(3.5MB)

**证据**:Top Consumers。`RetryCacheImpl` 3.5MB。

**根因**(已核实):`CelebornConf.scala:2966-2973` `celeborn.master.ha.ratis.raft.server.retrycache.expirytime` 默认 600s(10 分钟)。Ratis server 自带重试缓存,高 QPS 写入(worker 心跳)10 分钟窗口累积显著。

**修复建议**:调到 60~120s,顺带释放 ~3MB。次要矛盾。

#### P2-2: Finalizer 5274 个 / ZipFile 2316 个(13MB)

**证据**:Top Components。`<system class loader>` 5049 个 Finalizer,`ZipFile$ZipFileInflaterInputStream` 2316 个。

**根因**(已核实):
- `bin/celeborn-class` 把 `master-jars`(50+ jar)放 classpath,`URLClassLoader` 扫 jar(`ServiceLoader`、`getResources("META-INF/...")`、ratis SPI、Log4j2/SLF4J 扫描)打开 ZipFile,finalizer 回收时 native Inflater `end()` 排队。
- master 自身无业务 `new ZipFile`;arthas agent 也加载 jar 贡献一部分(→ [共性-3](#共性-3-arthas-诊断工具残留))。
- Finalizer 线程优先级低、native 回收慢 → 堆积。**不一定泄漏,但拖慢 GC**。

**修复建议**:下线 arthas;若要根治,减少 classpath jar 或改用 `URLClassPath` 缓存。优先级低。

#### P2-3: Arthas 残留(7.4MB)

→ 见[共性-3](#共性-3-arthas-诊断工具残留)。

### 修复优先级与依赖

```
P0-2 (Ratis rpc=grpc, 消除 48 arena)  ──> 独立,直接释放 ~96MB,最高 ROI
P0-1 (snapshot 阈值/purgeGap 调小)    ──> 解决 Ratis 日志积压 52MB
P1-1 (apply Striped Executor)          ──> 放大 P0-1 效果(apply 快 → snapshot 更易触发)
P1-2 (numArenas per-module / 堆调大)   ──> Celeborn 侧 arena 收敛
P2-* (RetryCache/Finalizer/arthas)    ──> 收尾
```

**先验证后改代码**:生产优先核查 ratis metric(`lastAppliedIndex - snapshotIndex` 差值、`closedSegments` 数)、`numHeapArenas` 实际值。配参类(P0-1 阈值、P0-2 rpc.type、P2-1 retrycache)可先于代码上线验证;P1-1/P1-2 需改代码。

### 关键文件索引

| 优化点 | 文件:行号(0.7.0-SNAPSHOT) |
|---|---|
| P0-1 snapshot 阈值 | `common/src/main/scala/.../CelebornConf.scala:3071-3078` |
| P0-1 purgeGap | `common/src/main/scala/.../CelebornConf.scala:2948-2955` |
| P0-1 purgeUptoSnapshotIndex(硬编码 true) | `master/src/main/java/.../ha/HARaftServer.java:333` |
| P0-1 segmentCacheNumMax(硬编码 2) | `master/.../ha/HARaftServer.java:410` |
| P0-1 Ratis 属性翻译全部生效点 | `master/.../ha/HARaftServer.java:314-438`(含 :333、:363-364、:410、:430-431) |
| P0-2 Ratis rpc.type 默认 netty | `common/src/main/scala/.../CelebornConf.scala:2866-2875` |
| P1-1 apply 单线程 executor | `master/.../ha/StateMachine.java:96` |
| P1-1 applyTransaction | `master/.../ha/StateMachine.java:177-208` |
| P1-1 workersMap 全局锁(10 处) | `master/.../clustermeta/AbstractMetaManager.java:231,241,296,330,380,572,582,600,648,686` |
| P1-1 takeSnapshot | `master/.../ha/StateMachine.java:256-297` |
| P1-2 numArenas 全局 key | `common/src/main/scala/.../CelebornConf.scala:2012-2018,620-622` |
| P1-2 allocator 构造 | `common/src/main/java/.../network/util/NettyUtils.java:171-178` |
| P1-2 module fallback | `common/src/main/scala/.../CelebornConf.scala:1709-1719` |
| P1-2 header heapBuffer | `common/src/main/java/.../network/protocol/MessageEncoder.java:84` |
| P2-1 RetryCache expiry | `common/src/main/scala/.../CelebornConf.scala:2966-2973` |
| P2-2 master 启动/JVM opts | `sbin/start-master.sh` |
| Master Ratis 启动 | `master/src/main/scala/.../Master.scala:157`(haEnabled 分支);`master/.../ha/MetaHandler.java:54` 实例化 `HARaftServer` |
| Ratis 版本 | `pom.xml:102` = 3.2.2 |

### 端到端验证方法

1. **ratis metric 先行**:生产采集 `lastAppliedIndex` - `snapshotIndex` 差值、`closedSegments` 数、`LogEntryProto` 实例数,确认 P0-1 主因。
2. **grpc 切换验证**:测试环境切 `celeborn.master.ha.ratis.raft.rpc.type=grpc`,验证 leader 选举、3 节点 HA、worker 注册/心跳/slot 分配正常,MAT 复测 HeapArena 下降。
3. **snapshot 频率**:调小 `autoSnapshotThreshold`/`purgeGap` 后,观察 closedSegments 随时间收敛、磁盘 ratis storage 目录 snapshot 文件正常滚动。
4. **apply 并发**:P1-1 改造后压测多 worker 高频心跳,观察 apply 吞吐、CPU flamegraph 中 applyTransaction 占比。
5. **MAT 复测**:全部修复后重新 heapdump,确认 `PoolArena$HeapArena`/`SegmentedRaftLogCache`/`LogEntryProto` 占比下降,堆占用回归合理水位(1g 堆下 master 控制面 <30% 为健康)。
6. **回归**:`./build/mvn -pl master test`;改了 `CelebornConf` 需 `UPDATE=1 build/mvn test -pl common -Dtest=ConfigurationSuite` 重生成文档;提交前 `./dev/reformat`;HA 相关需 3 节点集成测试。

---

## 附录:解析脚本与中间产物

CPU 火焰图解析(worker / master 共用一套):

- 火焰图解析器:`/tmp/parse_flame.mjs`(true stack-reconstruction,可复用;解析 async-profiler canvas 压缩格式 `cpool` + `n()/u()/f()` DFS 栈序列,`key>>>3` 为 cpool 下标、`width` 为子树 inclusive 采样数)
- self 排名:`/tmp/flame_self.tsv`(master 复跑时已覆盖为 master 数据)
- inclusive 排名:`/tmp/flame_inclusive.tsv`(同上)
- 调用栈路径追踪:`/tmp/path.mjs <html> <frame-name>`

Heapdump 解析:MAT(Eclipse Memory Analyzer)产出 `Leak_Suspects` / `Top_Consumers`(或 `System_Overview`)/ `Top_Components` 三份报告 + Class Histogram + System Properties(生产配置来源)。

## 交叉引用

- `performance-optimization-analysis.md` — 静态代码分析的性能优化点(与本报告的 Striped Executor、apply 锁优化等建议呼应)
- ~~`uniffle-port-analysis.md`~~ — **该文档已不存在于仓库**（原计划的 Uniffle→Celeborn 可移植性盘点未成文）
