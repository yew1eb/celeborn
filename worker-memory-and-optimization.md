# Celeborn Worker 内存与资源优化（根因分析 + 优化清单）

> **基线**：生产 `apache-celeborn-0.6.2.2`（build 4a767d22）｜ 对照本仓库 0.7.0-SNAPSHOT（HEAD `bedad1660`，机制一致、行号略有偏差）
> **数据**：2026-09-13 采集的三份 profile —— CPU 火焰图（async-profiler，221,898 样本）、分配火焰图（alloc，8,636 样本）、2.4 GB 堆转储 + MAT Leak Suspects
> **状态**：根因已确认，P0 项待实施
>
> 本文由 `worker-memory-analysis.md`（高内存水位根因分析）与 `worker-resource-optimization.md`（第二轮优化清单）合并而成：**第一部分回答"为什么高"，第二部分只写第一轮之后新发现的增量优化项**（边界见第二部分附录 A）。两部分各自独立编号。
>
> **生产环境**（dump 内 System Properties）：单 SSD 存储目录（`celeborn.worker.storage.dirs=/data1/hard_disk/0:disktype=SSD`），`flusher.ssd.threads=16`，`flusher.buffer.size=512k`，`network.memory.allocator.numArenas=64`，push/fetch IO 线程各 64，JDK 1.8.0_242（**未启用压缩指针**）。

---

## 0. 摘要

### 0.1 一句话根因

Worker 的内存高水位由"**无界缓冲对象驻留**"叠加"**writer 生命周期绑定 shuffle 生命周期**"造成——堆内是 Flusher 无界回收池的 78 万个 CompositeByteBuf 壳（1.23 GB）+ 11.3 万个驻留 PartitionDataWriter（382 MB）；堆外（真实水位判定的依据）是历史峰值期间 flush 积压 + 大量 writer 各持一个 direct flushBuffer 所钉住的 Netty direct memory。此外 Prometheus 抓取产生全 Worker 约 1/3 的堆分配，持续放大 GC 压力。

### 0.2 堆内贡献度（retained，占 2.3 GB used heap）

| 排名 | 对象 | Retained | 占比 | 性质 |
|---|---|---|---|---|
| 1 | `LocalFlusher.bufferQueue`（781,062 个 CompositeByteBuf 壳） | 1,233,157,640 B | **49.24%** | 无界池，只增不减（P0） |
| 2 | `PartitionDataWriter` × 113,033（含 LocalTierWriter/路径字符串/各自 flushBuffer 组件） | 382,550,656 B | **15.28%** | 生命周期绑定 shuffle 过期（P1） |
| 3 | Netty `PoolArena$HeapArena` × 120 + `PoolChunk` × 6,102 | ~400 MB | ~16% | Netty 堆内池（读路径/sort 等），正常驻留 |
| 4 | 其他（byte[]、String、CHM 结构等） | ~490 MB | ~19.5% | 业务对象 |

**堆外**（不在 dump 中，但才是 MemoryManager 水位判定依据）：`getMemoryUsage() = Netty usedDirectMemory + sortMemory`（`MemoryManager.java:460-462`），阈值 pause push 0.85 / pause replicate 0.95 / resume 0.70。构成 = DiskBuffer（flush 积压）+ readBuffer + 在途网络 buffer + **113K writer 各自持有的 direct flushBuffer** + pooled arena 缓存。需生产补采数据定量（见第一部分 §4）。

**CPU 无异常热点**：93% 在 Netty event loop 数据面（收包 20.5%、push 应答发送 8.2%、flusher 写盘 writev 9.5%、fetch pread+send 8%），另有 ~4% G1 remembered-set 维护开销（对象多 + 分配快的直接证据）。

### 0.3 Top 优化点一览

> 编号对应第二部分；"0.6.2.2"列为该版本是否已具备。

| 优先级 | 优化项 | 类型 | 预期收益 | 风险 | 0.6.2.2 | 上游状态 |
|---|---|---|---|---|---|---|
| **P0** | §1.1 开启 `worker.pushdata.mergeBuffer`（0.6.2.2 自带开关） | **纯配置** | 缓解小 push 钉 chunk 的堆外碎片 | 低 | ✅ 已有 | CELEBORN-2152 已合入 0.6.2 |
| **P0** | §1.2 修正 `resume(0.2) < readBufferRatio(0.35)` 配置冲突 | **纯配置** | 消除"永远无法 resume"风险 | 低 | ✅ | — |
| **P0** | §2 `ReadBufferDispatcher` 误用 pooled **heap** buffer | 一行代码 | 读 buffer 池（上限 35G）从堆内挪回堆外配额体系 | 中（需灰度） | ✅ 一行 | **main 未修** |
| **P1** | §3.1 onTrim 加排序/限量/closed 剔除 | 小补丁 | 泄压效率↑、小写 IO 放大↓ | 低 | ✅ | **main 未做** |
| **P1** | §3.2 cleanup 单 Runnable → 分片到 64 线程池 | ~10 行 | 大批量过期清理吞吐数量级提升 | 低 | ✅ | **main 未做** |
| **P1** | §4.1 per-writer 轻量化组合拳（3.4KB→~1.5KB） | 中补丁 | 113K writer 场景省 ~200MB 堆 | 中 | ✅ | 部分有 open PR #3825 |
| **P2** | §4.2 lazy takeBuffer + idle flush | 小-中补丁 | 空 writer 省堆；长尾 writer 释放 direct | 中 | ✅ | main 未见 |
| **P2** | §5.1 metrics 写入侧：固定 flusher key、metricName 缓存 | 单行级 | 消除 Random CAS + 每 push 2 次字符串分配 | 极低 | ✅ | main 未修 |
| **P2** | §5.2 committedFileInfos 全量落盘 → dirty-only/大间隔 | 配置/小补丁 | 消除每周期全量序列化 11 万 FileInfo + WAL 写放大 | 低 | ✅ | main 未修 |
| **P2** | §6 fetch/push 应答 header duplicate churn（alloc ~11%）+ MapPartitionDataReader 账外 direct buffer | 小补丁/随 §7 解决 | 降分配噪声 + 消除配额不对称 | 低 | ✅ | main 未修 |
| **已实锤** | §7 生产 0.6.2.2 **不含** CELEBORN-2301，回 port 后 fetch 零拷贝 + 顺带消除 §6 churn | 回 port | 省一次用户态拷贝 + ~11% alloc | 低 | 需回 port | 上游已合入 |

---

# 第一部分 根因分析（2026-09-13 三份 profile）

## 1. 发现详述

### F1（P0，确认）：`Flusher.bufferQueue` 无界回收池 —— 1.23 GB 堆壳

**数据证据（MAT）**：
- 单个 `LocalFlusher`（`/data1-15`，即唯一存储目录的第 15 号线程所属 flusher）retained **1,233,157,640 B (49.24%)**。
- 构成：`CompositeByteBuf$Component[]` × 781,062（1.14 GB）+ `CompositeByteBuf` × 781,062（68.7 MB）+ `LinkedBlockingQueue$Node` × 787,327（25 MB）。平均 **~1.6 KB/壳**。
- 抓取瞬间该 flusher 线程正停在 `Flusher.returnBuffer` → `CompositeByteBuf.removeComponents` → Netty `PoolChunk.collapseRuns`（归还组件时做 chunk 合并）——说明高峰期 flusher 线程还要额外承担大量 direct buffer 归还的 CPU 开销。
- 全堆 `CompositeByteBuf` 总数 978,831（shallow 86 MB，Component[] 1.2 GB），其中 78.1 万在该池内，其余 ~19.7 万为在飞 flushBuffer / writer 持有。

**代码机制（逐行核实）**：
1. `Flusher.scala:50`：`protected val bufferQueue = new LinkedBlockingQueue[CompositeByteBuf]()` —— **无界**（容量 = Integer.MAX_VALUE）。
2. `Flusher.scala:115-121` `takeBuffer()`：先 `poll()`，空则 `allocator.compositeDirectBuffer(maxComponents)` 新建（`maxComponents = celeborn.worker.push.compositeBuffer.maxComponents`，默认 128，`CelebornConf.scala:4690-4701`）。
3. `Flusher.scala:123-136` `returnBuffer()`（生产版本第 126 行，即 MAT 定位行）：摘组件 + `clear()` 后，`keepBuffer=true` 时**无条件** `bufferQueue.put(buffer)`。
4. `keepBuffer` 生产路径**全部硬编码为 true**：`TierWriter.scala:429`（LocalFlushTask）、`:630/:634/:644`（HDFS/OSS/S3）、`:497/:752`（close/destroy）。仅单测传 false。
5. 组件数组不收缩：`removeComponents` + `clear()` 只摘组件，CompositeByteBuf 内部 `Component[]` 保留峰值容量（128 槽 × 8 B 引用（无压缩指针）+ 数组头 ≈ 1 KB），这是 1.6 KB/壳 的主要构成。
6. **没有任何 trim/上限/清理**：全模块无 drain/clear；Worker 停止时 flusher 无线程 shutdown（daemon 线程随 JVM 消亡），`stopFlag` 生产代码从不置位。
7. 雪上加霜：`workingQueues`（`Flusher.scala:67`）同样无界，`addTask` 仅 `offer`（`Flusher.scala:138-140`），磁盘慢时 FlushTask 无限积压——**积压期间每个 task 钉住一个真实 direct buffer（512 KB 上限），这是堆外尖峰的直接来源**；积压消化后壳全部入池**永久驻留堆内**。
8. memory pressure 的 `onTrim → flushFileWriters`（`StorageManager.scala:941` → `:904-916`）反而对**全部** writer 强制 flush，每个 writer 每 512 KB 产一个 task，进一步推高在飞 buffer 峰值。

**稳态模型**：`bufferQueue.size ≈ 该 flusher 生命期内"同时在飞 CompositeByteBuf 数"的历史峰值`（poll-or-new + 无条件 put ⇒ 高水位只增不减）。78.1 万 = 历史某次磁盘抖动/内存压力期间，113K writer × 每 writer 数个积压 task 的在飞总量。

> 与既有文档的关系：`production-performance-analysis.md`（P0-1，基于上一次 dump 的 92 MB 堆积）与 `performance-optimization-analysis.md` §3.5.5 已指出同一问题。**本次数据证明该问题在新 dump 中恶化了一个数量级（92 MB → 1.23 GB），且至今未修复（上游 main 分支代码结构相同）。**

### F2（P1，确认）：113,033 个 PartitionDataWriter 驻留 —— 382 MB 堆 + 钉住 direct memory

**数据证据（MAT）**：
- `PartitionDataWriter` × 113,033，retained 382 MB（含 `LocalTierWriter` × 113,033 shallow 21.7 MB）；1:1 对应 `DiskFileInfo` × 113,033（47 MB）。
- 被一个 `ConcurrentHashMap$Node[2097152]`（2M 桶，22 MB）持有。
- 其 retained 子图含：`char[]` × 449K（58.6 MB，文件路径/writerString 字符串）、`CompositeByteBuf$Component[]` × 154K（51 MB）、**`PooledUnsafeDirectByteBuf` × 373K-542K（47-91 MB 堆壳，每个都钉住一块 direct memory）**。

**代码机制（逐行核实）**：
- 存放 map：`StorageManager.scala:59-64`（`workingDirWriters` 内层 map，key = 文件绝对路径）。
- **注册**：writer 创建即 put（`StorageManager.scala:542-562`，ReserveSlots 时每个 partition location 一个）。
- **移除只有两条路**：① shuffle 过期（`StorageManager.scala:677-736`，依赖 Master 心跳下发 expiredShuffleKeys，判定标准是 app/shuffle 已在 Master 注销）；② DestroyWorkerSlots（`Controller.scala:768-797`）。
- **commit/close 不移除**（`Controller.scala:403-405` 只 close；`StorageManager.scala:1042-1047` 只记 committedFileInfos）——**上游 main 分支行为相同，属设计使然，不是泄漏 bug**。
- 2M 桶容量 ⇒ 该 map **历史峰值超过 ~157 万个 writer**（CHM 只扩不缩），当前 113K 是清理后水位。
- 每个活跃 writer 构造即 `takeBuffer()` 持有一个 `compositeDirectBuffer`（`TierWriter.scala:68,215-229`），flush 后立即再取新 buffer（`:208`）。**11.3 万 writer × 每个最多 512 KB direct flushBuffer ⇒ 理论 worst case 钉住 ~55 GB direct memory；即使平均只装 1/10，也是 GB 级**——这是堆外水位最可能的结构性大头之一。MAT 中 suspect-2 子图的 542K 个 `PooledUnsafeDirectByteBuf`（每 writer ~4.8 个组件）与此吻合。**注**：第二部分 §4.2 对该表述有勘误——构造时取到的是 0 组件空壳，不分配 direct，真实占用按"已写未 flush 的数据量"计。

**规模合理性**：113K ≈ Σ(partition 数 × 副本 × split 文件 × 长生命周期 app 的历史 shuffle 数)。放大因素：大量小 partition、`push.replicate.enabled`、HARD_SPLIT、应用长寿/未正常注销（Master 不判过期则永不清）。

**dump 实测分布**（对 dump.hprof 全量扫描 UTF-16BE 路径字符串 `/data1/hard_disk/0/celeborn-worker/shuffle_data/<appId>/<shuffleId>/`，221,163 次命中 ≈ 2 条/文件）：

| 指标 | 数值 |
|---|---|
| 涉及 app 数 | **777 个**（同一 YARN 集群 1778763010219） |
| 涉及 shuffle 数（app/shuffle 组合） | **4,374 个** |
| 平均每 shuffle 在本 worker 的文件数 | ~26 个 |
| Top app 驻留 | 最多 93 个 shuffle / 10,250 个文件（application_..._14797180_1） |

结论：**驻留不是少数巨型 shuffle 造成，而是 777 个 app × 4,374 个未过期 shuffle 的累积**。单 shuffle 分摊（~26/worker）完全正常；异常点在于未过期 shuffle 总量——需用 `yarn application -list` 对比真实存活 app 数、并查 Master `RegisteredApplicationCount/RegisteredShuffleCount` 区分"业务真实规模"vs"已结束 app 元数据残留（清理滞后）"（排查步骤见第一部分 §4）。

### F3（确认）：CPU 火焰图 —— 数据面正常，GC 痕迹明显

按 self 样本归类（total = 221,898）：

| 分组 | 占比 | 主要 self 帧 |
|---|---|---|
| Netty 网络 IO 底层 | 30.8% | `__libc_recv` 20.5%、PoolChunk.allocateRun 0.9% |
| PUSH 数据面（PushDataHandler） | 29.5% | `__send`（应答）8.2%、`ConcurrentHashMap.get` 2.4%、`LongAdder.add`（metrics 计数）1.8% |
| FLUSHER 刷盘线程 | 16.2% | `__writev`（写盘）9.5%、`CompositeByteBuf.nioBufferCount` 1.2%、Netty chunk 归还（IntPriorityQueue）0.5% |
| FETCH 数据面 | 10.4% | `__pread`（读盘）4.6%、`__send` 3.4% |
| **G1 GC 相关** | **~4%** | `OtherRegionsTable::add_reference` 2.8%、`RSHashTable` 0.5%、`G1BlockOffsetArray` 0.5% |
| Netty 传输层（解码/心跳） | 4.5% | TransportFrameDecoder 0.3% 等 |
| METRICS 采集 | 0.17% | CPU 占比可忽略 |
| 其他（JVM 空闲线程等） | ~4% | — |

结论：无自旋/锁竞争/异常构造等病态热点；IO 系统调用占大头属正常数据面。**值得注意的两点**：① ~4% 的 G1 remembered-set 维护是"对象数量多（14.2M）+ 分配速率高"的直接代价，与 F1/F2/F4 互为因果；② PUSH 路径上 `LongAdder.add` + `CHM.get` + `recordAppActiveConnection` 合计 ~5%，是 per-push metrics 开销。

### F4（确认）：Alloc 火焰图 —— metrics 采集是分配 churn 首因

按 self 样本归类（total = 8,636）：

| 分组 | 占比 | 主要分配类型 |
|---|---|---|
| **METRICS 采集（PrometheusServlet）** | **36.8%** | char[] 16.3%、java.lang.Long 6.6%、scala cons 5.0% |
| PUSH 数据面 | 15.7% | char[] 5.3%（shuffleKey/路径字符串）等 |
| FETCH 数据面 | 15.7% | **DirectByteBuffer 10.8%**（读盘 buffer） |
| Netty 传输层 | 10.5% | char[]、UTF_8 Decoder 等 |
| FLUSHER | 5.0% | DirectByteBuffer 2.9% |
| 其他 | ~16% | — |

**机制（代码核实）**：Prometheus 每次抓取全量重建文本，**无缓存、无最小间隔**（`AbstractServlet.scala:33-35` → `AbstractSource.scala:647-688`）：每次抓取对 ~25 个 timer 各分配 32 KB snapshot 数组（`ResettableSlidingWindowReservoir.scala:48-54`，窗口 4096）并排序，全量 CHM `values().toList` 物化，每 metric 名正则归一化。运行期 `doStopTimer` 每 4096 次 update 预生成 12 行文本入队（`AbstractSource.scala:336-340`），高 QPS 下持续产出。抓取频率完全由外部 Prometheus `scrape_interval` 决定。**注册表规模本身不大（数百~2000 条，无 per-partition 注册）**，分配来自"每次抓取的全量重建 + timer snapshot 数组"。

**交叉印证**：F4（分配快）+ F1/F2（对象多）→ F3 中的 G1 开销 ~4% → 堆内高水位观感。metrics 本身不是 retained heap 主因，但它是**分配速率**主因，直接驱动 young GC 频率与写屏障开销。

---

## 2. 根因定级（假设验证矩阵）

| ID | 假设 | 结论 | 置信度 | 关键证据 |
|---|---|---|---|---|
| H1 | `Flusher.bufferQueue` 无界，堆壳只增不减 | **确认，P0** | 高（数据+代码双证） | MAT 49.24% + Flusher.scala:50/115-136 + TierWriter 全路径 keepBuffer=true |
| H2 | 113K writer 是"commit 不移除、shuffle 过期才移除"的设计性驻留，非泄漏 bug | **确认，P1** | 高 | MAT 15.28% + StorageManager.scala:59-64/542-562/677-736 + 上游行为一致；2M 桶证明历史峰值 >1.5M |
| H3 | 真实水位大头在堆外（hprof 不可见） | **机制确认，定量待补采** | 中-高 | MemoryManager.java:460-462 水位=Netty direct；writer 人均一个 direct flushBuffer + flush 积压 + arena 缓存 |
| H4 | metrics 采集是分配 churn 首因 | **确认，P2** | 高 | alloc 36.8% + AbstractSource 全量重建/32KB timer snapshot/无缓存 |

**因果链总览**：

```
业务侧: 大量小 partition × 副本 × 长寿 app
   ├─→ 11.3 万驻留 writer (F2) ──→ 堆 382MB + 每 writer 钉住 direct flushBuffer ──→ 堆外水位↑
   ├─→ flush 需求大, 磁盘抖动时 workingQueue 无界积压 ──→ 在飞 direct buffer 尖峰 ──→ 堆外水位↑ (触发 pause push)
   │      └─→ 积压消化后 78 万壳入无界 bufferQueue (F1) ──→ 堆 1.23GB 永久驻留
   └─→ Prometheus 高频抓取 (F4) ──→ 分配 churn 36.8% ──→ young GC 频繁 + G1 写屏障 ~4% (F3)
```

---

## 3. 修复与调优建议（分层）

### 3.1 立即可做（配置/运维，无需发版）

| # | 措施 | 预期收益 | 风险/回滚 |
|---|---|---|---|
| 1 | 调大 Prometheus `scrape_interval`（如 15s→60s），去掉重复抓取源 | 线性削减 36.8% 的分配 churn | 指标粒度变粗；回滚即改回 |
| 2 | `celeborn.metrics.timer.slidingWindow.size` 4096→512；`celeborn.metrics.sample.rate` 适当下调 | 每次抓取每 timer 的 snapshot 数组 32KB→4KB；timer 预生成文本频率 /8 | percentile 窗口变小；随时可调 |
| 3 | 若不需要 per-app 连接数：`celeborn.metrics.worker.appLevel.enabled=false`；保持 `metrics.worker.app.topResourceConsumption.count=0` | 降高基数 counter 与 CPU（recordAppActiveConnection ~1%） | 失去 app 维度指标 |
| 4 | 核对 `CELEBORN_WORKER_OFFHEAP_MEMORY`（默认仅 1g！）与容器 limit、`-Xmx` 的配比；对 `DirectMemoryUsageRatio`、`NettyMemory` 建告警 | 明确堆外上限，避免 RSS 超限被 OOM-kill | 需滚动重启 |
| 5 | 应用治理：排查长寿/未正常注销的 app（Master 不判过期 → writer 永驻）；评估大 reduce 分区数小文件的 shuffle、replicate、split 放大 | 从源头降低 writer 驻留量（F2） | 业务侧工作 |
| 6 | JVM：当前**未启用压缩指针**（堆 >32G 或显式关闭）。若 -Xmx >31G，评估降到 31G 以下启用 compressed oops | Node[]/Component[] 等引用占用减半（2M 桶 CHM 22MB→11MB，1.6KB/壳→~1.1KB/壳） | 堆变小需验证 GC；滚动重启 |

### 3.2 短期（代码修复，fork 内落地；与既有文档 P0-1/P0-2 一致）

| # | 措施 | 位置 | 预期收益 | 风险 |
|---|---|---|---|---|
| 1 | **bufferQueue 有界化**：容量 = `threadCount × N`（N 可配置，如 32），满则 `buffer.release()` 丢弃 | `Flusher.scala:50,131-133` | 直接消除 F1（1.23 GB 堆壳封顶为 线程数×N×1.6KB，16 线程×32 ≈ 0.8 MB） | 低；池 miss 时新建 buffer，等价于现状的 cold path |
| 2 | **workingQueues 有界化 + 反压**：`celeborn.worker.flusher.queue.capacity`，offer 失败时 `notifier.setException` 而非无限积压 | `Flusher.scala:67,138-140` | 消除堆外尖峰的结构性来源 | 中；需定义失败语义（文件标记失败/split），参考 `production-performance-analysis.md` P0-1 |
| 3 | 队列深时 keepBuffer 降级为 false | `TierWriter.scala:429` 等 + Flusher 暴露队列深度 | 高水位期优先释放内存 | 低 |
| 4 | metrics 抓取侧加 TTL 缓存（如 5s 内复用 snapshot） | `AbstractServlet.scala:33-35` | 抓取频率与开销解耦 | 低 |
| 5 | commit 后从 writer map 移除（writer 对象随 committedFileInfos 支持 fetch） | `Controller.scala:403-405`、`StorageManager.scala:542-562/1042-1047` | F2 驻留时长从"app 生命周期"降到"文件写入期" | **高**；需处理 DeviceMonitor 观察者、memory-pressure flush 对已移除 writer 的依赖，建议设计评审后做 |

### 3.3 长期（上游/架构）

1. 将 #1/#2 提交 Apache Celeborn 上游 JIRA/PR（上游 main 分支同样无界，属共性缺陷）。
2. 开启 `celeborn.network.memory.allocator.verbose.metric=true` 采集 per-arena `numActiveBytes`，建立 "pinned vs cached" 长期观测。
3. 灰度开启 Netty leak detection（`-Dio.netty.leakDetection.level=advanced`）验证 fetch/push 路径无 buffer 泄漏。
4. 评估 writer 轻量化（路径字符串去重/驻留、chunkOffsets 紧凑存储）降低 F2 单实例 ~3.4 KB。

---

## 4. 生产侧补采清单（验证 H3 定量，一次性）

**Metrics（Grafana）**：
1. `NettyMemory` vs `-XX:MaxDirectMemorySize` —— 水位本体；
2. `DiskBuffer` + `BufferStreamReadBuffer` + `SortMemory` + `MemoryFileStorageSize` 与 `NettyMemory` 的**差值** = arena 缓存 + 在途 buffer + writer 空 buffer 钉住；差值大即坐实结构性驻留；
3. `PausePushData` / `PausePushDataAndReplicate` / `PausePushDataTime` —— 是否真实触发暂停及频率；
4. `FlushWorkingQueueSize`（per mountpoint）、`FlushLocalDataTime` —— flush 积压证据；
5. `ReadBufferUsageRatio`、`ActiveChunkStreamCount`、`ActiveCreditStreamCount`。

**日志**：MemoryManager 每 10s 打印的 `Direct memory usage: x/y, disk buffer size...`；`Trigger action: PAUSE PUSH` / `Serving state transformed`。

**进程级命令**：
```bash
jcmd <pid> VM.native_memory summary        # 需 -XX:NativeMemoryTracking=summary
jcmd <pid> GC.run                           # 观察 NettyMemory 是否回落（区分泄漏 vs arena 驻留）
cat /proc/<pid>/status | grep VmRSS         # RSS 对账
grep -r "OFFHEAP_MEMORY\|MaxDirectMemorySize" <部署目录>/  # 确认实际上限
```

**双时间点 dump 对比（实锤 F1）**：高峰后 vs 低谷各抓一次 dump，对比 `LocalFlusher.bufferQueue` size —— 单调不减即证实"只增不减"。

**F2 驻留归因排查（777 app / 4,374 shuffle 是否正常）**：
```bash
yarn application -list | wc -l                                   # 真实存活 app 数 vs 777
yarn application -status application_<cluster>_<seq>_1            # 抽查 top app 是否已结束
curl -s http://<master>:9098/metrics/prometheus | grep -iE "registeredApp|registeredShuffle"
grep -i "expired" <worker.log> | tail                            # 过期清理是否持续发生
```
若大量 app 已 FINISHED/KILLED 但元数据仍在 → 查 Master app 心跳超时配置与僵尸 driver；若 app 真实存活 → 属业务规模问题，按第一部分 §3.1-4 降预算 + §3.2-5 代码改进应对。

**MAT 中可进一步执行的 OQL（附录 A）**：按 appId 分组统计 writer 数，定位治理对象（第一部分 §3.1-5）。

---

## 5. 附录

### A. MAT OQL 查询（在 MAT GUI 的 OQL 窗口执行）

```sql
-- 1. bufferQueue 实际长度
SELECT q.size FROM java.util.concurrent.LinkedBlockingQueue q
  WHERE q.toString() LIKE ".*"  -- 结合 LocalFlusher 实例过滤

-- 2. 按 applicationId 分组统计 writer（路径形如 /data1/hard_disk/0/<appId>/<shuffleId>/...）
SELECT f.filePath, count(*) FROM org.apache.celeborn.common.meta.DiskFileInfo f GROUP BY f.filePath

-- 3. writer 持有的 flushBuffer 组件数分布
SELECT w, w.currentTierWriter.flushBuffer.numComponents FROM org.apache.celeborn.service.deploy.worker.storage.PartitionDataWriter w
```

### B. 火焰图解析方法

async-profiler HTML 内嵌 `cpool`（前缀压缩字符串表，`charCodeAt(0)-32` 为前缀长度）+ `f/u/n` 深度优先帧序列。用 Python 重建树后按 self 样本归类（脚本逻辑见 F3/F4 口径；`f(key,level,left,width)` 设当前帧，`u`=子帧，`n`=兄弟帧）。

### C. 关键代码位置速查

| 主题 | 文件 | 行号 |
|---|---|---|
| bufferQueue 无界 | `worker/.../storage/Flusher.scala` | L50, L115-136 |
| workingQueues 无界 | 同上 | L67, L138-140 |
| keepBuffer 硬编码 true | `worker/.../storage/TierWriter.scala` | L429, L497, L630-644, L752 |
| writer map 注册/移除 | `worker/.../storage/StorageManager.scala` | L59-64, L542-562, L677-736 |
| commit 不移除 writer | `worker/.../Controller.scala` | L403-405 |
| 水位判定 | `worker/.../memory/MemoryManager.java` | L460-462, L309-330 |
| writer 构造即占 buffer | `worker/.../storage/TierWriter.scala` | L68, L208, L215-229 |
| metrics 抓取全量重建 | `common/.../metrics/source/AbstractSource.scala` | L647-688 |
| timer snapshot 32KB | `common/.../metrics/source/ResettableSlidingWindowReservoir.scala` | L48-54 |
| maxComponents 默认 128 | `common/.../CelebornConf.scala` | L4690-4701 |
| offheap 默认 1g | `sbin/start-worker.sh` | L31-37 |

### D. 相关既有文档

- `production-performance-analysis.md` — P0-1 flusher 队列无界/反压缺失、P0-2 单慢盘背压缺失（上一次 dump，92 MB 堆积）
- `performance-optimization-analysis.md` — §3.5.5 bufferQueue 无上界、§3.5.6 Flusher Random key
- `worker-storage-and-traffic-deep-dive.md` — §8.11 buffer 复用队列只增不减

---

# 第二部分 优化清单（第二轮深挖，增量）

> 基线：本仓库 HEAD（0.7.0-SNAPSHOT）；生产版本 0.6.2.2（build 4a767d22）。除特别注明外，涉及的类/配置在 v0.6.2 与 HEAD 逐行一致，**回 port 可行性默认"高"**。
> 去重：与第一部分、`production-performance-analysis.md`、`performance-optimization-analysis.md`、`worker-storage-and-traffic-deep-dive.md` 已覆盖的问题不重复，只写增量（见文末附录 A）。

## 1. P0：两个配置级问题（立即处理）

### 1.1 小 push 钉住 arena chunk —— 开启 mergeBuffer（0.6.2.2 自带，纯配置）

**机制（代码核实）**：push body 不是整包独立分配，而是"一次 TCP 读分配 + `retain().readSlice()` 切片"（`TransportFrameDecoder.java:125-162`，尺寸由 Netty AdaptiveRecvByteBufAllocator 按历史预测，可能远大于实际 body）。writer 侧 `TierWriter.scala:329/442/664` 直接 `flushBuffer.addComponent(true, buf)` **retain 原始接收 buffer**。

后果链：
- 每个 flushBuffer 组件钉住一个"可能只写了 2-3KB"的接收分配（PoolSubpage 粒度）；
- 113K writer × 平均 ~4.8 组件 ≈ 54 万个活跃小组件散布在 64 个 direct arena 上（dump 实测 `PooledUnsafeDirectByteBuf` 697,289 个）；
- 各 16MB chunk 因残余 subpage **永远无法整体归还 OS** → dump 中 `PoolChunk × 6,102 ≈ 95GB`，与 100G offheap 预算基本打满 —— **这就是堆外"降不下去"的核心机制**。

**现成缓解（CELEBORN-2152，0.6.2 已合入，生产已具备）**：
```conf
celeborn.worker.pushdata.mergeBuffer.enabled=true          # 默认 false
celeborn.worker.directMemoryRatioToMergeBuffer=0.2         # 默认 0.4；调低让低水位也触发
```
开启后超过水位阈值时把 push body **拷贝进紧凑分配的新 buffer**（`PushDataHandler.scala:1510-1517`），原始接收 buffer 立即释放回池 → 碎片源头被切断。局限：条件触发（低于阈值不拷）、拷贝仍在同一 allocator 池内；如要"无条件拷贝"只需改 `:1510-1511` 判断（小 patch）。

**配套**：`celeborn.worker.push.compositeBuffer.maxComponents`（默认 128，`CelebornConf.scala:4690-4701`）。push 平均 <2KB 时会先撞 128 组件触发 Netty `consolidateIfNeeded` 全量拷贝合并（已反编译核实，不抛异常）。512KB flush buffer ÷ 128 = 4KB/组件为分界；平均 push 更小的业务可把 maxComponents 降到 64/32，用"更早 consolidate"换"更低碎片"。

### 1.2 resume(0.2) < readBufferRatio(0.35)：可能永远无法 resume ⚠️

- 生产配置：`directMemoryRatioToPauseReceive=0.5`、`directMemoryRatioToResume=0.2`；
- 读 buffer 配额：`directMemoryRatioForReadBuffer=0.35`（默认），上限 = 0.35 × 100G = **35G**，且全部计入 `PlatformDependent.usedDirectMemory()`（即 memoryUsage 分子）；
- resume 条件：`memoryUsage < 0.2 × 100G = 20G`（`MemoryManager.java:309-330`）。
- **冲突**：读侧若驻留 >20G（读流量大时完全可能），写 buffer 清零也永不 resume → worker 卡在 PAUSED。现有防御校验（`MemoryManager.java:167-170`）只在 memoryFileStorage>0 时生效，绕过了此组合。

**修复（纯配置）**：`directMemoryRatioToResume` 提到 >0.35（如 0.4），或把 `directMemoryRatioForReadBuffer` 降到 0.15~0.2（配合 §1.2 判断读流量实际用量）。代码侧可加一行启动校验（resume > readBuffer + memFileStorage），回 port 极易。

---

## 2. P0：`ReadBufferDispatcher` 把"direct 配额"花在了堆上

**证据**：`ReadBufferDispatcher.java:166`（0.6.2 同位置）：
```java
ByteBuf buf = readBufferAllocator.buffer(bufferSize, bufferSize);  // PooledByteBufAllocator.buffer() = pooled HEAP！
```
- `PooledByteBufAllocator.buffer()` 无视 preferDirect，分配的是**堆内** pooled buffer；
- 但记账走的是 `readBufferCounter`——一个名义上从 **direct memory**（0.35×100G=35G）切出来的配额（`MemoryManager.java:172-177`）；
- 结果：① 读 buffer 实际吃**堆**（96G 堆内再多一个 35G 量级的潜在消耗者），推高 old gen 与 GC 压力；② 堆外水位（usedDirectMemory）看不到这部分，**配额体系失真**；③ 部分解释了 dump 中 `PoolArena$HeapArena × 120 / 244MB`。

**修复**：`buffer(...)` → `directBuffer(bufferSize, bufferSize)`（一行）。修复后读 buffer 回到堆外、被 NettyMemory 正确计量，堆内压力同步下降。**上游 main 至今未修**（已核对），可提 JIRA。回 port 0.6.2.2：一行。
**风险**：heap→direct 后 fetch 读路径失去"堆内更安全"的错觉，需灰度验证（观察 NettyMemory、ReadBufferUsageRatio、fetch 延迟）。

> 关联：dump 中 120 个 HeapArena 的另一半来源是 allocator 构造时 **eager 创建**（`NettyUtils.java:171-180`）：生产 `allocator.share=false`（fetch 侧）+ `numArenas=64` → 每个独立 allocator 实例 eager 建 64 heap + 64 direct arena。建议 fetch 侧改回 `share=true`（全 worker 一池，碎片集中、总量可控），`numArenas` 降到 16~32（超过并发分配线程数无收益）。注意 footgun：`allowCache=false` **只在 share=true 路径生效**（`NettyUtils.java:227`），share=false 时 server allocator 的线程缓存实际仍开着。

---

## 3. P1：泄压与清理路径

### 3.1 onTrim 全量 flush：无排序、无限量、不剔除 closed writer

**现状（代码核实）**：paused 期间，memory check 线程**每 10ms** 无条件尝试 `trimAllListeners`（`MemoryManager.java:340-399`），实际由 `trimInProcess` CAS + 单线程 actionService 串行成"全量 flush + 固定 sleep 1s"的紧循环（`StorageManager.scala:941-950`）。每轮：
- 遍历**全部** writer（含已 close、含空 buffer）——113K 次 map 遍历 + `synchronized flush()` 进出（`StorageManager.scala:903-935` → `PartitionDataWriter.java:253-256` → `TierWriter.scala:170-213`）；
- 空 buffer 的跳过发生在**锁内**（`TierWriter.scala:174`），遍历和锁开销照付；
- 每个非空 buffer（哪怕 1 字节）都生成 FlushTask → 小写 IO 放大 + FlushTask 洪峰（正是 F1 bufferQueue 涨峰的帮凶）；
- **对比**：memory-file evict 路径反而做了"最大优先 + 达标即停"（`MemoryManager.java:261-274`），onTrim 没有。

**优化（上游 main 未做，均为 `flushOnMemoryPressure` 一处的小补丁）**：
1. 遍历时跳过 `isClosed()` writer（`PartitionDataWriter.java:191` 已有判断方法）；
2. 按 `readableBytes` 降序排序、泄压达标即 break（复用 evict 的既有模式）；
3. 跳过低于阈值（如 flusher.buffer.size 的 1/4）的 mini buffer，攒到下轮。

### 3.2 过期清理：64 线程池实际并发度 = 1

**现状**：`Worker.scala:818-849` 每心跳（30s）把整批 expiredShuffleKeys 作为**单个 Runnable** 提交给 64 线程的 `cleanThreadPool`（:342-345，默认 64 线程）——批内 4.4K shuffle 的清理全部单线程顺序执行。单 shuffle 内存部分 ~1-5ms；worker 重启后全量过期（4.4K shuffle/11 万文件）场景内存部分就要 ~110s，期间 writer 持续驻留。

**修复**：按 shuffle 分片提交到 cleanThreadPool（~10 行），吞吐随线程数线性提升。上游 main 未做。
（注：代码库中不存在 `clean.sleepInterval=500ms` 配置——清理节奏 = 心跳批次；`worker.disk.clean.threads=4` 是每盘目录删除线程数，与上述池不同。）

---

## 4. P1/P2：writer 侧轻量化

### 4.1 per-writer 轻量化组合拳（3.4KB → ~1.5KB，113K 场景省 ~200MB 堆）

| # | 项 | 节省/writer | 位置 | 上游 |
|---|---|---|---|---|
| 1 | writerString 延迟构造（仅 equals/toString/日志用） | 140-190B | `PartitionDataWriter.java:56,79` | 未见 |
| 2 | shuffleKey per-shuffle intern 共享（现每 writer 2 份副本） | 80-160B | `PartitionDataWriterContext.java:65`、`TierWriter.scala:53-54,400` | 未见 |
| 3 | PartitionLocation 轻量化（lazy RoaringBitmap + lazy StorageInfo + intern host） | 300-400B × 2 份 | `PartitionLocation.java:102-103,127` | **open PR #3825（CELEBORN-2442，JOL 实测 -51~56%），建议跟踪合入而非自研** |
| 4 | 消除双份 PartitionLocation（context 持标量、WorkingPartition 不深拷贝） | 200-300B | `PartitionDataWriter.java:77`、`WorkingPartition.java:69-81` | 未见 |
| 5 | FileInfo.streams lazy 化（写路径从不用） | 60-80B | `FileInfo.java:31` | 未见 |
| 6 | chunkOffsets `ArrayList<Long>` → `long[]`（10GB 文件省 ~18KB） | 60-100B（小文件） | `ReduceFileMeta.java:26-59` | 未见 |
| 7 | close 后瘦身：channel、writerContext 置 null（连带释放 location/bitmap 链） | 300-500B（closed） | `TierWriter.scala:417`、`PartitionDataWriter.java:54` | 未见 |
| 8 | filename 字符串去重（`getFileName` 每次 new String） | 60-120B | `PartitionLocation.java:247-249` | 未见 |

**说明**：writer close 后仍驻留 map 至 shuffle 过期（既有结论），所以 #7 的"close 后瘦身"收益是实打实的；#3 直接跟踪上游 #3825；其余可打包为一个"writer 瘦身"内部 patch，全部字段级改动、无协议变更，回 port 0.6.2.2 均为高（已用 `git diff v0.6.2 HEAD` 核对相关类逐行一致）。

### 4.2 lazy takeBuffer + idle flush

**先修正一个重要前提**（对第一部分 F2 的勘误）：`Flusher.takeBuffer()` 返回 `compositeDirectBuffer(maxComponents)` 是 **0 组件空壳，此刻不分配任何 direct memory**——空 writer 的堆外占用 ≈ 0。真正占 direct 的是"写过数据但未达 512KB flush 阈值的长尾 writer"（占用 = 实际未 flush 数据量）。第一部分"每 writer 常驻 512KB"的表述应修正为"上限 512KB、按需发生"。

- **lazy takeBuffer**（首次 write 才取 buffer，`TierWriter.scala:68` 构造点取消）：调用点仅 2 处（:68/:208），`flush()` 已 null 安全（:170-171），改动 ~15-25 行，无新并发风险。收益：空 writer 省 ~150B 堆 + 减少 bufferQueue 竞争。**注意与上游 open PR #3809（CELEBORN-2428，TierWriter buffer accounting）同文件，回 port 注意顺序**。
- **idle flush**：当前 writer 只有 commit/过期/内存压力三条出路，无 idle 检测（`celeborn.worker.writer.close.timeout` 默认 120s 是 flush offer 超时和 close 等待预算，**不是** idle 关闭）。建议在 StorageManager 已有定时任务框架上加"最后写入超 N 秒则 flush 归还 buffer"，把长尾 writer 的 direct 占用归还给池。改动集中、回 port 高；上游未见。
- **空 writer 本身不可避免**（ReserveSlots 同步建 writer + 真实 `createNewFile`，延迟注册会与并发 push 产生竞态，**不建议做**）——正确策略是上面的瘦身 + lazy，把空 writer 变"廉价"。

---

## 5. P2：metrics 写入侧与优雅关机

### 5.1 metrics 写入侧（抓取侧 churn 已有文档，勿重复）

1. **Flusher timer key 用固定值**：`Flusher.scala:77` `s"Flusher-$this-${Random.nextInt()}"` —— 全局 `scala.util.Random` 的 AtomicLong CAS 竞争 + 每 flush task ~80B 字符串分配 + CHM put/remove。flusher 线程是单线程串行（`workingQueues(index).take()`），key 可改为固定 `s"Flusher-$this-$index"`；且 `flushTimeMetric`（TimeWindow，:84-87）已记录耗时，dropwizard timer 近乎冗余。**单行改动，上游 main 逐字节相同未修**。
2. **缓存 `metricNameWithCustomizedLabels`**（`AbstractSource.scala:711-719`）：labels 为空时仍每次 `metricsName + staticLabelsString` 新建 String——每 push 的 start/stopTimer 各 1 次（2 分配 + 2 CHM get ≈ 200-300ns/push）。addTimer 时缓存即可。上游未修。
3. **运维侧**：关闭 `celeborn.metrics.worker.appLevel.enabled`（省 per-push `channel.id().asLongText()` ~40 字符分配 + `splitShuffleKey` 两次 substring，CPU 火焰图 ~1% 主因）。
4. **timer in-flight key 表**（`AbstractSource.scala:184`）：清理阈值 5000 条/15min（:397-419）在随机 key 下可能永不触发——但正常路径 stopTimer 即 remove，残留 = 历史泄漏 key（数万条才几 MB），**非问题，无需改**。
5. MemoryManager check 20ms vs 10ms：CPU 差异可忽略；代价是 pause 反应延迟 +10ms（5GB/s 入口 ≈ 多缓冲 50MB）。维持现状即可。

### 5.2 committedFileInfos 全量落盘（graceful shutdown 开启时）

**现状**：`StorageManager.scala:315-331` 的 `saveCommittedFileInfosExecutor` 每周期**无条件全量**遍历 committedFileInfos，每个 shuffle 全量序列化所有 PbFileInfo（含完整 chunkOffsets，`PbSerDeUtils.scala:170,205-211`）再 `db.put`（WAL 写放大 + LSM compaction）。生产已把 interval 从默认 5s 调到 30s（system properties 确认），缓解了频率但没解决"全量"本质——4,374 shuffle/11 万文件下每轮仍序列化数十 MB proto。

**优化**：① dirty-only 增量落盘（记录变更的 shuffleKey，~20 行小 patch）；② 或继续调大 interval（30s→60s+，纯配置，代价是恢复点更旧）。RocksDB 本体（`RocksDBProvider.java:94-155`）未设 writeBufferSize/block cache，默认 off-heap ≤ ~128MB + 8MB block cache，量级可控，暂不用动。
**关 graceful shutdown 的代价**（供决策）：worker 异常重启后无法恢复"已 commit 未过期"shuffle 元数据，文件被当孤儿清理 → 进行中 fetch 失败 → stage 重算。重启不频繁且作业可容忍 retry 时可考虑关闭。

---

## 6. P2：fetch/push 应答写出的 header duplicate churn（alloc 火焰图 10.8% 的真实归因）

**验证修正（2026-09-13，对 alloc 火焰图全路径解析）**：FetchHandler 栈下 10.75% 的 `DirectByteBuffer` 分配**并非** `MapPartitionDataReader.<init>`（此前假设不成立），真实路径是：

```
FetchHandler.handleChunkFetchRequest → writeAndFlush
  → MessageWithHeader.transferTo（MessageWithHeader.java:121）
  → DefaultFileRegion.transferTo → SocketWritableByteChannel.write
  → PooledByteBuf.internalNioBuffer → DirectByteBuffer.duplicate   ← 每次写出都 duplicate header
```

- 机制：pre-CELEBORN-2301 的实现中 `MessageWithHeader` 实现 FileRegion 接口包裹 body，socket partial write 时 `copyByteBuf`/`SocketWritableByteChannel.write` 每次都要给 header 的 PooledByteBuf 生成一个 `DirectByteBuffer.duplicate` 视图（push 应答路径同样存在，合计 ~11% alloc 样本）；
- 性质：**是 churn 不是泄漏**（duplicate 共享底层内存，随 GC 回收），但在高 fetch/push QPS 下是可观的分配噪声与 GC 压力来源；
- 根治 = 回 port CELEBORN-2301（见 §7）：header 与 FileRegion 拆成两个独立 outbound 对象后，此路径消失。

**仍然成立但量级更小的账外分配**：`MapPartitionDataReader.java:118-121` 每个 credit stream 构造时 `ByteBuffer.allocateDirect(indexBufferSize + 16)`——非池化、不计入 readBufferCounter、却计入 `PlatformDependent.usedDirectMemory()` 参与 pause 判定（"账外占用却触发阈值"的不对称）。火焰图中占比很小（<0.1%），但配额不对称问题真实存在。修复：改池化/改 heap/计入 readBufferCounter，小补丁，回 port 高，上游未修。

（读路径其余结论：reduce 磁盘读主路径已完全零拷贝——本仓库已含 CELEBORN-2301 的 header/FileRegion 拆分，见 §7；ReadBufferDispatcher 的 `recycle` 按 `capacity()` 记账当前代码路径下不会漂移，属潜在风险，可加一行对账 metric；credit-stream 读 buffer 全局硬顶 = 0.35×maxDirect，有界，无需改。）

---

## 7. 已实锤：生产 0.6.2.2 **不含** CELEBORN-2301（fetch 零拷贝拆分）

`MessageEncoder.java:92-103`（commit 95419e14a）把 header 与 FileRegion 拆成两个独立 outbound 对象，EPOLL 下 fetch body 走真 sendfile。

**证据（来自生产 alloc 火焰图的调用栈）**：生产抓取的栈是 `MessageWithHeader.transferTo → DefaultFileRegion.transferTo`（§6），即 body 的 FileRegion 仍被 MessageWithHeader 包裹——这是 **pre-2301** 的写法。本仓库（0.7.0-SNAPSHOT）已含 2301，而生产 0.6.2.2 未含。

**回 port 价值（高）**：
1. fetch body 走 sendfile 零拷贝，省一次用户态 copy（大流量 fetch 下 CPU 收益明显）；
2. 顺带消除 §6 的 header duplicate churn（~11% alloc 样本）；
3. 自包含于 common 网络层（MessageEncoder/MessageWithHeader），回 port 容易、风险低。

---

## 8. 配置层速查表（无需改代码，按建议顺序）

| # | 配置 | 现状 → 建议 | 收益 |
|---|---|---|---|
| 1 | `CELEBORN_WORKER_MEMORY` / `OFFHEAP` | 96g/100g → **24g/32g**（依据见第一部分 §3.1） | pinned 下界 ~124G→~40G；堆 <31g 自动启用压缩指针 |
| 2 | `celeborn.worker.pushdata.mergeBuffer.enabled` | false → **true**；`directMemoryRatioToMergeBuffer` 0.4→**0.2** | 堆外碎片源头缓解（§1.1） |
| 3 | `celeborn.worker.directMemoryRatioToResume` | 0.2 → **0.4**（> readBufferRatio 0.35） | 消除永不 resume 风险（§1.2） |
| 4 | `celeborn.network.memory.allocator.share`（fetch 侧）/ `numArenas` | false/64 → **true / 16~32** | arena 数量与 HeapArena 驻留减半以上（§2） |
| 5 | `celeborn.metrics.worker.appLevel.enabled` | true → **false**（如无强需求） | per-push 字符串分配与 ~1% CPU（§5.1） |
| 6 | `celeborn.worker.push.compositeBuffer.maxComponents` | 128 → 小 push 业务可试 **64/32** | 更早 consolidate、更低碎片（§1.1） |
| 7 | `celeborn.worker.graceful.shutdown.saveCommittedFileInfo.interval` | 30s → 可再放宽 | 全量落盘频率（§5.2） |
| 8 | scrape_interval / `metrics.timer.slidingWindow.size` | → 60s / 512 | 抓取侧 churn（见第一部分 §3.1） |

## 附录 A：本轮刻意不重复、已在既有文档覆盖的问题

- bufferQueue 无界（P0）、workingQueues 无界+反压、writer 驻留至 shuffle 过期、metrics 抓取 churn → **本文第一部分**
- flusher 反压/单慢盘、HeapArena 数量、workingDirWriters 僵尸 writer、Netty Recycler 未禁用、Duplicate Strings → `production-performance-analysis.md` 第四章
- MemoryManager 跟踪不完整/trimAllListeners 串行、CompositeByteBuf 组件无限积累、updateDiskInfos 锁、ReadBufferDispatcher 强释引用计数、MapPartitionDataReader DirectBuffer → `performance-optimization-analysis.md` §3.3/3.5/3.9
- flush buffer 默认值、内存文件驱逐全量排序、Flusher Random key → `worker-storage-and-traffic-deep-dive.md` §8

## 附录 B：上游 PR/JIRA 动态（截至 2026-09-13）

| PR/JIRA | 状态 | 建议 |
|---|---|---|
| CELEBORN-2442 / PR #3825 PartitionLocation 内存 -51~56% | open | **跟踪合入**（对应 §4.1-#3） |
| CELEBORN-2428 / PR #3809 TierWriter buffer accounting | open | 回 port 时注意与 lazy takeBuffer 顺序 |
| CELEBORN-2152 mergeBuffer | 已合入 0.6.2 | 生产直接开配置（§1.1） |
| CELEBORN-2301 sendfile 拆分 | 已合入 | 核对 0.6.2.2（§7） |
| CELEBORN-2192/2184/1929、#880、2274/2296/2302、2277 | 已合入 | 本仓库均已包含 |
| ReadBufferDispatcher heap buffer、onTrim 排序限量、cleanup 分片、writer 瘦身、固定 flusher key、metricName 缓存、dirty-only 落盘、账外 direct buffer | **上游均无** | 可作为内部优化或提新 JIRA 贡献 |
