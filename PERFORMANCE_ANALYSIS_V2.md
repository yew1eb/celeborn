# Apache Celeborn 性能分析（Remote Shuffle Service 场景）

本文档面向 **Remote Shuffle Service** 场景，即 Spark 或 Flink 将 Celeborn 作为外部 shuffle 服务替代本地磁盘 shuffle。分析范围涵盖 Master 调度、Worker 存储写/读路径、Worker 内存管理、网络传输以及 Client/Spark 集成模块。文档中所有优化点均已通过代码定位验证，排除纯 Spark local shuffle 或与 Celeborn 无关的 JVM/OS 调优问题。所有代码位置基于当前主干分支。

---

## 优化点汇总（按场景收益 × 实现难度排序）

| 模块 | 编号 | 标题 | 路径分类 | 代码位置 | 场景收益 | 实现难度 |
|------|------|------|---------|---------|---------|---------|
| Worker 内存 | WM-R-4 | ReadBufferDispatcher 引用计数直接归零导致 use-after-free | Read Path | `worker/.../ReadBufferDispatcher.java:90` | 高 | 低 |
| Worker 存储 | WS-W-1 | 本地磁盘 Flush 缓冲区过小导致高频 syscall | Write Path | `common/.../CelebornConf.scala:3866` | 高 | 低 |
| Worker 内存 | WM-W-2 | MemoryManager 内存统计遗漏 DirectByteBuffer 分配 | Write Path | `worker/.../MemoryManager.java:455` | 高 | 中 |
| Worker 内存 | WM-R-1 | MapPartitionDataReader 直接分配 DirectMemory 绕过 MemoryManager | Read Path | `worker/.../MapPartitionDataReader.java:119` | 高 | 中 |
| Worker 存储 | WS-R-1 | FileSegmentManagedBuffer 每次读取都重新 open/close FileChannel | Read Path | `common/.../FileSegmentManagedBuffer.java:56` | 高 | 中 |
| Worker 存储 | WS-W-2 | TierWriter 单一 flushLock 导致写操作全串行化 | Write Path | `worker/.../TierWriter.scala:63` | 高 | 中 |
| Master | M-C-2 | Slot 分配负载计算每次全量重算，无缓存 | Control Path | `master/.../SlotsAllocator.java:721` | 高 | 中 |
| Client/Spark | CS-W-1 | limitMaxInFlight 使用 busy-wait sleep loop | Write Path | `common/.../InFlightRequestTracker.java:130` | 高 | 中 |
| Client/Spark | CS-W-3 | HashBasedShuffleWriter 一次性为所有分区申请 sendBuffer | Write Path | `client-spark/.../HashBasedShuffleWriter.java:138` | 高 | 中 |
| 网络传输 | NT-W-1 | Outbox 消息队列无界，网络拥塞时 OOM 风险 | Write Path | `common/.../Outbox.scala:92` | 高 | 中 |
| Master | M-C-1 | RequestSlots 全局锁内执行重型 Slot 分配计算 | Control Path | `master/.../Master.scala:972` | 高 | 高 |
| Client/Spark | CS-W-2 | DataPushQueue 全量扫描队列，分区数多时 CPU 非线性增长 | Write Path | `client/.../DataPushQueue.java:82` | 高 | 高 |
| Worker 内存 | WM-R-2 | MapPartitionDataReader 异常路径 retain() 后未 release() | Read Path | `worker/.../MapPartitionDataReader.java:169,207` | 中 | 低 |
| Worker 内存 | WM-W-1 | 内存压力检测轮询间隔 100ms，感知延迟过大 | Write Path | `worker/.../MemoryManager.java:175` | 中 | 低 |
| Worker 内存 | WM-R-3 | CreditStreamManager 单线程回收 + 100ms 延迟导致流资源积压 | Read Path | `worker/.../CreditStreamManager.java:42,348` | 中 | 低 |
| Client/Spark | CS-W-4 | pushDataRetryPool 为无界 cached 线程池 | Write Path | `client/.../ShuffleClientImpl.java:230` | 中 | 低 |
| 网络传输 | NT-W-2 | transferTo 单次上限 256KB，大 chunk fetch 需多次 syscall | Write Path | `common/.../MessageWithHeader.java:56` | 中 | 低 |
| Worker 存储 | WS-W-3 | Flink Hybrid Shuffle 缺少 fsync 保障 | Write Path | `worker/.../FlushTask.scala:77` | 中 | 低 |
| Master | M-C-3 | Worker Heartbeat 处理时对 activeShuffleKeys 全量遍历 | Control Path | `master/.../Master.scala:747` | 中 | 中 |
| Worker 存储 | WS-W-4 | MapPartition Index 写入逐次同步 append，无批量写入 | Write Path | `worker/.../PartitionMetaHandler.scala:238` | 中 | 中 |
| Worker 存储 | WS-W-5 | CompositeByteBuf 无限积累组件导致内存碎片 | Write Path | `worker/.../TierWriter.scala:329,441` | 中 | 中 |
| Worker 存储 | WS-R-2 | SSL 场景强制退出零拷贝路径导致 fetch 带宽下降 | Read Path | `common/.../FileSegmentManagedBuffer.java:135` | 中 | 高 |

---

## 1. Master 模块 — Control Path

Master 负责集群资源管理和 shuffle slot 分配，核心路径在 `master/src/main/scala/org/apache/celeborn/service/deploy/master/Master.scala` 和 `master/src/main/java/org/apache/celeborn/service/deploy/master/SlotsAllocator.java`。

### M-C-1 RequestSlots 全局锁内执行重型 Slot 分配计算

- **代码位置**: `master/src/main/scala/org/apache/celeborn/service/deploy/master/Master.scala:972`
- **问题描述**: `statusSystem.workersMap.synchronized` 块内调用 `SlotsAllocator.offerSlotsLoadAware()`，对 O(P×W×D) 进行计算（P=分区数，W=Worker数，D=磁盘数），全局锁持有期间所有 RequestSlots 请求串行化。
- **Remote Shuffle Service 收益**: 高 — 大规模作业（分区数 1k+、Worker 数 100+）下 RequestSlots 并发量高，串行化使 slot 分配成为控制面瓶颈，直接影响 shuffle 注册延迟和作业启动时间。
- **修复方向**: 将计算阶段移出锁范围，先快照必要的 Worker/Disk 元数据，释放锁后完成 O(P×W×D) 计算，最后以细粒度锁提交结果；或引入分 shard 锁按 appId 拆分并发。
- **预期收益**: 大集群（200+ Worker、5k+ 分区）下 RequestSlots P99 延迟预期降低 50%~70%，Master CPU 峰值下降明显。
- **实现难度**: 高

---

### M-C-2 Slot 分配负载计算每次全量重算，无缓存

- **代码位置**: `master/src/main/java/org/apache/celeborn/service/deploy/master/SlotsAllocator.java:721`
- **问题描述**: `getSlotsRestrictionsByLoadAwareAlgorithm()` 方法每次 RequestSlots 都重新计算所有磁盘分组和负载分配比例，未缓存中间结果，大集群下 CPU 开销高。
- **Remote Shuffle Service 收益**: 高 — 并发 shuffle 注册频繁时（如 Spark 宽依赖密集型作业），全量重算累积开销直接拉高 Master CPU，可能引发 GC 进而影响 HA 心跳稳定性。
- **修复方向**: 引入带 TTL 的缓存（如 500ms），缓存磁盘负载分组结果；Worker Heartbeat 更新磁盘状态时令缓存失效，避免过期数据影响准确性。
- **预期收益**: Master CPU 在高并发注册场景下降低 20%~40%；RequestSlots 吞吐提升与集群规模正相关。
- **实现难度**: 中

---

### M-C-3 Worker Heartbeat 处理时对 activeShuffleKeys 全量遍历

- **代码位置**: `master/src/main/scala/org/apache/celeborn/service/deploy/master/Master.scala:747`
- **问题描述**: 每次 Worker Heartbeat 都对 `activeShuffleKeys`（ConcurrentHashMap）执行全量遍历以找出过期 shuffle，shuffle 数量多时 O(N) 开销随 shuffle 规模线性积累。
- **Remote Shuffle Service 收益**: 中 — 长时间运行的集群中历史 shuffle key 积累后，每次心跳（默认 15s）的遍历开销不可忽视，叠加大量 Worker 的心跳并发会占用 Master 线程池资源。
- **修复方向**: 维护一个反向索引（shuffleKey → expireTime），用时间轮或优先队列做懒惰过期，心跳时仅检查到期队列头部而非全量扫描。
- **预期收益**: 心跳处理延迟在 10k+ 活跃 shuffle 场景下预期降低 60%+。
- **实现难度**: 中

---

## 2. Worker 存储模块 — Write Path

Worker 存储写路径核心文件位于 `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/` 目录。

### WS-W-1 本地磁盘 Flush 缓冲区过小导致高频 syscall

- **代码位置**: `common/src/main/scala/org/apache/celeborn/common/CelebornConf.scala:3866`
- **问题描述**: 本地磁盘 flush 缓冲默认仅 256KB，而 HDFS 路径配置为 4MB，导致本地磁盘每次 push 数据后 flush 频率为 HDFS 的 16×，syscall 次数大幅增加。
- **Remote Shuffle Service 收益**: 高 — Remote shuffle 写路径全部经过 Worker 本地磁盘或 DFS，flush 频率直接决定写入吞吐和 CPU 占用，低缓冲导致 I/O 放大显著。
- **修复方向**: 将本地磁盘默认缓冲上调至 1MB~4MB，并提供按存储介质独立配置的选项（NVMe vs HDD）；评估是否引入自适应缓冲大小策略。
- **预期收益**: 本地磁盘写入吞吐预期提升 20%~40%，Worker CPU（sys 部分）下降明显。
- **实现难度**: 低

---

### WS-W-2 TierWriter 单一 flushLock 导致写操作全串行化

- **代码位置**: `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/TierWriter.scala:63`
- **问题描述**: 单一 `flushLock` 保护 write、evict、takeBuffer、returnBuffer 所有操作，高并发 push 时所有操作互斥等待，无法并行。
- **Remote Shuffle Service 收益**: 高 — Mapper 并发写入同一 Worker 时，单锁成为写路径的序列化点，在 Netty IO 线程数较多时锁竞争显著影响 push 吞吐。
- **修复方向**: 拆分锁粒度：将 buffer 管理（takeBuffer/returnBuffer）与 flush 提交（write/evict）分离为两把独立锁；或改用 `StampedLock` 对只读操作使用乐观锁。
- **预期收益**: 高并发写入场景（100+ 并发 Mapper 写同一 Worker）下 push 吞吐预期提升 15%~30%。
- **实现难度**: 中

---

### WS-W-3 Flink Hybrid Shuffle 场景本地磁盘缺少 fsync 保障

- **代码位置**: `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/FlushTask.scala:77`
- **问题描述**: 本地磁盘 flush 无 fsync，Flink hybrid shuffle 上游写入、下游即刻读取场景下存在数据一致性风险（代码中有 TODO 注释明确标注此问题）。
- **Remote Shuffle Service 收益**: 中 — 仅影响 Flink hybrid shuffle 场景，但该场景下数据损坏会导致作业失败，收益体现在正确性保障而非性能提升。
- **修复方向**: 在 Flink hybrid shuffle 模式下，Region 完成时对 FileChannel 执行 `force(false)` （仅刷数据，不刷元数据以减少开销）；提供配置开关控制是否启用 fsync。
- **预期收益**: 消除 Flink hybrid shuffle 数据一致性风险；性能影响视 fsync 频率，预计单次增加 1~5ms 延迟。
- **实现难度**: 低

---

### WS-W-4 MapPartition Index 写入逐次同步 append，无批量写入

- **代码位置**: `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/PartitionMetaHandler.scala:238`
- **问题描述**: 每个 Region 完成时同步写 index，HDFS 路径逐次 `append`，无批量写入，频繁小 I/O 在 HDFS 场景下 RPC 开销高。
- **Remote Shuffle Service 收益**: 中 — HDFS 作为 DFS 后端时每次 append 都是一次 RPC，Region 数量多时累积延迟不可忽视；本地磁盘场景影响相对较小。
- **修复方向**: 缓存多个 Region 的 index 条目，批量写入（如每 N 个 Region 或超过阈值大小时一次性写入）；保证 close 时强制 flush 剩余条目。
- **预期收益**: HDFS 场景下 index 写入 RPC 次数减少 80%+，对应写路径延迟降低。
- **实现难度**: 中

---

### WS-W-5 CompositeByteBuf 无限积累组件导致内存碎片

- **代码位置**: `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/TierWriter.scala:329,441`
- **问题描述**: `flushBuffer.addComponent(true, buf)` 无限积累 `CompositeByteBuf` 组件，仅在 close 时 consolidate，极端场景下导致大量内存碎片，Netty 内存分配效率下降。
- **Remote Shuffle Service 收益**: 中 — 长时间运行或大 shuffle 场景下内存碎片积累会加剧 GC 压力并增加 Netty 内存分配失败概率，影响写入稳定性。
- **修复方向**: 设置 `CompositeByteBuf` 组件数量上限（如 1024），超限时触发一次中间 consolidate；或改用固定大小的 ByteBuf 链表替代无界 CompositeByteBuf。
- **预期收益**: 内存碎片减少，长时间运行下 Netty DirectMemory 占用稳定性提升；GC 频率预计降低 10%~20%。
- **实现难度**: 中

---

## 3. Worker 存储模块 — Read Path

Worker 存储读路径核心文件位于 `common/src/main/java/org/apache/celeborn/common/network/buffer/` 目录。

### WS-R-1 FileSegmentManagedBuffer 每次读取都重新 open/close FileChannel

- **代码位置**: `common/src/main/java/org/apache/celeborn/common/network/buffer/FileSegmentManagedBuffer.java:56`
- **问题描述**: 每次 `nioByteBuffer()` 调用都新建 `FileChannel`，读完立即关闭，没有 FD 缓存；reducer 并发 fetch 时大量重复 open/close 系统调用。
- **Remote Shuffle Service 收益**: 高 — Reducer fetch 是 remote shuffle 读路径的核心，高并发 fetch 下每次 open/close 的 syscall 开销在 Worker 侧累积明显，直接影响 fetch 吞吐和延迟。
- **修复方向**: 引入基于文件路径的 `FileChannel` 缓存池（带 LRU 淘汰和引用计数），复用 FD；注意正确处理文件删除场景（shuffle 清理时需从缓存中移除）。
- **预期收益**: 高并发 fetch 场景下 Worker 侧 syscall 减少 60%+，fetch 吞吐提升 20%~35%。
- **实现难度**: 中

---

### WS-R-2 SSL 场景强制退出零拷贝路径导致 fetch 带宽大幅下降

- **代码位置**: `common/src/main/java/org/apache/celeborn/common/network/buffer/FileSegmentManagedBuffer.java:135`
- **问题描述**: SSL 场景强制退出零拷贝路径，改用 `ChunkedStream` 内存拷贝；数据中心内部流量启用 SSL 时 fetch 带宽会大幅下降。
- **Remote Shuffle Service 收益**: 中 — 仅在启用传输层 SSL 时触发，但企业安全合规场景下 SSL 普遍启用，此时 fetch 性能劣化严重（内存拷贝开销可使带宽下降 30%~50%）。
- **修复方向**: 探索 SSL offload 方案（如 Netty SslHandler 配合 OpenSSL native 实现）以减少 JVM 侧内存拷贝；或对数据中心内部流量提供跳过 SSL 的白名单配置。
- **预期收益**: 启用 SSL 场景下 fetch 带宽恢复至接近非 SSL 水平，预计提升 40%~60%。
- **实现难度**: 高

---

## 4. Worker 内存模块 — Write Path

Worker 内存管理核心文件位于 `worker/src/main/java/org/apache/celeborn/service/deploy/worker/memory/`。

### WM-W-1 内存压力检测轮询间隔 100ms，感知延迟过大

- **代码位置**: `worker/src/main/java/org/apache/celeborn/service/deploy/worker/memory/MemoryManager.java:175`
- **问题描述**: 内存压力检测轮询间隔 100ms（`checkInterval` 默认值），感知延迟过大，内存突增时可能积压过多数据才触发 spill。
- **Remote Shuffle Service 收益**: 中 — 大量 Mapper 并发 push 时内存压力可在数百毫秒内从正常迅速达到临界，100ms 检测间隔可能导致 spill 响应滞后，引发 BackPressure 抖动甚至 OOM。
- **修复方向**: 将默认检测间隔缩短至 20~30ms；同时引入基于内存变化速率的自适应检测：内存增速超过阈值时临时切换为高频检测模式。
- **预期收益**: Spill 响应时间从最坏 100ms 降至 20~30ms，减少因检测滞后导致的 BackPressure 峰值。
- **实现难度**: 低

---

### WM-W-2 MemoryManager 内存统计遗漏 DirectByteBuffer 分配

- **代码位置**: `worker/src/main/java/org/apache/celeborn/service/deploy/worker/memory/MemoryManager.java:455`
- **问题描述**: `getMemoryUsage()` 仅统计 Netty DirectMemory 和 `sortMemoryCounter`，遗漏 `MapPartitionDataReader.indexBuffer`/`headerBuffer` 等通过 `ByteBuffer.allocateDirect()` 直接分配的内存，统计不完整导致实际内存已满但 spill 未触发。
- **Remote Shuffle Service 收益**: 高 — 统计遗漏直接导致 OOM 风险：系统认为内存充足而实际 DirectMemory 已耗尽，触发 JVM OOM 或 Netty `OutOfDirectMemoryError`，造成 Worker 崩溃。
- **修复方向**: 将所有 `ByteBuffer.allocateDirect()` 调用替换为经 `MemoryManager` 托管的接口，或在 `getMemoryUsage()` 中增加对 `DirectByteBuffer` 统计（可通过 `sun.misc.SharedSecrets` 或 JMX `MemoryPoolMXBean` 获取）。
- **预期收益**: 消除因统计遗漏导致的 OOM 风险；内存管控准确性提升使 spill 策略更稳定。
- **实现难度**: 中

---

## 5. Worker 内存模块 — Read Path

### WM-R-1 MapPartitionDataReader 直接分配 DirectMemory 绕过 MemoryManager

- **代码位置**: `worker/src/main/java/org/apache/celeborn/service/deploy/worker/storage/MapPartitionDataReader.java:119`
- **问题描述**: 直接调用 `ByteBuffer.allocateDirect()` 绕过 `MemoryManager`，所分配的 DirectMemory 不在内存管控范围内，大量并发 reader 下导致 OOM。
- **Remote Shuffle Service 收益**: 高 — 大规模 reduce 阶段并发 reader 数量可达数千，每个 reader 独立分配 DirectMemory 且不受管控，是 Worker OOM 的高频根因之一。
- **修复方向**: 将 `indexBuffer` 和 `headerBuffer` 的分配改为通过 `MemoryManager` 申请，纳入统一的内存配额管理；超出配额时阻塞或返回背压。
- **预期收益**: 消除并发 reader 引发的 OOM；内存使用上界可控，Worker 稳定性显著提升。
- **实现难度**: 中

---

### WM-R-2 MapPartitionDataReader 异常路径 retain() 后未 release() 导致内存泄漏

- **代码位置**: `worker/src/main/java/org/apache/celeborn/service/deploy/worker/storage/MapPartitionDataReader.java:169,207`
- **问题描述**: 异常路径中 `retain()` 后引用计数已增，但 `bufferRecycler.recycle(buffer)` 未对应 `release()`，导致 buffer 永久泄漏；触发频率低但每次泄漏一个 buffer。
- **Remote Shuffle Service 收益**: 中 — 长时间运行或异常频繁场景下内存泄漏累积，最终导致 DirectMemory 耗尽触发 OOM；定位难度高，对稳定性影响大。
- **修复方向**: 在异常路径中补充对应的 `release()` 调用；使用 try-finally 确保 `retain/release` 配对；引入 Netty ResourceLeakDetector 在测试环境验证修复效果。
- **预期收益**: 消除内存泄漏，长时间运行下 Worker DirectMemory 占用稳定，减少非预期 OOM 重启。
- **实现难度**: 低

---

### WM-R-3 CreditStreamManager 单线程回收 + 100ms 延迟导致流资源积压

- **代码位置**: `worker/src/main/java/org/apache/celeborn/service/deploy/worker/storage/CreditStreamManager.java:42,348`
- **问题描述**: 单线程 `recycleThread` 加 100ms 固定延迟（`DelayedStreamId.delayTime`），大量 reducer 并发读取时流资源回收跟不上创建速度，导致 stream 积压和内存增长。
- **Remote Shuffle Service 收益**: 中 — 大规模 reduce 阶段（如 10k+ reducer）并发建流时，回收速度瓶颈导致存活 stream 数量线性增长，加剧内存压力。
- **修复方向**: 将回收线程扩展为可配置的线程池（默认 2~4 线程）；评估 100ms 延迟是否必要（该延迟旨在防止过早回收），考虑缩短至 20~50ms 或基于引用计数直接回收。
- **预期收益**: 高并发读取场景下 stream 积压减少 70%+，相关内存开销降低。
- **实现难度**: 低

---

### WM-R-4 ReadBufferDispatcher 将引用计数直接归零导致 use-after-free

- **代码位置**: `worker/src/main/java/org/apache/celeborn/service/deploy/worker/memory/ReadBufferDispatcher.java:90`
- **问题描述**: `buf.release(refCnt)` 将引用计数直接归零而非减一，若其他地方还持有引用则触发 use-after-free，导致数据损坏或 NPE。
- **Remote Shuffle Service 收益**: 高 — 数据损坏是严重的正确性问题，会导致 reducer 读取到错误数据进而触发作业失败或产生错误计算结果，且此类 bug 复现困难、排查成本极高。
- **修复方向**: 将 `buf.release(refCnt)` 替换为 `buf.release()`（减一语义）；若确需强制归零，需通过统一的资源追踪机制确保无其他持有者。
- **预期收益**: 消除 use-after-free 数据损坏风险，fetch 正确性保障；对性能无影响。
- **实现难度**: 低

---

## 6. 网络传输模块 — Write Path / Control Path

网络传输核心文件位于 `common/src/main/scala/org/apache/celeborn/common/rpc/netty/` 和 `common/src/main/java/org/apache/celeborn/common/network/protocol/`。

### NT-W-1 Outbox 消息队列无界，网络拥塞时 OOM 风险

- **代码位置**: `common/src/main/scala/org/apache/celeborn/common/rpc/netty/Outbox.scala:92`
- **问题描述**: Outbox 消息队列使用无界 `LinkedList`，网络拥塞时消息堆积无限增长，极端负载下引发 OOM。
- **Remote Shuffle Service 收益**: 高 — Push 数据量大时网络偶发拥塞会导致 Client 侧 Outbox 快速积压，无界队列使 OOM 风险与网络质量强相关，影响整个 Executor 进程稳定性。
- **修复方向**: 将 `LinkedList` 替换为有界阻塞队列（如 `ArrayBlockingQueue`），超限时对生产者施加背压（阻塞或返回错误）；配合现有的 `limitMaxInFlight` 机制协同控流。
- **预期收益**: 消除网络拥塞场景下的 OOM 风险；背压机制使 push 速率与网络吞吐自适应匹配。
- **实现难度**: 中

---

### NT-W-2 transferTo 单次上限 256KB，大 chunk fetch 需多次 syscall

- **代码位置**: `common/src/main/java/org/apache/celeborn/common/network/protocol/MessageWithHeader.java:56`
- **问题描述**: `NIO_BUFFER_LIMIT` 固定为 256KB，大 fetch chunk（如 8MB）需分 32 次 `transferTo` 调用，增加系统调用开销和内核态切换次数。
- **Remote Shuffle Service 收益**: 中 — 对于大 chunk fetch 场景（`celeborn.client.fetch.chunkSize` 较大时）syscall 开销显著；小 chunk 场景影响有限。
- **修复方向**: 将 `NIO_BUFFER_LIMIT` 提升至 1MB~2MB，或改为可配置项；评估 Linux `sendfile` 单次传输上限（通常为 2GB）的约束，确认安全上限。
- **预期收益**: 大 chunk fetch 场景下 syscall 次数减少 75%+（256KB→1MB），Worker 侧 CPU sys 时间下降。
- **实现难度**: 低

---

## 7. Client / Spark 集成模块 — Write Path

Client 核心文件位于 `client/src/main/java/org/apache/celeborn/client/` 和 `client-spark/spark-3/src/main/java/org/apache/spark/shuffle/celeborn/`。

### CS-W-1 limitMaxInFlight 使用 busy-wait sleep loop，高并发下 CPU 开销高

- **代码位置**: `common/src/main/java/org/apache/celeborn/common/write/InFlightRequestTracker.java:130`
- **问题描述**: `limitMaxInFlight()` 使用 sleep loop 实现背压等待，每次迭代调用 `totalInflightReqs.sum()` 遍历 LongAdder cells，高并发下 CPU 占用高且唤醒延迟受 sleep 粒度影响。
- **Remote Shuffle Service 收益**: 高 — Mapper 并发写入时 `limitMaxInFlight` 是写路径的核心限速点，busy-wait 在等待期间持续消耗 CPU，与写入线程竞争资源，直接影响整体 push 吞吐。
- **修复方向**: 将 sleep loop 替换为基于 `Semaphore` 或 `LockSupport.parkNanos` 的条件等待；在 in-flight 请求完成时主动唤醒等待线程，消除 busy-wait 开销。
- **预期收益**: 等待期间 CPU 占用降低 80%+；唤醒延迟从 sleep 粒度（ms 级）降至 µs 级，提升 push 响应速度。
- **实现难度**: 中

---

### CS-W-2 DataPushQueue.takePushTasks() 全量扫描队列，分区数多时 CPU 开销非线性增长

- **代码位置**: `client/src/main/java/org/apache/celeborn/client/write/DataPushQueue.java:82`
- **问题描述**: `takePushTasks()` 内层双重循环遍历 `workingQueue` 和每个任务的 `partitionLocations`，时间复杂度 O(queue_size × workers_per_partition)，分区数和重试队列积压时 CPU 开销非线性增长。
- **Remote Shuffle Service 收益**: 高 — 分区数大（10k+）或网络抖动引发重试积压时，全量扫描成为写路径热点，导致 push 线程在调度上浪费大量 CPU 而非实际传输数据。
- **修复方向**: 引入按 Worker 分组的就绪队列索引，`takePushTasks()` 直接从就绪索引中取任务而非全量扫描；使用优先队列或分桶结构降低调度复杂度至 O(1) 或 O(log N)。
- **预期收益**: 大分区数场景（10k+ 分区）下 push 调度 CPU 开销降低 50%~70%，整体写入吞吐提升明显。
- **实现难度**: 高

---

### CS-W-3 HashBasedShuffleWriter 一次性为所有分区申请 sendBuffer，内存线性增长

- **代码位置**: `client-spark/spark-3/src/main/java/org/apache/spark/shuffle/celeborn/HashBasedShuffleWriter.java:138`
- **问题描述**: 每个 ShuffleWriter 一次性为所有分区申请独立 sendBuffer，分区数大（如 10k+）时内存与分区数线性增长，导致 Executor 内存压力显著增大。
- **Remote Shuffle Service 收益**: 高 — Remote shuffle 场景下分区数通常远大于本地 shuffle（无本地聚合优化），此问题在大规模作业中普遍存在，直接影响 Executor 堆外内存配置和 OOM 风险。
- **修复方向**: 改用懒分配策略：仅在首次写入该分区时申请 buffer；或引入共享的 buffer pool，多分区复用固定数量的 buffer（类似 Sort-based writer 的实现方式）。
- **预期收益**: 10k 分区场景下 Executor sendBuffer 内存占用减少 60%~80%，OOM 风险大幅降低。
- **实现难度**: 中

---

### CS-W-4 pushDataRetryPool 为无界 cached 线程池，大量失败时 OOM 风险

- **代码位置**: `client/src/main/java/org/apache/celeborn/client/ShuffleClientImpl.java:230`
- **问题描述**: `pushDataRetryPool` 为 cached 线程池（无队列上限），大量推送失败时重试任务无界堆积，可能引发 OOM 或级联故障导致 Executor 崩溃。
- **Remote Shuffle Service 收益**: 中 — 网络抖动或 Worker 短暂不可用时会触发批量重试，无界线程池在此场景下是稳定性隐患；正常情况下影响不大，但故障时会加速 Executor 崩溃。
- **修复方向**: 将 cached 线程池替换为固定大小线程池 + 有界队列；超出队列时对调用方返回背压（阻塞或直接抛出，由上层重试策略处理）。
- **预期收益**: 消除大规模重试场景下的 OOM 风险，故障恢复过程中 Executor 稳定性提升。
- **实现难度**: 低
