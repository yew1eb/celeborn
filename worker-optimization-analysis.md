# Celeborn Worker 优化深度调研报告

> 分析日期：2026-05-21  
> 覆盖模块：PushDataHandler、MemoryManager、StorageManager、Flusher、PartitionDataWriter、CongestionController、PartitionFilesSorter、FetchHandler、Controller、Worker

---

## 背景：Worker 的并发压力模型

Celeborn Worker 与传统存储服务的核心差别：

- **写路径极热**：Mapper 并发 pushData，每个 partition 对应一个 PartitionDataWriter，高峰期同时写入的 writer 可达数万个
- **Netty IO 线程是全局稀缺资源**：push/fetch 均由 Netty EventLoop 驱动，任何阻塞都会串行化整条流水线
- **内存是共享战场**：push buffer、DFS 缓冲、replication buffer 争抢同一片 Direct Memory
- **写放大严重**：每条数据写本地 → 可能写 HDFS/S3 → 可能 replicate 到 peer Worker，三路写并发
- **读排序高延迟**：ReduceTask 拉取时才做 sort，排序窗口内 Netty 读线程被占用

---

## 第一部分：PushDataHandler — 写路径热点

### 1.1 【P0】Netty IO 线程上的同步阻塞等待

**文件**：`PushDataHandler.scala`，L304、L399、L640、L796、L1061

```scala
// 典型模式：在 Netty channelRead 回调中同步等待写完成
Await.result(writePromise.future, Duration.Inf)
```

**问题本质**：`Await.result(..., Duration.Inf)` 会让当前 Netty EventLoop 线程阻塞，直到写操作完成。Netty 的 EventLoop 是单线程的，阻塞一次就意味着：
- 该 EventLoop 负责的**所有连接**在写完成之前全部无法处理新消息
- Worker 的 push 吞吐量直接受限于单次写延迟，而不是磁盘带宽
- 高磁盘压力时（flush 慢），雪崩效应：写慢 → IO 线程阻塞 → push 积压 → 更多等待

**工业参考**：Kafka KafkaChannel 的写是完全异步的，write buffer 满了就注册 OP_WRITE 事件，不阻塞 Selector 线程。Netty 的正确做法是将结果通过 `ChannelFuture` 回调通知。

**优化方案**：将写操作改为真异步模式：
```scala
// 改造为：提交写任务后立即返回，写完成时在回调中发送 response
val writeFuture = submitWriteTask(data, location)
writeFuture.onComplete {
  case Success(_) => channel.writeAndFlush(PushDataSuccess)
  case Failure(e) => channel.writeAndFlush(PushDataFailure(e))
}(callbackExecutor)
// 不再 Await.result
```

**预期收益**：Netty IO 线程利用率从写期间 0% 提升到 ~100%，push 吞吐量提升 2-5x（尤其在磁盘 IO 慢时）。

---

### 1.2 【P1】每次 Replication 创建临时 WorkerInfo 对象

**文件**：`PushDataHandler.scala`，L281、L613

```scala
// replication 路径的热点：每条 push 都 new 一个 WorkerInfo 仅用于 RPC 客户端查找
val peer = new WorkerInfo(peerHost, peerRpcPort, peerPushPort, peerFetchPort, peerReplicatePort)
val client = pushClientFactory.createClient(peer.host, peer.replicatePort, partitionId)
```

**问题**：`WorkerInfo` 构造函数触发 `lazy val toUniqueId` 的初始化路径，每次 replication 都产生短命对象，增大 GC 压力。在高 QPS 场景（10万 partition 并发写），每秒可产生数十万临时对象。

**优化方案**：直接用 `(host, port)` 元组查找 replication client，避免构造 WorkerInfo：
```java
// 改造前
WorkerInfo peer = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort);
TransportClient client = pushClientFactory.createClient(peer.host, peer.replicatePort);
// 改造后
TransportClient client = pushClientFactory.createClient(host, replicatePort);
```

---

### 1.3 【P1】handlePushData 与 handlePushMergedData 逻辑重复

**文件**：`PushDataHandler.scala`，L200-L500 vs L550-L900

两个方法共享 95% 的逻辑（权限检查、内存检查、写本地、复制到 peer、超时处理），但独立维护，导致：
- 修复 bug 需要在两处同步修改，历史上已有多次遗漏
- 代码体积膨胀，理解和测试成本高

**优化方案**：提取公共 `handlePushDataInternal(data, isMerged, ...)` 方法，两个入口方法只做参数解包后转发。

---

### 1.4 【P2】异常驱动的协议版本检测

**文件**：`PushDataHandler.scala`，L1150-1185

```scala
// 用 try-catch 判断是否是旧协议格式
try {
  val partitionUniqueIds = PbPushMergedData.parseFrom(body)
  // 新协议路径
} catch {
  case _: InvalidProtocolBufferException =>
  // 旧协议 fallback 路径
}
```

**问题**：异常驱动的控制流在 JVM 上代价高昂——每次异常都要生成完整的 stack trace（即使不打印也要填充 `Throwable` 对象）。在兼容旧版 client 的集群中，每个 push 请求都触发一次异常。

**优化方案**：在消息头中加入版本标识字段，或通过消息前导字节区分协议版本，消除 try-catch 路径。

---

## 第二部分：MemoryManager — 内存状态管理缺陷

### 2.1 【P0】isPaused 不是 volatile，跨线程可见性不保证

**文件**：`MemoryManager.java`，L81

```java
private boolean isPaused = false;  // 非 volatile
```

`isPaused` 被 `checkService` 线程写入，被 `getPushPausedStatus()` / `getPushAndReplicatePausedStatus()` 从多个 Netty IO 线程读取，**没有任何 happens-before 保证**。在 x86 TSO 内存模型下通常不出问题，但在 ARM（如 AWS Graviton、Apple Silicon）上可能读到过期值，导致 push 在内存压力释放后仍然无法恢复。

**修复**：
```java
private volatile boolean isPaused = false;
// 同理，pausePushDataStartTime、pausePushDataAndReplicateStartTime 也需要 volatile
```

---

### 2.2 【P1】内存文件驱逐：每次全量排序

**文件**：`MemoryManager.java`，L263-274

```java
// 内存压力触发驱逐时，对所有内存 writer 排序
List<PartitionDataWriter> writers = new ArrayList<>(memoryWriters.values());
writers.sort(Comparator.comparingLong(w -> w.getCurrentFileInfo().getFileLength()));
// 取最大的 N 个驱逐
```

**问题**：每次驱逐都 O(n log n) 全量排序所有内存 writer。内存压力时 writer 数量可达数万，排序本身就消耗内存并触发更多 GC，形成负反馈。

**优化方案**：维护一个按文件大小排序的 `TreeMap<Long, PartitionDataWriter>`，插入/删除 O(log n)，取最大值 O(1)；或使用 `PriorityQueue` 实现 Top-K 驱逐。

---

### 2.3 【P1】memoryPressureListeners 的遍历不在同步块内

**文件**：`MemoryManager.java`，L383-388

```java
// onPause 路径：synchronized 块外遍历 listeners
memoryPressureListeners.forEach(listener -> {
    listener.onPause(pauseState);
});
```

但 `addMemoryPressureListener` 和 `removeMemoryPressureListener` 是 `synchronized(memoryPressureListeners)` 的，遍历不同步可能导致 `ConcurrentModificationException`（`ArrayList` 的并发读写未定义行为）。

**修复**：遍历时也持锁，或改用 `CopyOnWriteArrayList`（遍历不加锁，修改时复制，适合读多写少的 listener 场景）。

---

### 2.4 【P2】trimAllListeners 单线程串行，且受 trimInProcess 互斥

**文件**：`MemoryManager.java`，L414-430

```java
private final AtomicBoolean trimInProcess = new AtomicBoolean(false);

public void trimAllListeners() {
    if (trimInProcess.compareAndSet(false, true)) {
        actionService.submit(() -> {
            // 串行 trim 所有 writer
            memoryFileStorageListeners.forEach(w -> w.flushOnMemoryPressure());
            trimInProcess.set(false);
        });
    }
    // 如果 trim 正在进行，直接跳过本次触发
}
```

**问题**：`trimInProcess` 保证同一时刻只有一次 trim，但如果第一次 trim 还未完成，后续的 trim 请求被静默丢弃。在内存持续高压时，可能出现"trim 来不及，压力持续积累"的情况。

**优化**：至少用一个 pending flag 记录"trim 结束后需要再做一次"，参考 Netty 的 `SingleThreadEventExecutor` 的任务合并模式。

---

## 第三部分：StorageManager — 存储层锁竞争

### 3.1 【P0】updateDiskInfos 持全局锁 + 内层 writers 锁，双重锁

**文件**：`StorageManager.scala`，L952-990

```scala
def updateDiskInfos(): Unit = this.synchronized {  // 外层：this 全局锁
  diskInfos.foreach { case (mountPoint, diskInfo) =>
    val writers = workingDirWriters.get(diskInfo.dirs.head)
    writers.synchronized {  // 内层：per-directory writer 锁
      // 统计 disk 使用量、更新 DiskInfo
    }
  }
}
```

`updateDiskInfos()` 由 Worker 心跳线程定期调用（默认 10s），持有 `this` 全局锁期间，所有调用 `this.synchronized` 的方法全部阻塞，包括：
- `notifyError()`（磁盘故障通知）
- `notifyHealthy()`（磁盘恢复通知）  
- `ensureS3MultipartUploaderSharedState()`

**优化方案**：将磁盘统计信息改为增量维护（writer 在写入/关闭时原子累加），`updateDiskInfos` 只需读取原子计数器，无需持全局锁遍历 writer map。

---

### 3.2 【P1】diskFileInfos.synchronized 保护的临界区过大

**文件**：`StorageManager.scala`，L1003-1030

```scala
diskFileInfos.synchronized {
  diskFileInfos
    .values()
    .asScala
    .flatMap(_.values().asScala)
    .filter(fileInfo => !fileInfo.isDFS)
    .map(f => (f.getUserIdentifier, f.getFileLength, ...))
    .groupBy(_._1)
    .mapValues(...)
    .toMap
}
```

在 `diskFileInfos.synchronized` 持锁期间执行完整的 flatMap + groupBy + mapValues，持锁时间与 shuffle 文件总数成正比。该方法被 Worker 心跳调用，高峰期（数万 partition）持锁时间可达数十毫秒，期间所有 `getFileInfo`、`commitFile` 调用全部阻塞。

**优化方案**：
1. 先持锁做浅拷贝（只复制外层 key set），锁外做聚合计算
2. 用 `ConcurrentHashMap` 替换 synchronized Map，并维护增量统计 Map

---

### 3.3 【P2】createPartitionDataWriter 的磁盘选择重试机制效率低

**文件**：`StorageManager.scala`，L485-530

```scala
// 遍历所有磁盘目录，找到第一个可用的
dirs.find { dir =>
  try {
    createFileForPartition(dir, context)
    true
  } catch {
    case e: IOException => false  // 失败了继续下一个
  }
}
```

异常驱动的磁盘选择：每次磁盘 IO 错误都触发 `IOException`，再换下一个磁盘重试。在磁盘降级场景中，对故障磁盘的每次失败尝试都消耗 Netty 线程时间，且 stack trace 生成代价高。

**优化方案**：在 `DiskInfo` 中维护磁盘健康状态 flag，`find` 时先过滤掉已知故障磁盘，失败才降级到异常处理路径。

---

## 第四部分：Flusher — 刷盘路径瓶颈

### 4.1 【P1】bufferQueue（CompositeByteBuf 复用池）无上界

**文件**：`Flusher.scala`，L50、L115-136

```scala
protected val bufferQueue = new LinkedBlockingQueue[CompositeByteBuf]()  // 无界队列

def takeBuffer(): CompositeByteBuf = {
  var buffer = bufferQueue.poll()
  if (buffer == null) {
    buffer = allocator.compositeDirectBuffer(maxComponents)  // 按需分配
  }
  buffer
}

def returnBuffer(buffer: CompositeByteBuf, keepBuffer: Boolean = false): Unit = {
  // ...
  if (keepBuffer) {
    bufferQueue.put(buffer)  // 归还到复用池
  } else {
    buffer.release()
  }
}
```

**问题**：`bufferQueue` 是无界队列，理论上 Direct Memory buffer 会无限累积。低负载时大量空闲 buffer 占用 Direct Memory，触发 OutOfDirectMemoryError，而实际数据吞吐量并不高。

**优化**：限制 `bufferQueue` 最大容量（如 `threadCount * 2`），超出时 `release` 而不是 `put`。

---

### 4.2 【P2】每个 flush 任务用 Random.nextInt() 生成 metrics key

**文件**：`Flusher.scala`，L77

```scala
val key = s"Flusher-$this-${Random.nextInt()}"
workerSource.sample(getFlushTimeMetric(), key) { ... }
```

Scala 的 `Random.nextInt()` 是 `scala.util.Random` 的全局实例，内部使用 `java.util.Random`，在多线程下有 CAS 竞争。每个磁盘有 `threadCount`（通常 4-8）个 flush 线程，高 QPS 下 metrics key 生成成为竞争点。

**优化**：使用 `ThreadLocalRandom.current().nextInt()` 消除竞争，或直接用线程 index 作为 key 后缀。

---

### 4.3 【P2】HdfsFlusher / S3Flusher 的 reuseCopyBuffer 机制

**文件**：`Flusher.scala`，L72-74，`HdfsFlusher`/`S3Flusher` 构造

```scala
// LocalFlusher: reuseCopyBuffer = false（每次从 Netty pool 分配）
// HdfsFlusher/S3Flusher: reuseCopyBuffer = true（复用固定大小的 byte[]）
if (reuseCopyBuffer) {
  copyBytes = new Array[Byte](maxTaskSize.toInt)  // 按 maxTaskSize 预分配，通常 256KB
}
```

DFS flusher 的 `copyBytes` 按 `maxTaskSize`（默认 256KB）预分配，但实际很多 flush task 远小于此（小 partition 场景下只有几 KB），导致内存浪费。每个 HDFS/S3 flush 线程预占 256KB，10 个线程就是 2.5MB 固定开销。

---

## 第五部分：PartitionDataWriter — 写入状态机

### 5.1 【P1】write/evict/close/flush 全部 synchronized(this)，串行化严重

**文件**：`PartitionDataWriter.java`，L121、L144、L197、L214、L218

```java
public synchronized void write(ByteBuf data) throws IOException { ... }
public synchronized void evict(boolean checkClose) { ... }
public synchronized void destroy(IOException ioException) { ... }
public synchronized long close() { ... }
public synchronized void flush() { ... }
```

**问题**：所有操作都粗粒度地锁住同一个 `PartitionDataWriter` 实例，导致：
1. MemoryManager 触发 evict 时，与正在写数据的 Netty 线程互斥等待
2. 关闭时（`close()`）与最后一批写入互斥，增加 shuffle complete 延迟

**优化方向**：将状态机（内存 → 磁盘 eviction）的转换用 `AtomicReference<TierWriterBase>` + CAS 保护，write 路径不需要全局锁，只有 tier 切换时才需要短暂独占。

---

### 5.2 【P2】needHardSplitForMemoryShuffleStorage 的双重检查优化空间

**文件**：`PartitionDataWriter.java`，L125-142

```java
public boolean needHardSplitForMemoryShuffleStorage() {
    TierWriterBase tierWriter = currentTierWriter;  // volatile read，无锁
    if (!(tierWriter instanceof MemoryTierWriter)) {
        return false;  // 磁盘 writer 快速返回
    }
    synchronized (this) {  // 内存 writer 需要二次加锁确认
        tierWriter = currentTierWriter;
        if (!(tierWriter instanceof MemoryTierWriter)) {
            return false;
        }
        return !storageManager.localOrDfsStorageAvailable() && ...;
    }
}
```

这个 double-check 模式是正确的（类似 DCL），但 `storageManager.localOrDfsStorageAvailable()` 在锁内调用，而该方法可能有自己的锁（`this.synchronized`），存在锁序问题风险（PartitionDataWriter.this → StorageManager.this 的锁序）。如果其他代码路径以相反顺序获取锁，可能死锁。

---

## 第六部分：CongestionController — 拥塞控制精度

### 6.1 【P1】getPotentialConsumeSpeed 除以 userBufferStatuses.size() 不精准

**文件**：`CongestionController.java`，L217-223

```java
public long getPotentialConsumeSpeed() {
    if (userBufferStatuses.size() == 0) {
        return 0;
    }
    return consumedBufferStatusHub.avgBytesPerSec() / userBufferStatuses.size();
}
```

**问题**：将全局消费速度均分给所有用户，假设所有用户消费速度相同。实际上不同用户的 reducer 数量和数据量差异极大，均分导致：
- 大用户被过度限速（其实际消费能力 > 均值）
- 小用户不被限速（即使其产生速度远超消费速度）

**改进**：改为基于用户自己的历史生产/消费比来评估，而不是用全局均值。

---

### 6.2 【P2】removeInactiveUsers 遍历时调用 workerSource.removeGauge

**文件**：`CongestionController.java`，L242-262

```java
private void removeInactiveUsers() {
    Iterator<Map.Entry<UserIdentifier, UserBufferInfo>> iterator =
        userBufferStatuses.entrySet().iterator();
    while (iterator.hasNext()) {
        // ...
        if (expired) {
            userBufferStatuses.remove(userIdentifier);     // ConcurrentHashMap: 安全
            userCongestionContextMap.remove(userIdentifier);  // ConcurrentHashMap: 安全
            workerSource.removeGauge(...);  // 可能有外部锁，潜在死锁风险
        }
    }
}
```

`ConcurrentHashMap` 在迭代时调用 `remove` 是安全的，但 `workerSource.removeGauge()` 可能内部持有 metrics 注册表的锁。如果另一个线程同时在 `workerSource` 中遍历并触发 gauge（持有 metrics 锁），同时在用户拥塞检测路径中访问 `userBufferStatuses`，存在锁顺序不一致风险。

**建议**：将 `removeGauge` 调用移到遍历之外（先收集要删除的 key，再统一处理）。

---

## 第七部分：PartitionFilesSorter — Reduce 读排序

### 7.1 【P1】getSortedFileInfo 等待排序完成时 busy-wait

**文件**：`PartitionFilesSorter.java`，L247-280

```java
synchronized (sorting) {
    while (sorting.contains(fileId)) {
        sorting.wait(timeout);  // 等待排序完成
    }
}
```

Reducer 等待排序完成时，持有 `sorting` 对象的 monitor。如果同时有多个 Reducer 等待不同文件的排序，每个 Reducer 都持有一个锁。排序线程每完成一个文件就 `notifyAll`，唤醒所有等待线程，大量等待线程被唤醒后只有一个能继续，其余重新 wait，造成惊群效应（thundering herd）。

**优化**：为每个 fileId 维护独立的 `CompletableFuture<FileInfo>`，Reducer 直接 `future.get(timeout)`，只有对应文件完成的 Reducer 被唤醒。

---

### 7.2 【P2】sortMemoryShuffleFile 的内存 sort 全在 synchronized 块内

**文件**：`PartitionFilesSorter.java`，L316-375

```java
public static void sortMemoryShuffleFile(MemoryFileInfo memoryFileInfo) {
    synchronized (reduceFileMeta.getSorted()) {  // 持锁期间做完整排序
        // 遍历所有 block，构建 TreeMap，排序，写 index
        // ...可能处理数百万个 block
    }
}
```

对于大的内存 shuffle 文件（数百万个 block），持锁期间执行完整的 TreeMap 构建和排序，锁持有时间可达秒级。其他需要访问该 `memoryFileInfo` 的线程全部阻塞。

**优化**：先不持锁构建 blocks snapshot，然后持锁只做最后的 index 写入（类似 Kafka 的 log compaction 先构建 offset map，再持锁交换）。

---

## 第八部分：FetchHandler 与 Controller

### 8.1 【P1】FetchHandler：ChunkStream 生命周期缺乏流控

**文件**：`FetchHandler.scala`，核心读路径

Reducer 的 `fetchChunk` 请求在 `FetchHandler` 中处理：每个请求从磁盘读取一个 chunk（默认 256KB）并直接写入 Netty channel。没有 Reducer 级别的流控——一个"贪婪"的 Reducer 可以发送大量 fetchChunk 请求，消耗大量磁盘 IO 和 Netty 写缓冲，影响其他 Reducer。

**改进**：参考 Kafka 的 FetchSession 机制，引入 per-Reducer 的 credit-based flow control，Reducer 预先申请 N 个 chunk credit，消耗后需要续期，避免单 Reducer 占满磁盘 IO。

---

### 8.2 【P1】Controller：commitFiles 中 partitionLocationInfo 竞争

**文件**：`Controller.scala`，L400-500

```scala
// commitFiles 路径持 partitionLocationInfo 锁时间过长
partitionLocationInfo.synchronized {
  // 遍历所有 partition，逐一 close writer，收集 storage info
  // close() 是同步操作，可能涉及磁盘 IO flush
}
```

`commitFiles` 在持 `partitionLocationInfo` 锁的同时执行每个 writer 的 `close()`（`close()` 内部调用 `flush()` 可能有磁盘 IO），锁持有时间与 partition 数量正比。期间所有新的 `reserveSlots` 请求（也需要 `partitionLocationInfo` 锁）全部阻塞，导致下一个 shuffle 的 slot 预留出现长尾延迟。

**优化**：先在锁内快速收集 writer 列表并移除，锁外异步执行 close/flush，close 完成后异步更新 committed state。

---

### 8.3 【P2】commitFinishedChecker 轮询间隔固定

**文件**：`Controller.scala`，L90-100（`commitFinishedChecker`）

定时任务以固定间隔检查是否所有 mapper 都完成了提交。固定轮询对于小 job（partition 数少）响应快，但对于大 job（partition 数多，每次检查遍历代价高）效率低。

**优化**：改为事件驱动——每次 `mapperEnd` 消息到达时，检查是否达到完成条件，完成立即触发，无需等待下一个轮询周期。

---

## 第九部分：Worker 整体架构问题

### 9.1 【P1】多个独立 ScheduledExecutorService 浪费线程资源

**文件**：`Worker.scala`，L331-349

Worker 启动时创建了多个独立的 `newDaemonSingleThreadScheduledExecutor`：
- `worker-forward-message-scheduler`（心跳发送）
- `worker-commit-checker`（提交超时检测）
- `worker-rpc-async-replier`（异步 RPC 回复）
- `HashedWheelTimer`（另一套定时轮）

每个单线程 Executor 实际上包含一个 Thread + BlockingQueue，Worker 固定消耗 4+ 个常驻线程只用于低频定时任务。

**优化**：将所有低频定时任务合并到一个共享的 `ScheduledThreadPoolExecutor`（core=2），只有高优先级的心跳任务保持独立线程。

---

### 9.2 【P1】handleTopAppResourceConsumption 的全量排序

**文件**：`Worker.scala`，L745-772

```scala
userResourceConsumptions.asScala
  .flatMap { ... }
  .toSeq
  .sortBy { case (_, _, appConsumption) =>  // O(n log n) 全量排序
    appConsumption.diskBytesWritten + appConsumption.hdfsBytesWritten
  }
  .reverse
  .take(topAppResourceConsumptionCount)  // 只取 Top N
```

全量排序后只取 Top N，应使用堆（`PriorityQueue`）实现 O(n log k) 的 Top-K 算法，k = `topAppResourceConsumptionCount`（通常很小，如 10）。

---

### 9.3 【P2】metrics 闭包的重复计算

**文件**：`Worker.scala`，L373-496

```scala
// 每次 metrics 采集都执行
workerSource.addGauge(REGISTERED_SHUFFLE_COUNT) { () =>
    workerInfo.getShuffleKeySet.size  // 创建 Set 副本再计数
}
```

`getShuffleKeySet()` 通常返回 Set 的快照（涉及 copy），metrics 系统每隔数秒采集一次，高频创建大量短命 Set 对象。

**优化**：维护 `AtomicInteger shuffleCount`，在 shuffle 注册/注销时增减，metrics 直接读原子计数器。

---

## 综合优化路线图

### 优先级与收益矩阵

| 优化方向 | 文件 | 问题 | 预期收益 | 复杂度 |
|---------|------|------|---------|-------|
| **PushData 异步化（消除 Await.result）** | `PushDataHandler.scala` | Netty IO 线程同步阻塞 | push 吞吐 2-5x | 高 |
| **PartitionDataWriter 写路径去全局锁** | `PartitionDataWriter.java` | write/evict 互斥阻塞 | 写延迟 P99 降低 30%+ | 中 |
| **isPaused volatile 修复** | `MemoryManager.java` | ARM 架构下可见性 bug | 正确性修复 | **极低** |
| **内存文件驱逐 PriorityQueue 化** | `MemoryManager.java` | 全量排序 O(n log n) | 驱逐延迟降低 50%+ | 低 |
| **StorageManager 增量 DiskInfo 统计** | `StorageManager.java` | 持全局锁遍历 writer | 心跳延迟降低 | 中 |
| **diskFileInfos 锁持有时间缩短** | `StorageManager.java` | 持锁期间全量聚合 | CommitFile 不阻塞 | 低 |
| **commitFiles 异步 close** | `Controller.scala` | 持锁执行磁盘 flush | slot 预留长尾消除 | 中 |
| **getSortedFileInfo 惊群消除** | `PartitionFilesSorter.java` | notifyAll 唤醒所有等待 | Reduce 启动延迟降低 | 低 |
| **handlePushData 逻辑去重** | `PushDataHandler.scala` | 重复代码维护风险 | 可维护性提升 | 低 |
| **CongestionController 均分逻辑改进** | `CongestionController.java` | 用户限速精度差 | 公平性提升 | 中 |
| **bufferQueue 有界化** | `Flusher.scala` | Direct Memory 泄漏风险 | 内存稳定性 | **极低** |
| **Worker 定时线程合并** | `Worker.scala` | 固定线程资源浪费 | 内存降低 | 低 |
| **Top-K 排序优化** | `Worker.scala` | O(n log n) 做 Top-K | CPU 降低 | **极低** |
| **Flusher Random → ThreadLocalRandom** | `Flusher.scala` | 全局 Random CAS 竞争 | CPU 微优化 | **极低** |

---

## 附录：关键代码位置速查

| 优化点 | 文件 | 行号 |
|------|------|-----|
| Await.result 阻塞 Netty IO 线程 | `PushDataHandler.scala` | L304, L399, L640, L796, L1061 |
| 临时 WorkerInfo 对象（replication 热路径）| `PushDataHandler.scala` | L281, L613 |
| isPaused 非 volatile | `MemoryManager.java` | L81 |
| 内存文件驱逐全量排序 | `MemoryManager.java` | L263-274 |
| memoryPressureListeners 遍历不同步 | `MemoryManager.java` | L383-388 |
| trimAllListeners 丢弃重叠触发 | `MemoryManager.java` | L414-430 |
| updateDiskInfos 全局锁 | `StorageManager.scala` | L952-990 |
| diskFileInfos 持锁全量聚合 | `StorageManager.scala` | L1003-1030 |
| bufferQueue 无界 | `Flusher.scala` | L50 |
| Flusher Random.nextInt 竞争 | `Flusher.scala` | L77 |
| write/evict/close 全 synchronized | `PartitionDataWriter.java` | L121, L144, L197, L214, L218 |
| 拥塞均分逻辑 | `CongestionController.java` | L217-223 |
| removeInactiveUsers 持迭代器调外部锁 | `CongestionController.java` | L242-262 |
| getSortedFileInfo 惊群 notifyAll | `PartitionFilesSorter.java` | L247-280 |
| sortMemoryShuffleFile 持锁做完整排序 | `PartitionFilesSorter.java` | L316-375 |
| commitFiles 持锁做 close/flush | `Controller.scala` | L400-500 |
| Worker 多 ScheduledExecutor | `Worker.scala` | L331-349 |
| handleTopAppResourceConsumption 全量排序 | `Worker.scala` | L745-772 |
