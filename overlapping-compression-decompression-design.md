# Celeborn Overlapping Compression & Decompression — 调研与设计文档

> **任务范围**:本次仅完成调研与方案设计,成果为仓库根目录的设计文档(已写入),**不进行任何代码改动**。后续是否实施由用户另行决定。
>
> **覆盖范围**:写路径压缩(Write Path Compression)与读路径解压(Read Path Decompression)的 overlapping 优化。

---

## 一、线程安全审计

在异步化改造前,必须解决 Compressor 与 Decompressor 的线程安全隐患。

### 1.1 现状分析

| 类 | 可变实例字段 | 线程安全 |
|---|---|---|
| `ZstdCompressor` | `Checksum checksum`(CRC32)、`byte[] compressedBuffer`、`int compressedTotalSize` | ❌ 非线程安全 |
| `Lz4Compressor` | `Checksum checksum`(CRC32)、`byte[] compressedBuffer`、`int compressedTotalSize` | ❌ 非线程安全 |
| `ZstdDecompressor` | `Checksum checksum`(CRC32) | ❌ 非线程安全 |
| `Lz4Decompressor` | `Checksum checksum`(XXHash) | ❌ 非线程安全 |

**详情**:

- `ZstdCompressor.compress()`(`client/.../compress/ZstdCompressor.java:40-55`):使用 `compressedBuffer` 存储压缩中间结果,`compressedTotalSize` 记录最终长度,末尾 `checksum.reset().update()` — 实例有状态,不可并发调用。
- `Lz4Compressor.compress()`(Lz4Compressor.java:43-53):同上模式,`LZ4BlockOutputStream` 内部有 `finish()` 调用,压缩 buffer 状态贯穿整个 compress 周期。
- `ZstdDecompressor.decompress()`(ZstdDecompressor.java:30-33):调用链为 `decompress(buffer, target, 0)` → 校验 header → `Zstd.decompressByteArray()` → `checksum.reset().update()` — checksum 是实例级可变字段。
- `Lz4Decompressor.decompress()`(Lz4Decompressor.java:29-38):同上,`LZ4BlockInputStream` 内部 read + checksum 验证。

**当前为什么安全**:Compressor 通过 `ThreadLocal<Compressor>` 隔离,每个线程持有独立实例执行同步压缩;Decompressor 在 Spark reduce task 单线程内同步调用。一旦引入并发解压/压缩(读路径异步解压、写路径 pipeline),ThreadLocal 隔离被打破(多个 worker 线程共享池)。

### 1.2 对比:Uniffle Codec 的线程安全设计

Uniffle 采用**单例(Singleton/LazyHolder)模式**,而非 ThreadLocal:

| Codec | 文件 | 模式 | 线程安全 |
|-------|------|------|---------|
| `Lz4Codec` | `common/.../compression/Lz4Codec.java` | LazyHolder 单例 | ✅ 无状态(每次 compress 创建局部 LZ4Compressor) |
| `ZstdCodec` | `common/.../compression/ZstdCodec.java` | LazyHolder 单例 + 参数注入 | ✅ 无状态(compress 内分配局部 buffer,支持 workerNumber 并行) |
| `SnappyCodec` | `common/.../compression/SnappyCodec.java` | LazyHolder 单例 | ✅ 无状态 |

Uniffle 的 `Codec.compress(byte[])` 方法内部分配临时 buffer 和压缩器,每次调用返回完整的 `byte[]` 结果,**天然线程安全**。Celeborn 的 ThreadLocal 模式虽然当前安全,但在异步化后不适用——要么改为无状态 compress(像 Uniffle),要么通过对象池管理。

**建议**:**Celeborn Compressor/Decompressor 应参考 Uniffle 的无状态模式**,将 `checksum`、`compressedBuffer`、`compressedTotalSize` 移为 `compress()/decompress()` 方法内的局部变量,彻底消除状态。回退并发的单元测试用于验证。

### 1.3 改造方案

| 类 | 改造 |
|----|------|
| `ZstdCompressor` | `checksum`、`compressedBuffer`、`compressedTotalSize` → 方法内局部变量;`Zstd.compressByteArray()` 本身静态无状态 |
| `Lz4Compressor` | `checksum` → 局部变量;`LZ4BlockOutputStream` 在 `compress()` 内创建/使用/销毁 |
| `ZstdDecompressor` | `checksum` → 方法内局部变量 |
| `Lz4Decompressor` | `checksum` → 方法内局部变量;`LZ4BlockInputStream` 在 `decompress()` 内创建/销毁 |

**改造验证**:分别编写并发单测(多线程共享同一实例并调用 compress/decompress)确保无 race condition。

---

## 二、Uniffle 方案调研:写路径 Overlapping Compression

> Uniffle 已实现写路径的 overlapping compression,以下为源码级分析。

### 2.1 核心设计:`OverlappingCompressionDataPusher`

**文件**: `client-spark/common/src/main/java/org/apache/spark/shuffle/writer/OverlappingCompressionDataPusher.java`

```java
// 继承 DataPusher,重写 send() 方法
public class OverlappingCompressionDataPusher extends DataPusher {

    private static final ExecutorService compressionThreadPool =
        Executors.newCachedThreadPool(...);  // 全局共享

    @Override
    public CompletableFuture<AddBlockEvent> send(AddBlockEvent event) {
        return CompletableFuture
            .supplyAsync(() -> {
                // Step 1: 在 compressionThreadPool 中
                // 遍历所有 block,调用 getData() 触发 DeferredCompressedBlock 的
                // rebuildFunction → 实际压缩(延迟求值)
                event.getShuffleDataInfoList().forEach(sdi -> sdi.getData());
                return event;
            }, compressionThreadPool)  // ← 专用压缩线程池
            .thenCompose(processedEvent ->
                super.send(processedEvent)  // Step 2: DataPusher 线程发送
            );
    }
}
```

**两步流水线**:
1. **Step 1** (compressionThreadPool): 遍历所有 ShuffleDataInfo,触达 `getData()` → `DeferredCompressedBlock.initialize()` → `rebuildFunction` → 实际压缩
2. **Step 2** (DataPusher 线程池): 调用父类 `DataPusher.send()` 发送已压缩数据

### 2.2 延迟压缩:DeferredCompressedBlock

**文件**: `client-spark/common/src/main/java/org/apache/spark/shuffle/writer/WriteBufferManager.java:426-474`

```java
private ShuffleBlock createDeferredCompressedBlock(int partitionId, WriterBuffer wb) {
    // 构建 rebuildFunction: 调用 codec.compress() 的闭包
    RebuildFunction rebuildFunction = block -> {
        long start = System.currentTimeMillis();
        byte[] compressed = codec.get().compress(wb.getData());
        block.compressedDataLen = compressed.length;
        block.data = ByteBuffer.wrap(compressed);
        compressTime += System.currentTimeMillis() - start;
    };

    return new DeferredCompressedBlock(rebuildFunction, wb.getData().length);
    // 注意: 此时尚未压缩,只记录了 rebuildFunction
}
```

`DeferredCompressedBlock` 继承 `CompressedBlock`:
- 持有 `rebuildFunction` (Runnable), 直到 `getData()` 首次调用时才执行实际压缩
- `getData()` → `initialize()` → `rebuildFunction.rebuild(this)` → 压缩
- 之后 `isInitialized = true`, 后续调用直接返回缓存结果

### 2.3 启用条件与配置

```java
// WriteBufferManager.java:191
this.isDeferredCompression = OverlappingCompressionDataPusher.isEnabled(rssConf);

// OverlappingCompressionDataPusher.isEnabled():
return rssConf.getBoolean(RSS_OVERLAPPING_COMPRESSION_ENABLED, false);
```

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `rss.overlapping.compression.enabled` | **false** | 是否启用延迟压缩 |

### 2.4 压缩统计:StatisticsCodec

```java
// common/.../compression/StatisticsCodec.java
// 装饰器模式,包装 Codec
public class StatisticsCodec extends Codec {
    private final Codec delegate;
    private long compressTime;
    private long dealCompressedBytes;
    private long dealUncompressedBytes;

    @Override
    public byte[] compress(byte[] src) {
        long start = System.currentTimeMillis();
        byte[] result = delegate.compress(src);
        compressTime += System.currentTimeMillis() - start;
        dealUncompressedBytes += src.length;
        dealCompressedBytes += result.length;
        return result;
    }
}
```

在 `WriteBufferManager.close()` 时输出 P25/P50/P75/P90 百分位统计,用于性能分析。

### 2.5 Uniffle 写路径数据流总结

```
Spark map task thread:              compressionThreadPool:        DataPusher thread pool:
  serialize record                        │                           │
  → copy to WriterBuffer                  │                           │
  → flush (buffer full)                   │                           │
    → createDeferredCompressedBlock()     │                           │
      (仅记录 rebuildFunction,无压缩)      │                           │
    → addBlockEvent →                     │                           │
    → DataPusher.send(event) ────────────>│ supplyAsync:                │
                                         │   sdi.getData()             │
                                         │   → decompress (延迟求值!)   │
                                         │   → 触发压缩               │────> thenCompose:
                                         │                             │   super.send(event)
                                         │                             │   → 网络发送
```

**关键设计优势**:
- 压缩与 map task 完全解耦: map task 线程不执行压缩,仅构建压缩任务描述(rebuildFunction)
- 压缩与网络发送流水线化: compressionThreadPool 压缩当前 batch 时,DataPusher 线程池正在发送上一个 batch
- Codec 单例无状态: 天然支持多线程并发压缩,无需 ThreadLocal

---

## 三、Celeborn 写路径:Compression Overlapping 优化

### 3.1 写路径全景

Celeborn 写路径中,压缩发生在 `ShuffleClientImpl.pushOrMergeData()`(第 1050-1058 行):

```java
// ShuffleClientImpl.java:1050-1058
if (shuffleCompressionEnabled && !skipCompress) {
    final Compressor compressor = compressorThreadLocal.get();
    compressor.compress(data, offset, length);       // 同步阻塞
    data = compressor.getCompressedBuffer();
    offset = 0;
    length = compressor.getCompressedTotalSize();
}
```

压缩后数据通过 `System.arraycopy(data, offset, body, BATCH_HEADER_SIZE, length)`(第 1083 行)拷贝到 batch body → 可选加密 → Netty async pushData。

引擎差异导致两条不同的推送路径:

#### 3.1.1 DataPusher 异步推送路径(doPush=true)

```
Spark map task thread:                      DataPusher background thread:
  serialize record                              │
  → copy to sendBuffers[part]                   │
  → flushSendBuffer()                           │
    → dataPusher.addTask()                     │
      → computeBatchCRC (caller thread)         │
      → copy to PushTask                        │
      → enqueue to DataPushQueue  =============>│ dequeue PushTask
  → continue serializing next batch            │ → pushData(task)
                                               │   → pushOrMergeData()
                                               │     → COMPRESS ← 同步阻塞!
                                               │     → encrypt(optional)
                                               │     → Netty pushData (async)
                                               │ → wait for response
                                               │ → reclaim task buffer
```

**关键事实**:
- 压缩在 **DataPusher 的后台线程**中同步执行(`celeborn-client-data-pusher-{taskId}`)
- map task 线程此时已继续序列化下一批数据
- DataPushQueue 按 worker inflight 容量调度,已在网络层面流水线化
- CRC 在 `addTask()` 阶段(map task 线程)已对原始数据计算,`pushData()` 调用的是不带 CRC 的版本(第 212 行 `pushData(task.getBuffer(), ...)` 走 `pushOrMergeData(..., skipCompress=false)`)

#### 3.1.2 Giant Record / 直接推送路径(doPush=true)

```
Spark map task thread:
  serialize giant record
  → pushDataWithCRC() 或 pushData()
    → pushOrMergeData()
      → compute CRC (if pushDataWithCRC)
      → COMPRESS ← 同步阻塞!
      → encrypt(optional)
      → Netty pushData (async)
```

Giant record 直接在 map task 线程同步压缩,然后异步网络发送。**存在问题**:压缩时 map task 线程被阻塞,无法做任何有用工作。

#### 3.1.3 Merge 聚合路径(doPush=false)

```
Spark map task thread (SortBasedPusher.pushData):
  sort records by partition
  for each partition's batch:
    → mergeData() → pushOrMergeData()
      → COMPRESS ← 同步阻塞
      → store batch in pushState.batchesMap
  → pushMergedData()
    → iterate batchesMap
    → for each: build PushData → Netty pushData (async)
```

此路径下多个 batch 的压缩在 map task 线程上顺序执行,然后批量发送。**压缩时间完全阻塞 map task 的生产力**。

#### 3.1.4 Flink 写入路径

Flink 在到达 Celeborn 之前使用自身的 `BufferCompressor` 进行压缩(`client-flink/.../BufferPacker.java`),Celeborn 侧的 `shuffleCompressionEnabled` 对其不生效(或 skipCompress 跳过)。**本节提出的写路径压缩优化不适用于 Flink 端。**

### 3.2 Celeborn vs Uniffle 写路径架构对比

| 维度 | Celeborn | Uniffle |
|------|----------|---------|
| **压缩位置** | DataPusher 线程内(pushOrMergeData) 或 map task 线程(merge/giant) | compressionThreadPool(专用线程池) |
| **压缩方式** | 同步阻塞(`compress()` 返回 void,结果存实例字段) | 延迟求值(DeferredCompressedBlock + rebuildFunction) |
| **Compressor 生命周期** | ThreadLocal,每线程一个实例 | Codec 单例,每次 compress 分配临时 buffer |
| **线程安全** | 依赖 ThreadLocal 隔离;异步化需改造 | Codec 无状态,天然线程安全 |
| **流水线机制** | 无(压缩与发送串行) | CompletableFuture 两步流水线(compress → send) |
| **压缩与 map task 耦合** | merge 路径耦合;DataPusher 路径部分解耦 | 完全解耦(map task 仅创建 rebuildFunction) |
| **可观测性** | 无压缩统计 | StatisticsCodec 装饰器,P25-P90 百分位统计 |
| **写入粒度** | batch(多条 record 聚合) | block(多条 record 聚合,粒度类似) |

**关键差距**:
1. Uniffle 已实现写路径 overlapping compression,Celeborn **目前为零**
2. Uniffle 的 Codec 设计(Celeborn 的 Compressor 等效物)天然无状态,为并发准备了基础;Celeborn 需先做 ThreadSafety 改造
3. Uniffle 的压缩与网络发送通过 CompletableFuture 流水线化,压缩完全不阻塞任何一条路径上的后续发送

### 3.3 问题分析

**核心矛盾**:压缩(CPU 密集)与 map task 序列化/聚合(CPU 密集)及网络发送(IO 密集)未能充分流水线化:

1. **DataPusher 路径**:虽然 map task 线程不被压缩阻塞(压缩在 DataPusher 线程),但 DataPusher 线程的"压缩 + 网络发送"是串行的,下一批数据必须等压缩完成才能开始网络发送
2. **Giant Record 路径**:map task 线程直接阻塞在压缩上
3. **Merge 路径(SortBasedPusher)**:所有 batch 的压缩在 pushMergedData 之前完成,map task 无法在此期间进行任何有用工作

**对于 zstd level 3 场景**,压缩成本显著高于 lz4(通常数倍),此阻塞窗口客观存在。

### 3.4 优化方案

#### 3.4.1 方案 A(推荐):对齐 Uniffle 的 Deferred Compression Pipeline

参考 Uniffle `OverlappingCompressionDataPusher` 的设计,引入:

1. **DeferredCompressedBlock**: 在 `addTask()` 时不压缩,仅记录待压缩的数据引用 + `rebuildFunction` 闭包
2. **压缩线程池**(全局共享 `compressionThreadPool`):
   - DataPusher 收到 `AddBlockEvent` 后,将压缩提交到 `CompletableFuture.supplyAsync(..., compressionThreadPool)`
   - 压缩完成后 `.thenCompose` 触发网络发送
3. **Codec 无状态化**(第一章):所有 Compressor 改为方法内局部状态

**与 Uniffle 的差异**:
- Celeborn 的 batch 头含原始长度信令,DeferredCompressedBlock 需额外携带 `originalLength`
- Celeborn 有加密流程(`cryptoHandler`),设在压缩之后;DeferredCompressedBlock 需暴露 `getCompressedByteBuf()` 供加密使用

```
实现架构:
  addTask(): 创建 DeferredPushTask(dataRef, rebuildFunction)
  DataPusher.send():
    CompletableFuture.supplyAsync(
      task.getBody() → rebuildFunction → compress → encrypt →
      batchHeader + compressedBody,
      compressionThreadPool
    ).thenCompose(body → Netty pushData(body))
```

#### 3.4.2 方案 B(备选):在 DataPusher.addTask() 中同步压缩

不引入延迟压缩,仅在 addTask 阶段由 map task 线程调用 compression。改动更简单,但压缩与序列化共享同一线程,收益低于方案 A。

**差异对比**:

| | 方案 A (Deferred) | 方案 B (addTask压缩) |
|---|---|---|
| 与 Uniffle 对齐 | ✅ 完全对齐 | ❌ 不同 |
| 压缩与序列化重叠 | ✅ (分别在不同线程) | ❌ (共享 map task 线程) |
| 压缩与网络发送重叠 | ✅ (CompletableFuture 流水线) | 部分(DataPusher 无压缩,仅发送) |
| 实现复杂度 | 中等(需 Deferred 机制) | 低(仅移动压缩点) |
| DataPushQueue 内存 | 原始数据(大) | 压缩后数据(小) |

**推荐方案 A**:收益更大,且与 Uniffle 设计对齐,降低社区接受门槛。

#### 3.4.3 Merge 路径处理

Merge 路径(doPush=false)的多个 batch 压缩后存储在 `pushState.batchesMap`,随后 `pushMergedData()` 批量发送:
- 当前 map task 线程顺序压缩所有 batch → 批量发送,已部分重叠
- 可将 mergeData 中的压缩改为 deferred,pushMergedData 时触达 `getData()`,通过 CompletableFuture 批量等待然后发送
- 首版建议保持 merge 路径不变,聚焦 DataPusher 路径(doPush=true)

#### 3.4.4 引擎覆盖范围

| 引擎 | Compression Overlapping 适用? | 说明 |
|------|------------------------------|------|
| Spark (HashBased) | ✅ 适用 | DataPusher 异步路径,方案 A 直接生效 |
| Spark (SortBased) | ⚠️ 部分适用 | DataPusher 仅用于超大 batch;merge 路径已有部分重叠 |
| Spark (Giant Record) | ✅ 适用 | 直接调用路径,同样可 deferred |
| Flink | ❌ 不适用 | 自身有 BufferCompressor,不到达 Celeborn Compressor |
| MR / Tez | ⚠️ 待评估 | 需确认其推送路径是否使用 DataPusher |

---

## 四、读路径:Decompression Overlapping 优化

### 4.1 现状分析

解压点在 `CelebornInputStreamImpl.fillBuffer()`(CelebornInputStream.java:925-931),**在 Spark reduce task 线程上同步执行**:

```java
if (shouldDecompress) {
    int originalLength = decompressor.getOriginalLen(compressedBuf);
    if (rawDataBuf.length < originalLength) {
        rawDataBuf = new byte[originalLength];
    }
    limit = decompressor.decompress(compressedBuf, rawDataBuf, 0); // 同步阻塞!
}
```

**数据流全貌**:

```
fillBuffer() 核心循环:
  while (currentChunk.isReadable() || moveToNextChunk()) {
    1. read batch header (mapId/attemptId/batchId/size)
    2. attemptId dedup / failedBatches filter  ← 解压之前
    3. [可选解密: cryptoHandler.decrypt()]      ← 解压之前
    4. read compressed data into compressedBuf
    5. DECOMPRESS ← 同步阻塞!
    6. shuffleIntegrityCheck                    ← 解压之后
    7. return hasData → reduce task 消费 record
  }
```

**关键事实**:

1. **压缩块粒度 = per-batch**:chunk = N 个 batch 顺序拼接,每个 batch = 16 字节头(mapId/attemptId/batchId/size) + 一个独立、自描述的压缩块(头部含 magic/compressedLen/originalLen/CRC32)。块间无流状态,**可任意并行解压、无需按顺序**。
2. **网络 fetch 已流水线化**:`WorkerPartitionReader` 的 `maxReqsInFlight`(默认 3)让网络 I/O 与处理已重叠;`WorkerPartitionReader`/`LocalPartitionReader`/`DfsPartitionReader` 返回的都是压缩态 chunk。
3. **未重叠的部分**:解压 与 record 反序列化/聚合/排序 串行在同一线程。这正是需要优化掉的部分。
4. **解压前有 attemptId 过滤(第 849-878 行)、可选解密(第 881-910 行)**,异步化时这套顺序语义必须保持。
5. **`shuffleIntegrityCheckEnabled` 校验在解压之后(第 935-937 行)**,保留在 task 线程按序执行。

### 4.2 Uniffle 读路径详解:DecompressionWorker

**文件**: `client/src/main/java/org/apache/uniffle/client/impl/DecompressionWorker.java`

#### 4.2.1 核心结构

```java
public class DecompressionWorker {
    private final Codec codec;
    private final ExecutorService threadPool;   // 固定线程池(默认 1 线程)
    private final Semaphore semaphore;          // 背压信号量
    private final int maxConcurrentDecompressionSegments;

    // 二维索引: by (batchIndex → segmentIndex)
    private final ConcurrentHashMap<Integer,
        ConcurrentHashMap<Integer, DecompressedShuffleBlock>> resultMap;

    // 统计指标
    private long decompressionMillis;     // 实际解压耗时
    private long bufferAllocationMillis;  // buffer 分配耗时
    private long decompressionBytes;      // 解压总字节
    private long peekMemoryUsed;          // 峰值内存
}
```

**线程池策略**:
- 每个 reader(partition)创建一个 `DecompressionWorker`
- 线程池为固定大小(默认 1),避免 N个 partition × M线程的爆炸
- **关键**:信号量背压**仅在 `threads==1` 时启用** — 多线程下 Semaphore acquire+锁会导致死锁

#### 4.2.2 异步提交:`add()`

```java
public void add(int batchIndex, ShuffleDataResult shuffleDataResult) {
    // 遍历 DataResult 中的每个 BufferSegment
    for (BufferSegment segment : shuffleDataResult.getBufferSegments()) {
        try {
            semaphore.acquire();  // ← 背压: 获取许可
        } catch (InterruptedException e) { ... }

        CompletableFuture<ByteBuffer> future = CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            // 切片压缩数据
            ByteBuffer compressed = sdr.getDataBuffer().slice(offset, compressedLen);
            // 分配解压目标 buffer (direct 或 heap, 与源类型一致)
            ByteBuffer target = allocateTargetBuffer(originalLen);
            bufferAllocationMillis += System.currentTimeMillis() - start;
            // 解压
            codec.decompress(compressed, originalLen, target, 0);
            decompressionMillis += System.currentTimeMillis() - start;
            // 更新内存统计
            peekMemoryUsed = Math.max(peekMemoryUsed, nowMemoryUsed.addAndGet(originalLen));
            return target;
        }, threadPool);

        // 登记到 resultMap[batchIndex][segmentIndex]
        resultMap.get(batchIndex).put(segmentIndex, new DecompressedShuffleBlock(future, ...));
    }
}
```

#### 4.2.3 顺序消费:`get()`

```java
public DecompressedShuffleBlock get(int batchIndex, int segmentIndex) {
    DecompressedShuffleBlock block = resultMap.get(batchIndex).remove(segmentIndex);
    // 消费完成后释放信号量许可
    semaphore.release();
    // 有序清理: 之前的 batch 和 segment 已被消费,清理 resultMap 条目
    cleanupPreviousEntries(batchIndex);
    return block;
}
```

**Cleanup 机制**:上游按顺序消费 `(batchIndex, segmentIndex)`,消费者每次调用 `get()` 后,worker 清理所有 ≤ 已消费 index 的 Map 条目,避免内存泄漏。

#### 4.2.4 消费端等待:`DecompressedShuffleBlock`

```java
public class DecompressedShuffleBlock extends ShuffleBlock {
    private final CompletableFuture<ByteBuffer> future;

    public ByteBuffer getByteBuffer() {
        // 阻塞等待解压完成(带超时)
        return future.get(fetchSecondsThreshold, TimeUnit.SECONDS);
    }
}
```

**关键**:`f.get()` 的阻塞时间 = 解压线程还没来得及完成的部分,这是衡量"重叠效果"的核心指标 — 真正的解压耗时已隐藏在异步线程中,只有"等待"时间被 task 线程感知。

#### 4.2.5 关闭与统计

```java
public void close() {
    threadPool.shutdown();
    long decompressionThroughput = decompressionBytes /
        Math.max(decompressionMillis / 1000.0, 0.001);
    // 输出完整统计日志
    LOG.info("Overlapping decompression stats: bufferAllocation={}ms, " +
        "decompression={}ms, getWait={}ms, peekMemoryUsed={}MB, " +
        "decompressionThroughput={}MB/s", ...);
}
```

#### 4.2.6 集成点:`ShuffleReadClientImpl.read()`

```java
// ShuffleReadClientImpl.java:358-386
private int read() {
    ShuffleDataResult sdr = ...; // 从网络/本地/Dfs 获取一个 chunk
    if (decompressionWorker != null) {
        // 异步提交整个 chunk 的解压任务
        decompressionWorker.add(batchIndex++, sdr);
    }
    bufferSegmentQueue.addAll(sdr.getBufferSegments());
    return sdr.getBufferSegments().size();
}

// readShuffleBlockData():
if (decompressionWorker != null) {
    // 从 worker 取已解压的 block(可能阻塞等待)
    block = decompressionWorker.get(batchIndex - 1, segmentIndex++);
} else {
    // 同步路径
    block = new CompressedShuffleBlock(compressedData, originalLen);
}
```

**关键:即使 block 被跳过(去重/过滤),也必须调用 `decompressionWorker.get()` 释放信号量许可**(Uniffle 后续 bugfix 的重点)。

### 4.3 Celeborn 读路径优化方案

#### 方案 A(推荐):对齐 Uniffle 的 DecompressionWorker

**整体设计**:

1. **新文件** `client/src/main/java/org/apache/celeborn/client/read/DecompressionWorker.java`:
   - 每个 `CelebornInputStreamImpl` 持有一个 worker(per-partition reader)
   - 内部线程池 = **ShuffleClient 级全局共享 daemon 线程池**(线程数可配,默认 1)
   - `add(batchIndex, compressedBuf, originalLen)`:提交异步解压,结果记录在 `ConcurrentHashMap<Integer, CompletableFuture<byte[]>>` 中
   - `get(batchIndex)`:清理 previous entries,返回 `CompletableFuture<byte[]>`,主线程调用 `.get()`
   - 背压:信号量控制并发解压后总字节数

2. **Decompressor 线程安全改造**(见第一章):`checksum` 移为方法内局部变量

3. **改造 `CelebornInputStream.java`**:
   - `fillBuffer()` 中解压调用改为 `decompressionWorker.get(batchIdx).get()`
   - shuffleIntegrityCheck 保留在 task 线程按序执行
   - `close()` 时 shutdown worker

4. **对比 Celeborn vs Uniffle 读路径设计**:

| 维度 | Uniffle DecompressionWorker | Celeborn 适配方案 |
|------|---------------------------|------------------|
| 解压粒度 | per BufferSegment(chunk 内多个 segment) | per batch(chunk 内多个 batch) |
| 索引维度 | (batchIndex, segmentIndex) 二维 | batchIndex 一维(每 batch 一个独立压缩块) |
| Codec 线程安全 | ✅ 天然无状态 | ❌ 需改造 checksum 为局部变量 |
| 背压机制 | Semaphore(仅在 threads==1) | 同:Semaphore 限制在途字节数 |
| 跳过 block 处理 | 必须调用 get() 释放许可 | 同:必须 get() 释放许可 |
| 统计指标 | decompression/bufferAllocation/getWait/throughput | 对齐:解压耗时 + 等待耗时 + 吞吐 |

#### 方案 B(备选):next-batch 预解压

改动更小的方案:在 `fillBuffer()` 成功解压 batch N 后,立即通过全局共享线程池提交 batch N+1 的预读+解压。下次 `fillBuffer()` 调用时,`future.get()` 大概率已完成。深度为 1 的流水线。

### 4.4 引擎覆盖范围

| 引擎 | Decompression Overlapping 适用? | 说明 |
|------|-------------------------------|------|
| Spark | ✅ 适用 | 通过 CelebornInputStream 读取,解压在 reduce task 线程 |
| Flink | ❌ 不适用 | 自身使用 BufferDecompressor,不经过 Celeborn Decompressor |
| MR / Tez | ⚠️ 待确认 | 需验证其读路径是否使用 CelebornInputStream |

---

## 五、配置与开关

### 5.1 读路径配置(CelebornConf.scala,client 分类)

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `celeborn.client.shuffle.overlappingDecompression.enabled` | boolean | **false** | 是否启用异步解压流水线 |
| `celeborn.client.shuffle.overlappingDecompression.threads` | int | 1 | 共享解压线程池大小(ShuffleClient 级全局) |
| `celeborn.client.shuffle.overlappingDecompression.maxInflightBytes` | int | 262144 (256KB) | 在途解压数据最大字节数(Semaphore 背压) |

**生效条件**:`shuffleCompressionCodec != none` 且非加密场景时生效。

### 5.2 写路径配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `celeborn.client.shuffle.overlappingCompression.enabled` | boolean | **false** | 是否启用延迟压缩(DataPusher 路径 deferred 机制) |
| `celeborn.client.shuffle.overlappingCompression.threads` | int | 1 | 压缩线程池大小(全局共享) |

**生效条件**:`shuffleCompressionCodec != none` 且 DataPusher 路径(doPush=true)时生效。

### 5.3 Uniffle 对应配置参考

| Uniffle 配置 | 默认值 | Celeborn 对应 |
|-------------|--------|---------------|
| `rss.overlapping.compression.enabled` | false | `overlappingCompression.enabled` |
| `rss.overlapping.decompression.enabled` | false | `overlappingDecompression.enabled` |
| `rss.read.overlapping.decompression.fetch.seconds.threshold` | 300 | (待定,get 超时) |
| `rss.read.overlapping.decompression.max.concurrent.segments` | 10 | `maxInflightBytes`(改用字节,而非 segment 计数) |
| `rss.read.overlapping.decompression.thread.num` | 2 | 1(更保守) |

**差异说明**:Uniffle 以 `maxConcurrentSegments`(segment 个数)控制背压,而 segment 大小不一;Celeborn 建议改用 `maxInflightBytes`(解压后总字节数),背压更精确。

---

## 六、测试策略

### 6.1 单元测试

| 测试 | 说明 |
|------|------|
| `DecompressionWorkerTest.java` | 基本 add/get 顺序、背压触发、跳过块许可释放、异常传播(对照 Uniffle 同名测试) |
| `OverlappingCompressionTest.java` | DataPusher deferred 压缩:数据正确性、completableFuture 异常传播 |
| `CompressorThreadSafetyTest.java` | 多线程并发共享 Compressor/Decompressor,验证无 race condition |
| `DeferredPushTaskTest.java` | rebuildFunction 只执行一次、getData 幂等 |

### 6.2 回归测试

- `./build/mvn test -pl client` — 现有测试全量回归,开/关 overlapping 各跑一遍

### 6.3 Benchmark

| 场景 | 配置 | 指标 |
|------|------|------|
| Spark Terasort 100GB | codec=zstd, level=3 | shuffle write/read 耗时, executor CPU 利用率 |
| Spark GroupBy | codec=lz4 | shuffle read 等待耗时 vs 解压耗时 |
| Spark Join (skewed) | codec=zstd | 内存峰值(背压参数验证) |

**诚实预期**:Celeborn 网络 fetch 已流水线化(maxReqsInFlight=3),读路径收益只来自**解压与下游处理(反序列化/聚合/排序)的重叠**;写路径收益取决于 DataPusher 路径占比与压缩耗时占比。不应照搬 Uniffle 的 50% 宣称值,以自己 benchmark 为准。

---

## 七、风险与注意事项

### 7.1 通用风险

1. **异常路径兼容性**:fetch failure → FetchFailedException → stage rerun,异步路径的异常需逐一对齐现有传播逻辑
2. **内存峰值**:在途 buffer 由背压参数封顶,但 peer 测试需确认上限合理
3. **加密场景**:首版不支持加密批次重叠(回退同步),后续迭代再覆盖
4. **可观测性前置**:在优化落地前应先有耗时统计手段,用数据验证收益

### 7.2 写路径特有风险

1. **batch header 语义**:压缩前置后 batch header 中的 size 字段需使用**原始**长度
2. **CRC 语义**:若压缩前已 computeBatchCRC(原始数据),压缩后校验需适配
3. **DeferredCompressedBlock 内存**:未触达压缩前持有原始字节数组引用,可能增加 GC 压力;须与 DataPusher send 的触发时机联动
4. **Merge 路径互斥**:方案 A 仅覆盖 doPush=true 路径

### 7.3 读路径特有风险(Uniffle 经验教训)

1. **跳过 block 的许可释放**:Uniffle 的 ~9 个 bugfix 主要集中在此 — 即使 block 因 dedup/attempt 过滤被跳过,也必须调用 `decompressionWorker.get()` 释放信号量许可,否则随 chunk 推进而阻塞
2. **partition 切换(moveToNextChunk)**:切换 partition 时必须 `close()` 前 worker,释放所有 pending 许可
3. **attemptId 过滤 & 去重**:异步预解压不应解出将被跳过的 batch;或接受少量浪费(在最坏情况下,所有 batch 被跳过,相当于浪费了一次解压 CPU)
4. **线程膨胀**:必须使用 ShuffleClient 级共享线程池,每个 partition reader 不单独创建线程池

---

## 八、Uniffle vs Celeborn 架构对比总结

### 8.1 写路径

| 维度 | Uniffle | Celeborn(当前) | Celeborn(建议) |
|------|---------|---------------|---------------|
| **Overlapping Compression** | ✅ 已实现 | ❌ 无 | 对齐实现 |
| **实现模式** | DeferredCompressedBlock + CompFut pipe | 同步压缩 | 同 Uniffle |
| **Codec 线程安全** | 单例无状态 | ThreadLocal 有状态 | 改造为无状态 |
| **压缩线程池** | CachedThreadPool(全局) | 无 | FixedThreadPool(全局,可配) |
| **可观测性** | StatisticsCodec P25-P90 | 无 | 需新增 |

### 8.2 读路径

| 维度 | Uniffle | Celeborn(当前) | Celeborn(建议) |
|------|---------|---------------|---------------|
| **Overlapping Decompression** | ✅ 已实现(PR #2602) | ❌ 无 | 对齐实现 |
| **解压粒度** | per BufferSegment | per batch | per batch |
| **线程池** | 每 reader 独立 FixedPool(1线程) | 无 | 全局共享 FixedPool |
| **索引** | (batchIndex, segmentIndex) 二维 | N/A | batchIndex 一维 |
| **背压** | Semaphore(segment 计数) | 无 | Semaphore(字节计数) |
| **统计指标** | 解压/分配/等待/吞吐 | 无 | 对齐 |

### 8.3 关键差异

| 差异 | 说明 |
|------|------|
| **Codec 无状态 vs ThreadLocal** | Uniffle Codec 天然支持多线程, Celeborn 的 Compressor 依赖 ThreadLocal 隔离,是异步化的最大障碍 |
| **batch vs segment 粒度** | Celeborn per-batch 是独立压缩块;Uniffle per-segment 更细。Celeborn 更简单(一维索引) |
| **写路径 Deferred 触发时机** | Uniffle:DataPusher.send() 异步触发;Celeborn 若对齐需在 DataPusher 层注入 |
| **Flutter/MR/Tez** | 两边均以 Spark 为主要覆盖引擎,其他引擎不适用或待验证 |

---

## 九、实施优先级建议

| 阶段 | 内容 | 理由 |
|------|------|------|
| **P0** | ThreadSafety 改造(Compressor + Decompressor checksum/buffer 局部化) | 所有异步化的前置条件,改动最小,可独立合入 |
| **P1** | 读路径方案 A(DecompressionWorker) | 收益明确(Uniffle 已验证),Celeborn 读路径缺解压重叠 |
| **P1** | 可观测性(解压耗时 + 等待耗时统计) | 收益验证的基础,可独立合入 |
| **P2** | 写路径方案 A(Deferred Compression Pipeline) | 收益取决于 DataPusher 路径占比与 codec,需 benchmark 确认 |
| **P3** | 加密场景兼容 | 异步解压路径支持加密批次 |
| **P3** | 压测:调整背压参数(threads/maxInflightBytes)最佳值 | 产出生产推荐配置 |
