# Celeborn JMH 基准测试

本目录是 Apache Celeborn 的 JMH 基准测试集合文档。benchmark 与被测代码同模块,放在各模块的
`src/test/java` 下(少数复用 celeborn 已有测试桩)。本套 benchmark 共 19 个(18 个新增 + master 既有
的 `SlotsAllocatorJmhBenchmark`),覆盖 checksum、序列化、网络收发、压缩、client 写路径、worker
拥塞控制/buffer 池/写盘、SSL 等核心热路径。每个 benchmark 针对 celeborn 自己的一条真实热路径;其中
`ResettableSlidingWindowReservoir.size()` 已据 benchmark 结果做了无锁读优化(见"优化点候选清单"#2),
其余优化点经逐个验证后确认无收益或风险过高未改。

## 设计原则

1. **测 celeborn 自己的代码,不测 Kafka 的。** Kafka 的 `jmh-benchmarks` 模块是灵感来源(也是优化 commit 线索),
   但只有测 celeborn 实际发布的类才有意义。与 Kafka 业务强耦合的概念(`MemoryRecords`、`ProducerRecord`、
   KRaft image、consumer group assignor 等)一律不移植。
2. **一个组件一个 benchmark,一个 benchmark 一个 PR。** 体积小、易 review、可独立运行。
3. **复用现有 JMH 配置。** JMH `1.37` 在根 `pom.xml` 的 `dependencyManagement` 声明;各 benchmark 模块声明
   `jmh-core` + `jmh-generator-annprocess` 为 `test` scope 依赖(模板见 `master/pom.xml`)。注解处理器从 test
   classpath 自动发现——无需 `sbt-jmh` 插件、无需额外 profile、无需新模块。
4. **遵循 `SlotsAllocatorJmhBenchmark` 范式。** `@Benchmark` + `@State(Scope.Benchmark)` + `@Setup` + `Blackhole`;
   `@Fork(1)` + `@Warmup` + `@Measurement` + `@BenchmarkMode(AverageTime)`;带一个转发到
   `org.openjdk.jmh.Main` 的 `public static void main`。
5. **与被测类同包**,当需要访问 package-private 成员时(例如 `CelebornCRC32` 的 package-private `compute`/`addData`/`get`)。

## 移植价值总览(哪些值得从 Kafka 移过来)

对 Kafka `jmh-benchmarks` 的 50 个 benchmark,逐个核对 celeborn 是否有等价实现后,只有 **A 类有移植价值并已全部落地**(共 19 个,含 master 既有的 `SlotsAllocatorJmhBenchmark`)。B、C 两类不值,理由如下。

| 类别 | 数量 | 移植价值 | 处理 |
|---|---|---|---|
| **A. celeborn 有等价实现** | 19 | **有** | 已移植/新写(见下方"Benchmark 目录")。benchmark 测 celeborn 自己的类,能驱动优化 |
| **B. Kafka 业务强耦合** | ~38 | 无 | 不移植。celeborn 架构无对应概念 |
| **C. celeborn 无实现的通用工具类** | ~7 | 无(移植=引入新组件,非 benchmark) | 不移植。见下表 |

**A 类明细**(已落地):`CelebornCRC32`、`CelebornHistogram`/`ResettableSlidingWindowReservoir`、`JavaUtils.newConcurrentHashMap`、`Lz4/Zstd Compressor`、`Encoders`、`ManagedBuffer`(`Nio`/`Netty`/`FileSegment`)、`Message` encode/decode、`TransportFrameDecoder`、`SSLFactory`、`EncryptedMessageWithHeader`、`DataPusher`、`WorkerStatusTracker`、`TimeSlidingHub`/`BufferStatusHub`、`CongestionController`、`BufferQueue`、`LocalFlushTask`、`SlotsAllocator`(既有)。其中前 4 个有 Kafka 直接对应(CRC32C/HdrHistogram/CopyOnWriteMap/LZ4 压缩),其余是 celeborn 自有热路径、Kafka 无对应而新写。

**B 类**(不移植,Kafka 特有概念):`record/`(`MemoryRecords` v2 消息格式)、`producer/`(`ProducerRecord`/`RecordAccumulator`)、`fetchsession/`/`fetcher/`(增量 fetch/副本拉取)、`common/*Request*Response*`(Kafka RPC 对象)、`coordinator/`/`assignor/`/`metadata/`(group coordinator/KRaft image/分区分配)、`acl/`/`server/`/`partition/`/`storage/`/`log/`(broker 内部)、`connect/`/`streams/`/`consumer/`/`timeline/`。celeborn 用 credit-based stream + ratis,无这些概念。

**C 类**(不移植,celeborn 无实现,移植要先给 celeborn 引入该组件):

| Kafka 类 | Kafka 用途 | celeborn 现状 | 若要移植 |
|---|---|---|---|
| `LRUCache` | 元数据缓存 | 用 Guava `CacheBuilder` + `ConcurrentHashMap` | 先给 celeborn 引入 LRUCache 类(功能 PR,非 benchmark) |
| `Murmur2` | partition 路由哈希 | partition 哈希由 Spark/MR/Tez 框架的 `Partitioner` 提供,celeborn 不算 | 同上,且 celeborn 无 partition 路由场景 |
| `ByteUtils`(varint) | 协议变长整数编解码 | celeborn 协议层(`Encoders`/`TransportMessage`)全用定长;protobuf 的 varint 在库内部 | 若协议层引入变长编码再谈 |
| `BytesCompare` | byte 字典序比较 | 无 `Comparator<byte[]>` 场景 | 无对应 |
| `CopyOnWriteMap` | 低写高读 COW map | 用 `ConcurrentHashMap` | 同上 |
| `ImplicitLinkedHashCollection` | O(1) 增删+保序集合 | 无 | 无对应 |
| `TimingWheel` | 超时任务时间轮 | 无(celeborn 用 `ScheduledExecutorService`) | 若引入超时任务管理再谈 |

> **结论**:Kafka 50 个 benchmark 里**只有 A 类(19 个)对 celeborn 有移植价值,已全部落地**。B、C 类要么架构无关,要么要先给 celeborn 引入新组件(那是功能 PR,不是 benchmark 贡献)。

## 构建与运行

```bash
# 编译某模块的 test 源码(同时触发 JMH 注解处理器)
./build/mvn -pl <module> -am test-compile -DskipTests

# 通过 main 在进程内运行单个 benchmark(不 fork),快速冒烟
./build/mvn -pl <module> exec:java \
  -Dexec.mainClass=<全限定 benchmark 类名> \
  -Dexec.classpathScope=test \
  -Dexec.args="-f 0 -wi 1 -i 1 <参数覆盖...> <benchmark 方法正则>"

# 真正测量用 fork 模式(-f)——见下方说明
```

> **经 `exec:java` 的 fork 运行:** JMH 在 `-f >= 1` 时会 fork 子 VM,而 `exec:java` 不会把
> `jmh-core` 加到 forked VM 的 classpath,所以 fork 运行会报
> `找不到或无法加载主类 org.openjdk.jmh.runner.ForkedMain`。这同样影响既有的
> `SlotsAllocatorJmhBenchmark`——是"用 `mvn exec` 跑 JMH"的固有摩擦,不是任何 benchmark 的问题。
> 冒烟用 `-f 0`(进程内);要拿真实数字请打 fat jar(`java -jar`)或走 sbt
> (`celeborn-<module>/Test/compile` 后运行 `main`)。

## Benchmark 目录

### common 模块 —— 校验和、指标、序列化、网络

| Benchmark | 测什么 | 关键参数 | 样例结果(`-f 0 -wi 2 -i 3`,64B/4K 等) | 优化线索 |
|---|---|---|---|---|
| `CelebornCRC32JmhBenchmark` | `CelebornCRC32.compute`(单次)vs 流式 `addData`+`addChecksum`+`get` | `chunks`, `bytes` | computePerChunk 0.074 µs,streaming 0.081 µs | `compute` 每次都 new `CRC32`;`addChecksum` 高并发 CAS combine |
| `CelebornHistogramJmhBenchmark` | `ResettableSlidingWindowReservoir` 写/读(`@Group` 3 写 + 1 读) | `reservoirSize` | update 381 ns,readPercentile 21 ns(`-t 1` 近似;group+`-f 0` 数据波动大) | `update`/`getSnapshot` 全 `synchronized`——写路径是瓶颈 |
| `ConcurrentMapJmhBenchmark` | `JavaUtils.newConcurrentHashMap`(CELEBORN-474 JDK8 快路径)vs 原生 `ConcurrentHashMap` vs `HashMap` | `mapSize`, `writePercentage` | celebornComputeIfAbsent 3.7 ns | 热点元数据 map 的选型 |
| `EncodersJmhBenchmark` | `Encoders.Strings/IntArrays/StringArrays` encode/decode | `stringBytes`, `arrayLen` | encodeString 236 ns,encodeStringArray(128) 32.9 µs,decodeStringArray 42.5 µs | 数组路径上每个 String 反复 `getBytes(UTF_8)` 转换+拷贝 |
| `ManagedBufferJmhBenchmark` | `NioManagedBuffer` vs `NettyManagedBuffer`:`convertToNetty`/`size`/`nioByteBuffer`/`retain-release` | `bytes` | nioConvertToNetty 13.0 ns,nettyConvertToNetty 17.5 ns,nettyRetainRelease 9.5 ns | 量化 `wrappedBuffer` vs `duplicate().retain()` 差距 |
| `MessageEncodeDecodeJmhBenchmark` | `PushMergedData` encode/decode | `partitionCount`, `decodeBody` | encode 4.1 µs,decode 10.2 µs(64 partitions) | decode 的 body 包裹(`decodeBody=true`)开销 |
| `TransportFrameDecoderJmhBenchmark` | `TransportFrameDecoder.channelRead` 分帧循环(经 `EmbeddedChannel`) | `framesPerBuffer` | 64 帧 7.4 µs(单次;`-f 0` 多次跑误差大,见下注) | 每帧的 header 累积 / `decodeNext` / `Message.decode` |
| `SSLFactoryJmhBenchmark` | `SSLFactory.createSSLEngine` + `SSLEngine.wrap` TLS 数据路径 | `payloadBytes` | createClientEngine 2.09 µs,wrap 25 ns(4096 B) | 每连接 engine 构造开销;TLS 加密开销 |
| `FileSegmentManagedBufferJmhBenchmark` | `FileSegmentManagedBuffer.convertToNetty`(零拷贝)/`nioByteBuffer`(read vs mmap)/`size` | `length` | convertToNetty 0.238 µs,nioByteBuffer 28.3 µs(4K) | 零拷贝 vs nioByteBuffer 差距;`nioByteBuffer` 每次 `new RandomAccessFile`+open channel |
| `EncryptedMessageWithHeaderJmhBenchmark` | `EncryptedMessageWithHeader.readChunk`(SSL 发送路径,header+body 分块流) | `bodyBytes` | readChunks 618 ns(4096 B) | SSL 流式分块无零拷贝,对比非 SSL 的 `MessageWithHeader.transferTo` |

> **关于样例结果**:均为 `-f 0`(进程内,不 fork;fork 经 `mvn exec:java` 会因 `ForkedMain` 不在 classpath 失败,见"构建与运行")在当前开发机上的单次/短配置测量,用于量级对比,不代表生产数字。含后台线程或 `EmbeddedChannel` 状态的 benchmark(`CelebornHistogramJmhBenchmark`、`TransportFrameDecoderJmhBenchmark`、`DataPusherJmhBenchmark`)在 `-f 0` 下误差偏大,数字仅供量级参考;正式测量建议 fork 模式(`java -jar` fat jar)。

### client 模块 —— 压缩、写路径、worker 状态

| Benchmark | 测什么 | 关键参数 | 样例结果(`-f 0 -wi 2 -i 3`) | 优化线索 |
|---|---|---|---|---|
| `Lz4CompressorJmhBenchmark` | `Lz4Compressor.compress` / `Lz4Decompressor.decompress` | `chunkSize`(64K–4M) | compress 26.1 µs,decompress 16.1 µs(64K) | `checksum.reset()` 复用;压缩比不利时 `initCompressBuffer` 反复扩容 |
| `ZstdCompressorJmhBenchmark` | `ZstdCompressor.compress` / `ZstdDecompressor.decompress` | `chunkSize`, `level` | compress 104.0 µs,decompress 77.8 µs(64K,level 1) | 解压 buffer 分配;zstd-jni level 调优 |
| `DataPusherJmhBenchmark` | `DataPusher.addTask` 写路径入队(pushData override 成 no-op) | `bytes` | addTask ~3–19 µs(4096 B,含后台 pushThread,`-f 0` 误差大) | idle 队列池复用、`arraycopy`、working 队列 offer/drain |
| `WorkerStatusTrackerJmhBenchmark` | `workerAvailable` / `workerExcluded`(每次 push 前都查) | `excludedCount` | miss 6.0 ns,hit 10.3 ns,excluded 10.3 ns(100 excluded) | 规模化时双集合查询开销 |

### worker 模块 —— 拥塞控制、buffer 池

| Benchmark | 测什么 | 关键参数 | 样例结果(`-f 0 -wi 2 -i 3`) | 优化线索 |
|---|---|---|---|---|
| `TimeSlidingHubJmhBenchmark` | `BufferStatusHub`/`TimeSlidingHub` 的 `add` / `avgBytesPerSec`(override 时钟,无 sleep) | `timeWindowSecs`, `bytesPerChunk`, `millisPerAdd` | add 30.7 ns,avgBytesPerSec 12.9 ns | `add`/`sum` 全 `synchronized`;`removeExpiredNodes` 内联在 `add` 里 |
| `BufferQueueJmhBenchmark` | `BufferQueue.poll`/`recycleToLocalPool`/`recycle` round trip | `poolSize`, `bufferSize` | pollAndRecycleLocal 14.0 ns,bufferAvailable 2.0 ns | `poll` 是 `synchronized(buffers)` 而 local recycle 无锁 |
| `CongestionControllerJmhBenchmark` | `CongestionController.isUserCongested` 限流判定 + `getUserCongestionContext`(computeIfAbsent) | `userCount` | isUserCongested 7.1 ns,getContext 15.3 ns(16 users) | 限流判定读 over-high-watermark 标志 + 单用户 produce speed |
| `LocalFlushTaskJmhBenchmark` | `LocalFlushTask.flush` 写盘 FileChannel gather write | `bufferBytes`, `gatherApiEnabled` | gather 11.5 µs / 逐 buffer 22.2 µs(64K) | `gatherApiEnabled` 量化价值(gather write 快 ~1.9×);对应 kafka `TestLinearWriteSpeed` |

### master 模块 —— slot 分配

| Benchmark | 测什么 | 关键参数 |
|---|---|---|
| `SlotsAllocatorJmhBenchmark` *(既有)* | `SlotsAllocator.offerSlotsRoundRobin` | 1500 workers,100k partitions |

## 优化点候选清单

benchmark 结果暴露出 7 个候选优化点。**重要:分配类优化必须用 `-prof gc` 看 `gc.alloc.rate.norm`(每 op 分配字节数)验证,不能用误差大的时间测量**——后者会因测量噪声掩盖 GC 收益(见 #1/#3)。

**有优化收益的(已落地,共 2 项):**

| 优化 | 改动 | 验证(GC profiler) |
|---|---|---|
| `ResettableSlidingWindowReservoir.size()` 无锁读 | `index`/`full` 改 `@volatile`,`size()` 不再 `synchronized`;`getSnapshot` 少一次 monitor enter。`update`/`getSnapshot`/`reset` 保留 `synchronized`(正确性需要) | 独立烟测验证 reset/update/getSnapshot/size 语义不变;test-compile 通过 |
| `Encoders.Strings` 用 netty `writeUtf8` | `encodedLength`/`encode` 用 `ByteBufUtil.utf8Bytes`/`writeUtf8` 替代 `s.getBytes(UTF_8)`,消除每个 String 的中间 byte[] 分配 | `gc.alloc.rate.norm` 330328 → 98920 B/op(**-70%**),`gc.time` 25ms→6ms(-76%);wire 兼容烟测 ASCII/中文/emoji round-trip 全 OK(字节与 `getBytes(UTF_8)` 一致) |

**其余 5 项无收益或不适合改**(详见下表):#1 CRC32 复用(`-prof gc` 证实 alloc.norm≈0,JIT 已消除分配,真无收益)、#7 gather 配置(默认已 `true`,无需改)验证为无收益;#4 TimeSlidingHub、#5 Lz4Compressor、#6 FileSegmentManagedBuffer 风险高(并发原语/压缩正确性/生命周期)暂不改。另用 `-prof gc` 扫了 MessageEncodeDecode/TransportFrameDecoder/ManagedBuffer 的分配,decode 路径的分配是协议语义必需、ManagedBuffer 的分配是 convert 语义必需,无可消除的冗余分配。

**详细验证结果:**

| # | 目标 | 验证结果 |
|---|---|---|
| 1 | `CelebornCRC32.compute` 每次 new `CRC32` → 复用线程局部实例 | **无收益(已用 `-prof gc` 证实)**:`compute` 的 `gc.alloc.rate.norm` ≈ 0.027 B/op、`gc.count` ≈ 0——`new CRC32()` 被 JIT 逃逸分析标量替换消除,没有分配可省,复用 ThreadLocal 反增开销。不改 |
| 2 | `ResettableSlidingWindowReservoir` 全 synchronized | **已优化**:`size()` 改无锁读(volatile 字段),`getSnapshot` 少一次 monitor enter;`update`/`reset` 保留 synchronized(正确性需要)。烟测验证 reset/update/getSnapshot/size 语义不变 |
| 3 | `Encoders.StringArrays.encode` 每 String 重新转 UTF-8 byte[] → 直接写 ByteBuf | **已优化**:`Strings.encodedLength`/`encode` 改用 `ByteBufUtil.utf8Bytes`/`writeUtf8`。`-prof gc` 证实 `gc.alloc.rate.norm` 330328→98920 B/op(**-70%**),`gc.time` 25ms→6ms。wire 兼容烟测通过(字节与 `getBytes(UTF_8)` 一致)。注:之前用误差大的时间测量误判为"无收益" |
| 4 | `TimeSlidingHub.add` synchronized | **不推荐**:并发原语核心,`sum` 内含写副作用 `removeExpiredNodes`,无安全读路径可去锁;全盘无锁化风险高。不改 |
| 5 | `Lz4Compressor.compress` 压缩比不利时 `initCompressBuffer` 反复扩容 | 风险高:压缩格式正确性敏感,需保证与解压侧兼容。暂不改 |
| 6 | `FileSegmentManagedBuffer.nioByteBuffer` 每次 open channel | 生命周期复杂:partition 文件多,ThreadLocal 缓存 channel 不现实。暂不改 |
| 7 | `LocalFlushTask.flush` `gatherApiEnabled` | **无需改**:配置 `celeborn.worker.flusher.local.gatherAPI.enabled` 默认已是 `true`;benchmark 量化 gather 快 ~1.8× 仅作选型佐证 |

> **认知**:celeborn 这些热路径多数已被 JIT/默认配置/库实现良好优化,微基准暴露的"差距"常是物理限制或已优化项(#1 CRC32 即是 JIT 已消除分配)。但仍能用 `-prof gc` 的 `gc.alloc.rate.norm` 挖出被时间测量噪声掩盖的分配收益(#3 Encoders,GC -70%)。19 个 benchmark 里 2 项落地优化(#2 锁、#3 分配),其余需真实负载 profiling。


## Kafka → celeborn 优化 commit 映射

移植的 benchmark 保留了 Kafka 血统,使原始优化意图可追溯。下表是 kafka benchmark → KAFKA-XXXX 优化 → celeborn 对应组件与移植状态。

| Kafka benchmark | Kafka 优化 | celeborn 对应 | 状态 |
|---|---|---|---|
| `Crc32CBenchmark` (KAFKA-13900) | Java 9 direct-buffer CRC32C | `CelebornCRC32` | 已移植 |
| `HistogramBenchmark` (#17221) | HdrHistogram vs Yammer | `ResettableSlidingWindowReservoir` | 已移植 |
| `ConcurrentMapBenchmark` (KAFKA-12708) | CopyOnWriteMap vs CHM | `JavaUtils.newConcurrentHashMap` | 已移植 |
| `RecordBatchIterationBenchmark` (KAFKA-5150/8106/14633) | LZ4 解压/减少拷贝 | `Lz4/Zstd` 压缩器 | 改造适配 |
| —(无 Kafka 对应) | 网络分帧/发送路径 | `TransportFrameDecoder`、`Encoders`、`ManagedBuffer`、`Message` | 新写 |
| —(无 Kafka 对应) | worker 拥塞/buffer 池 | `TimeSlidingHub`、`BufferQueue`、`CongestionController` | 新写 |
| —(无 Kafka 对应) | client 写路径 | `DataPusher`、`WorkerStatusTracker` | 新写 |

## 明确不移植的

不移植的 B 类(Kafka 业务强耦合)与 C 类(celeborn 无实现的通用工具类)清单与理由,见上方"移植价值总览"。那里已逐项说明为何不移植、celeborn 现状由什么满足。
