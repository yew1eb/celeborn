# CI 日志问题记录

> 用途:逐份分析 GitHub Actions CI job 日志,沉淀需要跟进的问题。
> **维护约定**(后续追加新 CI 日志时遵循):
> 1. 在「日志索引」表末尾加一行;
> 2. 新问题加入「问题清单」并标注来源 job;老问题复现则在对应问题下追加证据行;
> 3. 该 job 的预期 ERROR 归类追加到「各 job 附录」;
> 4. 可复用文末「追加模板」。

## 日志索引

| # | Job | 日期 | 行数 | 结论 |
|---|-----|------|------|------|
| 1 | Test with SBT(Spark 4.2) | 2026-10-29 | 29207 | 绿色:124 succeeded / 0 failed / 7 canceled,无 `##[error]` |
| 2 | Test with Maven | 2026-10-29 | 29698 | 绿色:2× `BUILD SUCCESS`,12 组 `Tests run` 全为 0 失败,无 Maven `[ERROR]` |
| 3 | celeborn_integration_test | 2026-10-29 | 11679 | 绿色:3× `BUILD SUCCESS`(行 1992/4607/11654),无 Maven `[ERROR]` 与 `##[error]`;匹配 "ERROR" 字样的 7 行均为 INFO 输出 |
| 4 | celeborn_cpp_integration_test | 2026-10-29 | 5053 | 绿色:10× `BUILD SUCCESS`(行 2497 起,每测试一个),9 个 Java/C++ 混合读写测试全部通过;18 条 ERROR 均为测试退出阶段噪音(P10/P11),另有 ~882 条线程 linger WARNING(→ P11) |

> 前两份日志(SBT/Maven)的共同背景:ERROR 绝大多数为故障注入/混沌测试的预期输出(逐 job 归类见文末附录),真正需要跟进的集中在问题清单;cpp job(第 4 份)的 ERROR 则全部为退出阶段噪音(P10/P11)。

## 问题清单(按优先级,跨 job 合并)

### P1. Netty epoll 接收缓冲泄漏 —— 两个 job 同一泄漏指纹(疑似真 bug)

- **SBT 证据**:日志行 12417(08:30:04),`push-server-1497-2`,ReusedExchangeSuite 执行期间:
  ```
  ERROR [...] ResourceLeakDetector: LEAK: ByteBuf.release() was not called before
  it's garbage-collected.
  Created at: PreferredDirectByteBufAllocator.ioBuffer ← MaxMessageHandle.allocate
              ← AbstractEpollStreamChannel.epollInReady
  ```
  前后用例为 CELEBORN-1177「OpenStream should register stream via ChunkStreamManager」与 CELEBORN-980(异步删除原始文件),泄漏窗口与 **stream 未正确关闭/异步删除** 高度相关。
- **Maven 证据**:日志行 16788(08:44:03),`rpc_app_lifecyclemanager-server-1996-1`,堆栈与 SBT **完全同源**(epoll 收包路径分配,无任何 celeborn 业务帧),同样紧邻 ReusedExchange 用例。
- **定性**:两个不同 CI job 同一指纹且都紧邻 ReusedExchange 用例,不再像偶发噪音;指向 RPC 请求收到但未走完处理/释放路径(连接被粗暴关闭)。
- **跟进**:单独复跑 ReusedExchange 相关 Suite(加 `-Dio.netty.leakDetection.level=paranoid`)确认稳定复现;沿 push server 收包 → stream/ChunkStreamManager 生命周期追 release 路径;发 Jira 时附两份 CI 日志作证据。

### P2. PartitionFilesSorter composite buffer 泄漏(Maven 新发现,疑似真 bug)

- **Maven 证据**:日志行 16274(08:40:55),`rpc_app_client-client-1806-1`,`MemoryStorageReusedExchangeSuite` 附近,worker 侧:
  ```
  ERROR [...] ResourceLeakDetector: LEAK: ByteBuf.release() was not called before
  it's garbage-collected.
  Created at:
    io.netty.buffer.AbstractByteBufAllocator.compositeBuffer(...)
    org.apache.celeborn.service.deploy.worker.storage.PartitionFilesSorter.getSortedFileInfo(PartitionFilesSorter.java:223)
  ```
- **定性**:与 P1 不同 —— 堆栈直接指向 **worker 存储层业务代码**,说明 `PartitionFilesSorter` 排序文件信息时创建的 composite buffer 存在一条漏调 `release()` 的分支(大概率在排序异常 / stream 提前关闭 / 异步删除路径上)。
- **跟进**:读 `PartitionFilesSorter.java:223` 附近的分配与释放分支确认遗漏点;可作为社区 PR 候选。

### P3. S3 e2e 测试在 CI 恒 CANCELED ×7(SBT,已知问题)

- **现象**:7 个取消用例全部 S3 相关:`only s3` / `s3` / `memory evict to s3` / `memory evict to s3 after partition split` / `memory does not evict to s3` / `push fails no way of evicting` / `only memory`。
- **定性**:S3 e2e 在 CI 从未真正执行。已在 [celeborn-ci-spark42-analysis.md](celeborn-ci-spark42-analysis.md) P4 实锤(正确性问题:坏了也不会被发现)。
- **跟进**:先发 JIRA / dev@ 邮件列表讨论(补 S3 凭据/限定触发条件,或明确移出 CI)。

### P4. GitHub Actions 弃用 warning ×2(SBT,CI 维护)

- `setup-java v4 is deprecated` → 迁移到 `actions/setup-java@v5`(日志行 155)。
- Node.js 20 已弃用:`actions/cache@v4`、`actions/checkout@v4`、`actions/setup-java@v4` 被强制跑在 Node 24 上(日志行 29207)。
- **celeborn_integration_test 复现**:日志行 11679(09:00:18),同一 Node 20 弃用告警,涉及的 action 增加了 `actions/setup-python@v5`:
  ```
  ##[warning]Node.js 20 is deprecated. The following actions target Node.js 20
  but are being forced to run on Node.js 24: actions/checkout@v4,
  actions/setup-java@v4, actions/setup-python@v5.
  ```
- **celeborn_cpp_integration_test 复现**:日志行 5053(全 job 最后一行),同一 Node 20 弃用告警,涉及 `actions/checkout@v4`、`actions/setup-java@v4`。
- **定性**:三个 job 均出现,是仓库级 workflow 维护项,非单 job 问题。
- **跟进**:改 workflow 版本号(各 action 升到 v5/v6),纯维护项。

### P5. WorkerStatusTracker ERROR 消息空首行(两 job 均现,日志质量)

- **现象**(SBT ×14,Maven ×14):
  ```
  ERROR [...] WorkerStatusTracker:
  Reporting failed workers:
  Host:... RESERVE_SLOTS_FAILED ...
  ```
  消息以换行开头,ERROR 级别 + 空首行,可读性差。出现在 ChangePartitionManagerUpdateWorkersSuite、LifecycleManagerSuite 等测试中。
- **定性**:log4j 消息格式问题(`log.error("\n" + ...)`)。
- **跟进**:改成单行格式,可作为社区小 PR。

### P6. Jersey WADL WARNING ×39(SBT,日志噪声)

- **现象**:每次 master/worker 起 HTTP 服务打一条:
  ```
  WARNING: JAXBContext implementation could not be found. WADL feature is disabled.
  ```
  整个 SBT CI 共 39 条,是该 job 唯一一类 WARNING。
- **定性**:REST API 用不到 WADL,纯噪声。
- **跟进**:禁用 Jersey `WadlFeature` 或补 JAXB 依赖,消噪声。

### P7. 编译/构建 WARNING(Maven,卫生项)

- ~70 条 Maven pom 告警:`'artifactId'`/`'version'`/`'build.plugins.plugin.version'` 缺失(老式 pom 写法 / 插件未固定版本)。
- **celeborn_integration_test 复现**:同批 pom 告警在此 job 重复出现(每个 profile 构建一组,约 3×;另新增 `build-helper-maven-plugin` 未固定版本 @ openapi-client/pom.xml:173)。
- **celeborn_cpp_integration_test 复现**:同批 pom 告警再次全量出现(每次 `exec:java` 前的 reactor 构建一组,约 240 条,行 928-964 集中段),同一指纹,不新增问题。
- 代码级告警(可用 `./build/mvn spotless:check` + 编译告警清理一并处理):
  - `SparkCryptoHandlerSuiteJ.java` 5 处(deprecated API 使用,行 58/70/79/94/107)
  - `CelebornPartitionUtilSuiteJ.java:206,39`
  - `SparkUtils.java:483,8`
- shade 插件 duplicate JAR 提示(netty-*、commons-io、RoaringBitmap 等)。

### P8. MemoryManager `ServingState` switch 缺 `NONE_PAUSED` 分支(celeborn_integration_test,主代码缺陷候选)

- **celeborn_integration_test 证据**:日志行 1552 与 10108(08:44:36 / 08:56:32,两个构建 profile 各一次):
  ```
  [WARNING] worker/src/main/java/org/apache/celeborn/service/deploy/worker/memory/MemoryManager.java:[613,4]
  [MissingCasesInEnumSwitch] Non-exhaustive switch; either add a default or
  handle the remaining cases: NONE_PAUSED
  ```
- **定性**:**主源码** errorprone 告警(非测试代码),本地已核实:`MemoryManager.resumeByPinnedMemory(ServingState)` 的 switch 只处理 `PUSH_AND_REPLICATE_PAUSED` / `PUSH_PAUSED`。当前 `NONE_PAUSED` 语义上无事可做,不构成运行时 bug,但属于"新增枚举值易漏处理"的隐患模式。
- **跟进**:补空 case `case NONE_PAUSED: break;`(或 `default`),2 行改动,可作社区小 PR 候选。

### P9. 测试代码 ErrorProne 告警(celeborn_integration_test,测试质量项)

- **celeborn_integration_test 证据**(每个 profile 重复 1 组,~3×):
  - `ShuffleClientSuiteJ.java` 行 703/739/775/906 `[MissingFail]`:期望抛异常的 try-catch 未调 `fail()`(日志行 1134/1137 等,javac 同步给出 `assertThrows` 改法建议):
    ```
    [WARNING] client/src/test/java/org/apache/celeborn/client/ShuffleClientSuiteJ.java:[906,6]
    [MissingFail] Not calling fail() when expecting an exception masks bugs
    ```
  - `[DefaultCharset]` 大量(`getBytes()` 隐式平台编码):`CommitMetadataTest`(~15 处)、`CelebornCRC32Test`(~11 处)、`RocksDBRecoverySuiteJ`(~4 处)、`CelebornInputStreamCryptoRoundTripSuiteJ`(~4 处)
  - `MemoryReducePartitionDataWriterSuiteJ.java:85` `[UnusedMethod]` 私有方法从未使用;行 627 `[UseCorrectAssertInTests]` 用了 Java `assert`
  - `CommitMetadataTest.java:110` `[NarrowingCompoundAssignment]` int→byte 复合赋值
- **定性**:测试健壮性/可移植性问题,不影响 CI 绿色;`MissingFail` 有掩盖真回归的风险,优先级最高。
- **跟进**:批量改 `assertThrows` / `getBytes(UTF_8)` / 删无用方法,可拆成多个社区 PR。

### P10. `LifecycleManager.onStop` 在 dispatcher 线程内自等待 `awaitTermination`(celeborn_cpp_integration_test,主代码缺陷候选)

- **触发场景**:`.github/workflows/cpp_integration.yml` 用 `exec:java` 在同 JVM 跑 9 个 Java/C++ 混合读写测试,每个测试的 `finally` 中 `lifecycleManager.stop()`(`worker/src/test/.../JavaCppHybridReadWriteTestBase.scala:163/281/394`)触发该 ERROR,9 个测试各 1 条、堆栈完全一致:
  - 行 2645(JavaWriteCppReadTestWithNONE)/ 2927(LZ4)/ 3203(ZSTD)/ 3480(CppWriteJavaRead NONE)/ 3757(LZ4)/ 4034(ZSTD)/ 4311(CppMergeWrite NONE)/ 4588(LZ4)/ 4865(ZSTD)。
- **完整堆栈**(首个,08:08:53.847,`celeborn-dispatcher-14`):
  ```
  ERROR [celeborn-dispatcher-14] Inbox: An error happened while processing message
  in the inbox for LifecycleManagerEndpoint
  java.lang.InterruptedException
    at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.reportInterruptAfterWait(AbstractQueuedSynchronizer.java:2019)
    at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.awaitNanos(AbstractQueuedSynchronizer.java:2093)
    at java.util.concurrent.ThreadPoolExecutor.awaitTermination(ThreadPoolExecutor.java:1475)
    at org.apache.celeborn.common.rpc.netty.Dispatcher.awaitTermination(Dispatcher.scala:217)
    at org.apache.celeborn.common.rpc.netty.NettyRpcEnv.awaitTermination(NettyRpcEnv.scala:331)
    at org.apache.celeborn.client.LifecycleManager.onStop(LifecycleManager.scala:330)
    at org.apache.celeborn.common.rpc.netty.Inbox.processInternal(Inbox.scala:173)
    at org.apache.celeborn.common.rpc.netty.Inbox.$anonfun$process$1(Inbox.scala:230)
    at org.apache.celeborn.common.rpc.netty.Inbox.safelyCall(Inbox.scala:332)
    at org.apache.celeborn.common.rpc.netty.Inbox.process(Inbox.scala:230)
    at org.apache.celeborn.common.rpc.netty.Dispatcher$MessageLoop.run(Dispatcher.scala:268)
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    at java.lang.Thread.run(Thread.java:750)
  ```
- **时间线**(首个测试,断言通过后):
  | 时间 | 事件 |
  |---|---|
  | 08:08:48.629 | C++ reader 结果校验通过(`partition 0/1 sum result`,行 2640-2642),测试逻辑完成 |
  | 08:08:53.847 | 本 ERROR(+5.2s,与 `MiniClusterFeature.shutdownMiniCluster()` 中 `Thread.sleep(5000)` 后的 interrupt 阶段时序吻合,`MiniClusterFeature.scala:298-302`) |
- **根因(本地 ai-1 分支已核对,行号与 CI 逻辑一致但偏移 ~28 行,CI 为 main)**:
  `OnStop` 由 dispatcher 消息循环线程处理(`Inbox.processInternal:173 → onStop`),而 `LifecycleManager.onStop()` 调 `rpcEnv.shutdown()` 后**同步** `rpcEnv.awaitTermination()`(`client/src/main/scala/.../LifecycleManager.scala:302-303`)→ `NettyRpcEnv.awaitTermination`(`NettyRpcEnv.scala:330`)→ `Dispatcher.awaitTermination()`(`common/.../netty/Dispatcher.scala:216-217`)等 `Long.MaxValue` 让线程池终止 —— **当前线程本身就是该线程池成员**,正常路径永远无法返回(自等待),只能靠外部 interrupt 逃逸,于是 interrupt 被 `Inbox.safelyCall` 记为 ERROR。
- **定性**:测试结果不受影响(CI 全绿),但该自等待在生产路径同样是潜在卡点:endpoint 停止过程会挂起直到某个无关线程碰巧 interrupt 它,且每次都产生一条 ERROR 级误报。
- **修复建议(社区 PR 候选)**:
  1. `LifecycleManager.onStop` 去掉同步 `awaitTermination`(由调用方 `LifecycleManager.stop()` 在 dispatcher 线程之外等待),或
  2. 最低限度:catch `InterruptedException` 并 `Thread.currentThread().interrupt()`,降级为 debug 日志,消除误报。
- **复现**:
  ```bash
  MAVEN_OPTS="-XX:MaxDirectMemorySize=2G" build/mvn -pl worker test-compile exec:java \
    -Dexec.classpathScope="test" \
    -Dexec.mainClass="org.apache.celeborn.service.deploy.cluster.JavaWriteCppReadTestWithNONE"
  ```
  (CI 环境:Zulu JDK 8.0.512-8 / x64,Ubuntu 22.04;ERROR 出现在测试断言通过后的 teardown 阶段)

### P11. exec:java 退出阶段 shutdown hook 失败 + 大量线程 linger(celeborn_cpp_integration_test,测试框架/清理缺陷)

- **证据 A —— `ShutdownHookManager` NoClassDefFoundError,9 条**(与 P10 的 9 条一一相隔 ~15s;首个为行 2797,08:09:08.841,`shutdown-hook-thread`):
  ```
  ERROR [shutdown-hook-thread] ThreadExceptionHandler: Uncaught exception in executor
  service shutdown-hook-thread, thread Thread[shutdown-hook-thread,5,
  org.apache.celeborn.service.deploy.cluster.JavaWriteCppReadTestWithNONE]
  java.lang.NoClassDefFoundError: org/apache/celeborn/common/util/ShutdownHookManager$2
    at org.apache.celeborn.common.util.ShutdownHookManager.getShutdownHooksInOrder(ShutdownHookManager.java:229)
    at org.apache.celeborn.common.util.ShutdownHookManager.executeShutdown(ShutdownHookManager.java:104)
    at org.apache.celeborn.common.util.ShutdownHookManager$1.run(ShutdownHookManager.java:79)
    at java.lang.Thread.run(Thread.java:750)
  Caused by: java.lang.ClassNotFoundException: org.apache.celeborn.common.util.ShutdownHookManager$2
    at org.codehaus.mojo.exec.URLClassLoaderBuilder$ExecJavaClassLoader.loadClass(URLClassLoaderBuilder.java:211)
    at java.lang.ClassLoader.loadClass(ClassLoader.java:352)
    ... 4 more
  ```
  同类次序:行 2797/3079/3355/3632/3909/4186/4463/4740/5017(对应 9 个测试类)。
- **证据 B —— 线程 linger,873 条 "will linger despite being asked to die via interruption" + 9 条 "was interrupted but is still alive after waiting at least ~15000msecs"**。首个样本(行 2661,08:09:08.817,`JavaWriteCppReadTestWithNONE`):
  ```
  [WARNING] thread Thread[worker-metrics-cleaner,5,
  org.apache.celeborn.service.deploy.cluster.JavaWriteCppReadTestWithNONE] was interrupted
  but is still alive after waiting at least 14971msecs
  ```
  即每个测试 JVM 退出前被 exec:java 的线程收割逻辑卡 ~15s。插件自身的量化结论(行 2759-2760,每个测试各 1 组,共 9 组):
  ```
  [WARNING] NOTE: 97 thread(s) did not finish despite being asked to via interruption.
  This is not a problem with exec:java, it is a problem with the running code.
  Although not serious, it should be remedied.
  [WARNING] Couldn't destroy threadgroup org.codehaus.mojo.exec.AbstractExecJavaBase$IsolatedThreadGroup[name=org.apache.celeborn.service.deploy.cluster.JavaWriteCppReadTestWithNONE,maxpri=10]
  ```
  —— 每个测试遗留 **97 个未退出线程**,插件明确指出"问题在运行代码,应当修复"。泄漏线程统计(全 job):
  | 线程 | 次数 | 未关闭原因 |
  |---|---|---|
  | `LocalFlusher@<workingDir>@hash-/-N`(N=0..9,按盘位编号) | 432(48/测试) | Worker 每个 workingDir×盘位一个 flusher 线程,`shutdownMiniCluster` 的 `stop(EXIT_IMMEDIATELY)` 未优雅停掉全部 flusher |
  | `worker-metrics-cleaner` | 153(17/测试) | `AbstractSource.scala:67` 每个 metrics source 一个 `ScheduledExecutorService`,仅 `destroy()`(`AbstractSource.scala:691`) 才 `shutdown()`;interrupt 无法杀死 parked 的调度线程 |
  | `worker-timer` / `worker-expired-shuffle-cleaner` | 各 27(3/测试) | Worker 生命周期组件未完全关闭 |
  | `master-quota-checker` / `master-partition-size-updater` / `master-rack-resolver-refresher` | 各 9~27 | Master 侧同类问题 |
  | `celeborn-push-timeout-checker-0..3` / `celeborn-fetch-timeout-checker-0..1` / `transport-messages-runner` 等 | 各 9 | 客户端/worker 驻留线程 |
- **时间线**(首个测试,与 P10 接续):
  | 时间 | 事件 |
  |---|---|
  | 08:08:53.847 | P10 的 Inbox ERROR(teardown 开始) |
  | 08:09:08.81x | exec:java 线程收割:上述 linger WARNING 集中出现 |
  | 08:09:08.841 | 证据 A 的 shutdown-hook ERROR(hook 执行失败) |
  | 08:09:08.84x | `BUILD SUCCESS`(行 2792),进入下一个测试;**~15s 全部耗在退出清理上** |
- **根因**:
  - 证据 A 为 `exec:java`(同 JVM 运行测试主类)的已知缺陷:exec-maven-plugin 的 `ExecJavaClassLoader` 在 JVM 退出钩子阶段无法再加载匿名类 `ShutdownHookManager$2`,导致 `ShutdownHookManager.executeShutdown` 整体失败、**其注册的所有 shutdown hook 均未执行**,进一步放大证据 B。
  - 证据 B 主因在 `MiniClusterFeature.shutdownMiniCluster()`(`worker/src/test/.../MiniClusterFeature.scala:285-309`):仅 `worker.stop(EXIT_IMMEDIATELY)` + `rpcEnv.shutdown()` + `Thread.sleep(5000)` 后粗暴 `thread.interrupt()`,而 interrupt 对上表定时/驻留线程无效;且 `AbstractSource.destroy()` 并未在全部 source 上被调用。
- **定性**:测试全绿、无功能影响;但每测试浪费 ~15s,**9 个测试累计 ~2.5 分钟 CI 时长**,且 873+9=882 条 WARNING 淹没有效信号。
- **修复建议(按性价比排序)**:
  1. Worker/Master 的 `stop()` 路径对全部 metrics source 调 `destroy()`(单消 `worker-metrics-cleaner` 153 条,代码改动小);
  2. 测试改 `exec:exec`(fork 独立 JVM,退出即回收,证据 A/B 一并消失;断言需改走退出码/结果文件);
  3. `shutdownMiniCluster` 补齐各组件(LocalFlusher/timeout-checker 等)的关闭调用。
- **复现**:同 P10 命令(9 个测试类逐一执行);本地无需 cpp 二进制也可观察退出阶段——linger WARNING 与 hook ERROR 由 MiniCluster teardown 产生。

## 各 job 附录:预期内的 ERROR(无需处理)

### Test with SBT(2026-10-29)

| 模式 | 次数 | 来源 |
|------|------|------|
| `ReducePartitionCommitHandler: CommitFiles ... Timeout/MOCK_FAILURE, will retry` | ~620 | CommitFiles 重试的故障注入测试 |
| `ShuffleClientImpl: Push data ... failed, remain revive times N` / `Exception raised while pushing data` | ~610 | Revive / 切 replica 混沌测试(杀 worker) |
| `MasterClient: Send rpc with failure` / `Worker: Fail report to master` | ~240 | 同上 |
| `WorkerPartitionReader: Fetch chunk failed` + `CelebornInputStream: CommitMetadata mismatch` | ~120 | CelebornIntegrityCheckSuite 的损坏数据测试(断言的就是数据不一致) |
| `TransportResponseHandler: Still have N requests outstanding when connection is closed` | 73 | worker 被杀时在途请求,teardown 噪声 |
| `LifecycleManager: DestroyWorkerSlots ... RpcEnvStoppedException` | ~10 | LifecycleManagerDestroySlotsSuite 停机竞态测试 |
| `Utils: Uncaught exception in thread task-result-getter-1` (InterruptedException) | 1 | IntegrityCheck 负向测试 abort 任务时 Spark 侧噪声 |

### Test with Maven(2026-10-29)

| logger | 次数 | 来源 |
|------|------|------|
| `ReducePartitionCommitHandler` | 618 | CommitFiles 重试的故障注入测试(与 SBT 同类) |
| `ShuffleClientImpl` | 615 | Revive / 杀 worker 混沌测试 |
| `MasterClient` / `Worker` | ~230 | shutdown 期间 `RpcEnvStoppedException`(Worker.exitImmediately 等正常停机路径) |
| `WorkerPartitionReader` + `TransportResponseHandler` + `CelebornInputStream` | ~120 | IntegrityCheck 损坏数据测试(`Expected exception, logging the full exception`)+ fetch 失败注入 |
| `WorkerStatusTracker` / `Master` | 16 | `all workers are excluded` 等 worker 排除逻辑测试路径 |
| `CelebornIntegrityCheckSuite` / `TaskSetManager` | 3 | 负向测试预期 abort |

### celeborn_integration_test(2026-10-29)

无任何预期内 ERROR(本 job 为集成测试构建 job,全部 ERROR 字样为 INFO 输出)。535 条 WARNING 按来源归类:

| WARNING 模式 | 次数 | 来源 |
|------|------|------|
| `Plugin is modifying testCompileSourceRoots through add()` | 38 | Maven 4 前向兼容提示,等上游插件更新 |
| pom effective-model 告警(`'artifactId'/'version' contains an expression` 等) | ~180 | Scala 多版本拼名的惯用写法(→ P7) |
| ErrorProne 代码级告警(DefaultCharset/MissingFail/UnusedMethod 等) | ~250 | 见 P8/P9 |
| shade overlapping classes / `Discovered module-info.class` | ~15 | shade 合包常规噪音 |
| `unknown enum constant Level.FULL`(j2objc ReflectionSupport) | 6 | 可选注解依赖缺失,无实际影响 |
| JMH `Could not determine source for class ...jmh_generated` | ~20 | JMH 生成代码正常提示 |
| `JAR will be empty` / `Platform.java Unsafe is internal proprietary API` | 3/15 | 空模块预期 / sun.misc.Unsafe 已知告警 |
| Node.js 20 deprecated(`##[warning]`) | 1 | workflow 维护项(→ P4) |

### celeborn_cpp_integration_test(2026-10-29)

job 流程:`build/mvn clean install -DskipTests`(行 507,10 个模块全 SUCCESS)→ 编译 cpp 客户端 → 依次 `exec:java` 跑 9 个混合读写测试(Java 写/C++ 读 ×3 压缩算法、C++ 写/Java 读 ×3、C++ merge 写/Java 读 ×3),全部通过,每步一个 `BUILD SUCCESS`(行 2497/2792/3074/3350/3627/3904/4181/4458/4735/5012)。

本 job 的 18 条 ERROR **均为退出阶段噪音**(非预期内业务错误,已列入问题清单 P10/P11):

| 模式 | 次数 | 来源 |
|------|------|------|
| `Inbox: An error happened ... for LifecycleManagerEndpoint` + `InterruptedException` | 9(每测试 1) | P10:`LifecycleManager.onStop` 在 dispatcher 线程内自等待 `awaitTermination` |
| `ThreadExceptionHandler: ... NoClassDefFoundError: ShutdownHookManager$2` | 9(每测试 1) | P11:exec:java 的 ClassLoader 在 JVM 退出钩子阶段失效 |

1368 条 WARNING 按来源归类:

| WARNING 模式 | 次数 | 来源 |
|------|------|------|
| `thread ... will linger despite being asked to die via interruption` | 873 | P11:MiniCluster/Worker/Master 线程未优雅关闭 |
| `thread ... was interrupted but is still alive after waiting at least ~15000msecs` | 9(每测试 1) | P11:每测试退出卡 ~15s |
| pom effective-model 告警(`'artifactId'/'version' contains an expression` 等) | ~240 | 每步 reactor 构建重复一组(→ P7) |
| javac 测试代码告警(`common/worker/client/master src/test/java ...` 路径警告) | ~55 | deprecated API 等,与 P9 同类 |
| 空白 `[WARNING]` 分隔行 | 120 | Maven effective-model 输出格式(`It is highly recommended ...` 段落分隔) |
| `NOTE: N thread(s) did not finish ... via interrupt` / `Couldn't destroy threadgroup` | 18 | exec:java 退出清理(→ P11 佐证:每测试 97 个线程未退出) |
| `Plugin is modifying testCompileSourceRoots through add()` | ~20 | Maven 4 前向兼容提示,等上游插件更新 |
| `Discovered module-info.class. Shading ...` / `JAR will be empty` / `Unsafe is internal proprietary API` | ~30 | shade 合包 / 空模块 / sun.misc.Unsafe 已知告警 |
| `Could not determine source for class ...` (JMH) | 9 | JMH 生成代码正常提示 |
| `unknown enum constant Level.FULL` | 3 | 可选注解依赖缺失,无实际影响 |
| Node.js 20 deprecated(`##[warning]`) | 1(行 5053) | workflow 维护项(→ P4) |

## 追加模板(新 CI 日志分析后复制使用)

```markdown
## 日志索引(加一行)
| N | <Job 名> | <日期> | <行数> | <绿色/红色 + 一句话结论> |

### P<编号>. <新问题标题>(<Job 名>,定性)
- **<Job 名> 证据**:日志行 <行号>(<时间>),<线程>,<上下文>:
  ```
  <关键日志摘录>
  ```
- **定性**:<是预期输出还是疑似真 bug;与已有 P 几号的关系>
- **跟进**:<复现步骤 / Jira / PR 候选>

## 各 job 附录(新加一节)
### <Job 名>(<日期>)
| logger/模式 | 次数 | 来源 |
```
