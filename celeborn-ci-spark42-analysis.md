# Celeborn CI(Spark 4.2)日志分析报告 — 可提交社区的优化点

> 分析对象:`~/Downloads/job-logs.txt` — apache/celeborn GitHub Actions `spark4` matrix job
> (Spark 4.2.0 + Scala 2.13 + Zulu JDK 17,ubuntu-22.04,2026-10-19)
> 结合本地代码库 `celeborn-main`(main 分支,626a07cfc)逐项核实到源码行级。
> 交付形式:纯分析报告,不含代码改动。所有提案均可独立拆成 CELEBORN-xxxx JIRA/PR。

---

## 1. CI 时间线分解

Job 总时长 **约 44 分钟**(05:16:59 → 06:01:15),构成如下:

| 阶段 | 时间区间 | 耗时 | 占比 |
|---|---|---|---|
| checkout + setup-java | 05:17:00–05:17:14 | 15s | <1% |
| curl 下载 apache-maven-3.9.16-bin.tar.gz | 05:17:16–05:19:05 | **1m49s** | 4% |
| `mvn clean install -DskipTests`(15 模块,串行) | 05:19:05–05:25:38 | 6m32s | 15% |
| `mvn test`(spark-it 119 用例 + client-spark 单测) | 05:25:38–06:01:14 | **35m33s** | 81% |

单条 mvn 命令的真实开销容易被低估:本次日志中 Maven **发行包本身**从 `archive.apache.org`
下载就花了 1 分 49 秒(见 §4 P3),而 spark4 矩阵共 3 个 job(spark 4.0/4.1/4.2),
全仓库 maven.yml 共约 20+ 个 job 都在重复这笔开销。

### 1.1 spark-it 各 Suite 耗时 Top(ScalaTest,119 用例 / 22 个 suite)

| Suite | 耗时 | 行数跨度(日志噪声) |
|---|---|---|
| CelebornFetchFailureSuite | **6m27s** | L6530–12716(~6.2k 行) |
| ChangePartitionManagerUpdateWorkersSuite | 3m44s | L3819–4612 |
| LifecycleManagerCommitFilesSuite | 2m48s | L12716–14731 |
| SparkUtilsSuite | 2m26s | L16645–18279 |
| CelebornFetchFailureDiskCleanSuite | 2m21s | L28097–29935 |
| CelebornHashCheckDiskSuite | 2m13s | L18279–27730(~9.5k 行!) |
| 其余 16 个 suite | 均 < 2 min | — |

### 1.2 单用例耗时 Top

| 用例 | 耗时 | 慢因(见 §3) |
|---|---|---|
| serialize/deserialize GetReducerFileGroupResponse with broadcast(SparkUtilsSuite) | 137.6s | suite 尾部用例,背了整个集群 bootstrap + 前序 SparkSession 的账 |
| fallback with workers unavailable(ShuffleFallbackSuite) | 136.5s | 集群起停内联进测试体 + 固定 sleep |
| test commit files with mocking failure(LifecycleManagerCommitFilesSuite) | 110.6s | 10s×多轮的 commit 重试退避 |
| test map end & get reducer file group(ChangePartitionManager… 首用例) | 79.0s | suite 首用例含集群启动 |
| Fetch Failure with multiple shuffle data(CelebornFetchFailureSuite) | 70.9s | 每用例独立 SparkSession 冷启动 + 真实 shuffle |

---

## 2. 日志噪声量化

这是本次日志最显眼的问题——**45,492 行日志中 25,863 行(57%)是 Java 异常堆栈的 `at ...` 行**,且绝大多数不指向任何真实故障:

| 噪声源 | 次数 | 来源(源码已核实) | 性质 |
|---|---|---|---|
| `MasterClient: Send rpc with failure` + `ApplicationHeartbeater: AskSync HeartbeatFromApplication failed` | **591 × 2 条**(每次各带 ~13 行堆栈,合计 ~15,000 行) | master 停止后泄漏的 heartbeater 线程仍在发心跳 | 测试资源泄漏 |
| `Worker: Fail report to master` | 107 次 | worker shutdown hook 在 master 已停后仍发 `WorkerLost` RPC | 测试 teardown 顺序问题 |
| `ReducePartitionCommitHandler: Ask worker…commitFiles` 失败 | ~600 条 | LifecycleManagerCommitFilesSuite 的 **mock 故意失败**,属测试预期 | 预期行为按 ERROR 全栈打印 |
| push 失败 / pusher 异常等 | ~百条级 | FetchFailure / StageRerun 类用例的预期失败路径 | 同上 |

后两类与真实缺陷无关但按 ERROR 级全栈打印,在 CI 日志里会造成"满屏 ERROR"的误导
(与已合入的 [CELEBORN-2462] `1fe959a32` 治理 decommission 期间误导性 WARN/ERROR 是同一类问题,但发生在客户端与测试侧)。

---

## 3. 热点慢因(源码级)

### 3.1 MiniClusterFeature:每个 suite 保底 ~20s 的固定 sleep

`worker/src/test/scala/org/apache/celeborn/service/deploy/MiniClusterFeature.scala`:

- **L123** `createMaster` 末尾无条件 `Thread.sleep(5000L)` —— 而 `setUpMaster`(L177-184)随后又会轮询等待 master 就绪,这个 5 秒纯属重复付费;
- **L242** `setUpWorkers` 在 worker 线程 start 后固定 `Thread.sleep(5000)`,**workerNum=0 时也照睡**(ShuffleFallbackSuite 直接受害,见 3.3);
- **L276** 轮询重试粒度也是 5s;
- **L303** `shutdownMiniCluster` 固定 `Thread.sleep(5000)`,且 **L304-308 对同一批 worker 做第二次 `worker.stop(EXIT_IMMEDIATELY)`** —— 此时 master 已在 L299 停止,worker 的 `WorkerLost` RPC 必然失败,正是 107 次 `Fail report to master` ERROR 的直接触发点。

每个触集群的 suite 保底浪费 ~20s(createMaster 5s + setUpWorkers 5s + shutdown 5s + 轮询粒度损耗),22 个 suite × spark3/spark4 两套矩阵,单次 CI 累计浪费**数分钟量级**。而且 `worker.registered`/`worker.initialized` 标志早已存在(L257-261 的检查就在用),条件等待改造是顺手的事。

### 3.2 LifecycleManager 泄漏 → 591×2 条 heartbeater ERROR

`tests/spark-it/src/test/scala/org/apache/celeborn/tests/client/LifecycleManagerSuite.scala`:

- **L111-127**(CELEBORN-1258 用例)`new LifecycleManager(APP, celebornConf)` 之后**从不调用 `stop()`**(对比 L85、L108 的两个用例都显式 stop 了);
- 泄漏的 manager 持有 `ApplicationHeartbeater` 单线程定时器(默认 10s 间隔,`client/src/main/scala/org/apache/celeborn/client/ApplicationHeartbeater.scala:59-131`),在 `afterAll` 停掉 master 之后持续向已死的 master 发心跳;
- 每次失败 = 2 条 ERROR 全栈:`MasterClient.java:167`(`sendMessageInner` 耗尽重试)+ `ApplicationHeartbeater.scala:142`(`requestHeartbeat` 的 catch),≈591 对,贡献了全日志约 1/3 的行数。

结构性诱因:`client/src/test/scala/org/apache/celeborn/client/WithShuffleClientSuite.scala:57-65` 的
`afterEach` 只 stop trait 字段 `lifecycleManager`(在这些用例里是 null),而各测试用**局部 `val lifecycleManager` 遮蔽**了该字段,谁忘了 stop 就泄漏。

### 3.3 个别 suite 把集群起停塞进了测试体

`tests/spark-it/src/test/scala/org/apache/celeborn/tests/spark/ShuffleFallbackSuite.scala:32-38` 把
`beforeAll/afterAll` 重写为空,改为在测试体内 `setupMiniClusterWithRandomPorts(workerNum = 0)` /
`shutdownMiniCluster()`——于是 ~20-30s 的集群成本全部被 scalatest 记到 "fallback with workers unavailable"
这个用例头上(136.5s),且 workerNum=0 仍要为 `setUpWorkers` L242 的固定 5s 买单。

### 3.4 其余慢因速记

- **CelebornFetchFailureSuite(6m27s)**:9 个用例 × 每用例新建并 stop 一个 SparkSession(local[2,3])+ 真实 shuffle 作业 + 2 处 `Thread.sleep(3000)`(L270/L367,barrier stage 重投前);无 eventually 滥用,主要是"每用例一套 Spark 上下文"的固有成本,优化空间有限;
- **LifecycleManagerCommitFilesSuite mock failure(110.6s)**:client 端 commit 重试默认 `retryWait=10s`(`CelebornConf.CLIENT_COMMIT_FILE_REQUEST_RETRY_WAIT`,CelebornConf.scala:5655-5670)× 多轮,测试 conf 里完全可以调小;
- **LifecycleManagerUnregisterShuffleSuite**:检查间隔已改成 5s,但 `eventually(timeout(120.seconds))`(L78/L112)的窗口没跟着收紧;
- **SparkUtilsSuite 尾部用例 137.6s**:用例本体无 sleep,是 scalatest 把 suite 级集群 bootstrap(L16645 前)+ 前序用例遗留 job 线程 + `System.gc()` 的账记在了最后一个用例上——单看"用例耗时"会误判,需结合 suite 级时间看。

---

## 4. 优化提案(按 价值/工作量 排序)

### P1【测试提速|收益最大】MiniClusterFeature 固定 sleep → 条件等待
- **问题**:§3.1 的三处 `Thread.sleep(5000)` + shutdown 双重 stop;
- **改动思路**:删除 `createMaster` L123 冗余 sleep(setUpMaster 已轮询);`setUpWorkers` 用 `registered/initialized` 标志条件轮询(粒度降到 200-500ms);`workerNum=0` 直接短路;shutdown 去掉第二次 worker stop(或至少调换顺序避免对已停 master 发 RPC);
- **预期收益**:每个触集群 suite 省 15-20s;本次日志 22 个 suite ≈ **省 5-7 分钟/次 CI**,spark3/spark4 矩阵同步受益;
- **附带收益**:消除 107 次 `Fail report to master` ERROR;
- **验证**:`build/mvn -Pspark-4.2 -pl tests/spark-it -am test` 前后对比 suite 耗时与 ERROR 行数。

### P2【日志噪声|改动最小】修复 LifecycleManager 泄漏
- **问题**:§3.2,CELEBORN-1258 用例不 stop manager → 591×2 条 ERROR 堆栈(~15k 行,占日志 1/3);
- **改动思路**:最小改法是该用例补 `lifecycleManager.stop()`;更稳的改法是 `WithShuffleClientSuite.afterEach` 兜底(如把局部 manager 注册到 trait 字段或一个待清理集合);
- **预期收益**:spark-it 测试日志量下降 ~30%,ERROR 输出全部指向真实断言失败,显著降低 flaky 排障成本;
- **验证**:跑 LifecycleManagerSuite,确认日志中 `AskSync HeartbeatFromApplication failed` 归零。

### P3【CI 提速|最简单】缓存 Maven 本体
- **问题**:`build/mvn`(L62-65 curl、L85 下载后 `rm -rf` tarball)在**每个 job** 都从 `archive.apache.org` 重新下载 Maven 3.9.16,本次耗时 1m49s(网络差时更久;典型值 ~25s);tarball 用后即删,无法复用;
- **依据**:`~/.m2` 已由 `setup-java cache: maven` 覆盖(maven.yml L139),但 Maven 二进制不在其中;**对等先例**:cb086f2ca [CELEBORN-2432] 已给 sbt.yml 加了 sbt launcher + coursier 缓存,Maven 侧是缺口;
- **改动思路**:`actions/cache@v4` 缓存 `build/apache-maven-3.9.16/` 目录,key 含 pom 的 `maven.version`(pom.xml L68);
- **预期收益**:~20 个 job × 25s~2min,**每次 CI 总节省 10-40 分钟** 累计机器时间;
- **验证**:推分支跑 Actions,确认日志中不再出现 curl 进度条。

### P4【正确性】S3 e2e 测试在 CI 从未真正执行
- **问题**:spark4 job(maven.yml L146)传了 `-Paws`,注释还写着 "enable AWS profile to run end-to-end tests with S3 storage",但:
  - `hadoop-aws`(提供 `S3AFileSystem`)只通过 `master/pom.xml:212-228` 的 `aws` profile 注入,且该 profile **无 `<activation>`**;
  - 测试阶段命令(maven.yml L149)**没有 `-am`**,master 不在 reactor 内、从本地仓库解析,而 Maven 命令行 `-P` 不作用于 reactor 外的模块;
  - 结果 `SparkTestBase.scala:43` 的 `isClassPresent("org.apache.hadoop.fs.s3a.S3AFileSystem")` 恒为 false,7 个 s3 用例**每次都 CANCELED**(本地 surefire 报告同样 `canceled 7`);
- **这与 b659439ed [CELEBORN-2254] 引入 S3 集成测试时的声明意图相悖** —— S3 分层存储 e2e 在 CI 上从未被覆盖过;
- **改动思路**(二选一):把 hadoop-aws 以 test scope 挂到 `tests/spark-it` 自身;或给 master 的 aws profile 加 activation。若社区不愿跑,则应从 PROFILES 里去掉 `-Paws` 消除误导;
- **验证**:改后跑 spark-it,确认 s3 用例从 CANCELED 变为执行(需要 MinIO testcontainer)。

### P5【次级候选】
| 项 | 说明 | 预期 |
|---|---|---|
| Maven 并行构建 | install 阶段加 `-T 1C`(当前 6:32 全串行);测试阶段保持串行避免 flaky | 构建阶段或省 1-2min |
| ShuffleFallbackSuite | 集群起停挪回 beforeAll/afterAll(§3.3) | 用例归因更真实,省内联 20s |
| UnregisterShuffleSuite | 120s eventually 窗口随 5s 检查间隔收紧 | 超时更早暴露 |
| CommitFilesSuite | 测试 conf 调小 `CLIENT_COMMIT_FILE_REQUEST_RETRY_WAIT` | mock failure 用例从 110s 降数十秒 |
| spark4 job 含 spark-3 模块单测 | TEST_MODULES(L147)显式列 `client-spark/spark-3`,其单测在 spark3/spark4 两套矩阵都跑(如 SparkShuffleManagerSuite 78.7s)——因 spark-4-shaded 就是打包 spark-3 客户端,跑是有意义的版本覆盖,是否精简需社区讨论 | 待讨论 |

### 不建议提案
- pom 中 `artifactId/version contains an expression` WARNING(每次构建 ~281 条):Scala 交叉构建(`${scala.binary.version}`)的通行做法,社区大概率 wontfix。

---

## 5. 提案优先级建议

若按"一个 PR 一件事"拆分提交社区:

1. **P2**(几十行测试改动,消 1/3 日志噪声)—— 最适合作为首个贡献,风险趋零;
2. **P3**(workflow + 10 行,有 cb086f2ca 先例背书)—— 审议阻力最小;
3. **P1**(测试基建改造,收益最大但需细致验证不引入 flaky);
4. **P4**(正确性问题,建议先发 JIRA/邮件列表讨论方案再动代码);
5. P5 各项可附在对应 JIRA 的 follow-up 里。

---

*报告依据:job-logs.txt 时间戳逐行统计 + celeborn-main 本地源码核实(所有 文件:行号 均已 Read 确认)。*
