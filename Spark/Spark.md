##### Spark 程序必须做的第一件事情是创建一个SparkContext对象，它会告诉 Spark 如何访问集群。

### *RDD：*

&ensp;&ensp;它是一个容错且可以执行并行操作的元素的集合。有两种方法可以创建 RDD : 在你的 driver program（驱动程序）中 parallelizing 一个已存在的集合，或者在外部存储系统中引用一个数据集，例如，一个共享文件系统，HDFS，HBase，或者提供 Hadoop InputFormat 的任何数据源。

1. 在已存在的集合上通过调用 SparkContext 的 parallelize 方法来创建并行集合，该集合的元素从一个可以并行操作的 distributed dataset（分布式数据集）中复制到另一个 dataset（数据集）中去。

2. 可以使用 SparkContext 的 textFile 方法来创建文本文件的 RDD。此方法需要一个文件的 URI（计算机上的本地路径），并且读取它们作为一个 lines（行）的集合。

   - 2-1 SparkContext.wholeTextFiles 可以读取包含多个小文本文件的目录, 并且将它们作为一个 (filename, content) pairs 来返回；

   + 2-2 使用 SparkContext 的 sequenceFile[K, V] 方法，其中 K 和 V 指的是文件中 key 和 values 的类型。



### 广播变量：

&ensp;&ensp;广播变量(groadcast varible)为只读变量，它有运行SparkContext的driver程序创建后发送给参与计算的节点。对那些需要让工作节点高效地访问相同数据的应用场景。

&ensp;&ensp;广播变量也可以被非driver程序所在节点(即worker)访问，访问方法就是调用该变量的value方法

&ensp;&ensp;广播变量的优势：是因为不是每个task一份变量副本，而是变成每个节点的executor才一份副本。这样的话，就可以让变量产生的副本大大减少。

> 广播变量，初始的时候，就在Drvier上有一份副本。task在运行的时候，想要使用广播变量中的数据，此时首先会在自己本地的Executor对应的BlockManager中，尝试获取变量副本；如果本地没有，BlockManager，也许会从远程Driver上面去获取变量副本；也有可能从距离比较近的其它节点的Executor的BlockManager上去获取，并保存在本地的BlockManager中；BlockManager负责管理某个Executor对应的内存和磁盘上的数据，此后这个executor上的task，都会直接使用本地的BlockManager中的副本。

&ensp;&ensp;**Spark中分布式执行的代码需要传递到各个Executor的Task上运行。对于一些只读、固定的数据(比如从DB中读出的数据),每次都需要Driver广播到各个Task上，这样效率低下。广播变量允许将变量只广播（提前广播）给各个Executor。该Executor上的各个Task再从所在节点的BlockManager获取变量，而不是从Driver获取变量，从而提升了效率。**

&ensp;&ensp;一个Executor只需要在第一个Task启动时，获得一份Broadcast数据，之后的Task都从本节点的BlockManager中获取相关数据。



### Spark stage过程

整个 computing chain 根据数据依赖关系自后向前建立，遇到 ShuffleDependency 后形成 stage。在每个
stage 中，每个 RDD 中的 compute() 调用 parentRDD.iter() 来将 parent RDDs 中的 records 一个个 fetch 过来。



### 自定义RDD

如果要自己设计一个 RDD，那么需要注意的是 compute() 只负责定义 parent RDDs => output records 的计算逻辑，具体依赖哪些 parent RDDs 由 getDependency() 定义，具体依赖 parent RDD 中的哪些 partitions 由 dependency.getParents()定义。

```scala
 // RDD x = (RDD a).cartesian(RDD b)
 // 定义 RDD x 应该包含多少个 partition，每个 partition 是什么类型
 override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.size * rdd2.partitions.size)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  // 定义 RDD x 中的每个 partition 怎么计算得到
  override def compute(split: Partition, context: TaskContext) = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    // s1 表示 RDD x 中的 partition 依赖 RDD a 中的 partitions（这里只依赖一个）
    // s2 表示 RDD x 中的 partition 依赖 RDD b 中的 partitions（这里只依赖一个）
    for (x <- rdd1.iterator(currSplit.s1, context);
         y <- rdd2.iterator(currSplit.s2, context)) yield (x, y)
  }

  // 定义 RDD x 中的 partition i 依赖于哪些 RDD 中的哪些 partitions
  //
  // 这里 RDD x 依赖于 RDD a，同时依赖于 RDD b，都是 NarrowDependency
  // 对于第一个依赖，RDD x 中的 partition i 依赖于 RDD a 中的
  //    第 List(i / numPartitionsInRdd2) 个 partition
  // 对于第二个依赖，RDD x 中的 partition i 依赖于 RDD b 中的
  //    第 List(id % numPartitionsInRdd2) 个 partition
  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )
```



### Job的提交过程

以count()为例：

1. *org.apache.spark.rdd.RDD#count*

```scala
def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum
```

2. *org.apache.spark.SparkContext#runJob*

```scala
def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.length)
  }
	/* ... */
def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = getCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
    }
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
  }
```

3. *org.apache.spark.scheduler.DAGScheduler#runJob*

```scala
def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
    waiter.completionFuture.ready(Duration.Inf)(awaitPermission)
    waiter.completionFuture.value.get match {
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case scala.util.Failure(exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }
```

>submitJob首先为Job生成一个Job ID，并且生成一个JobWaiter的实例监听Job的执行情况
>
>Job由多个Task组成，只有所有Task都成功完成，Job才标记为成功。若失败，则通过jobFailed方法处理。

4. *org.apache.spark.scheduler.DAGScheduler#submitJob*

```scala
def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }
```

5. *org.apache.spark.util.EventLoop#post*

```scala
def post(event: E): Unit = {
    eventQueue.put(event)
  }
```

当eventProcessLoop对象投递了JobSubmitted事件之后，对象内的eventThread线程实例对事件进行处理，不断从事件队列中取出事件，调用onReceive函数处理事件，当匹配到JobSubmitted事件后，调用DAGScheduler的handleJobSubmitted函数并传入jobid、rdd等参数来处理Job。

>DAGScheduler::submitJob会创建JobSummitted的event发送给内嵌类eventProcessActor
>
>（在源码1.4中，submitJob函数中，使用DAGSchedulerEventProcessLoop类进行事件的处理）

6. *org.apache.spark.scheduler.DAGScheduler#handleJobSubmitted*

```scala
private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {
	/* ... */
  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)
	/* ... */
```

#### *job 的生成和提交代码*

1. rdd.action() 会调用 `DAGScheduler.runJob(rdd, processPartition, resultHandler)` 来生成 job。
2. runJob() 会首先通过`rdd.getPartitions()`来得到 finalRDD 中应该存在的 partition 的个数和类型：Array[Partition]。然后根据 partition 个数 new 出来将来要持有 result 的数组 `Array[Result](partitions.size)`。
3. 最后调用 DAGScheduler 的`runJob(rdd, cleanedFunc, partitions, allowLocal, resultHandler)`来提交 job。cleanedFunc 是 processParittion 经过闭包清理后的结果，这样可以被序列化后传递给不同节点的 task。
4. DAGScheduler 的 runJob 继续调用`submitJob(rdd, func, partitions, allowLocal, resultHandler)` 来提交 job。
5. submitJob() 首先得到一个 jobId，然后再次包装 func，向 DAGSchedulerEventProcessLoop 发送 JobSubmitted 信息，该 eventProcessLoop 收到信息后进一步调用`dagScheduler.handleJobSubmitted()`来处理提交的 job。之所以这么麻烦，是为了符合事件驱动模型。
6. handleJobSubmmitted() 首先调用 finalStage = newStage() 来划分 stage，然后submitStage(finalStage)。由于 finalStage 可能有 parent stages，实际先提交 parent stages，等到他们执行完，finalStage 需要再次提交执行。再次提交由 handleJobSubmmitted() 最后的 submitWaitingStages() 负责。

#### *newStage() 如何划分 stage*

1. 该方法在 new Stage() 的时候会调用 finalRDD 的 getParentStages()。
2. getParentStages() 从 finalRDD 出发，反向 visit 逻辑执行图，遇到 NarrowDependency 就将依赖的 RDD 加入到 stage，遇到 ShuffleDependency 切开 stage，并递归到 ShuffleDepedency 依赖的 stage。
3. 一个 ShuffleMapStage（不是最后形成 result 的 stage）形成后，会将该 stage 最后一个 RDD 注册到`MapOutputTrackerMaster.registerShuffle(shuffleDep.shuffleId, rdd.partitions.size)`，这一步很重要，因为 shuffle 过程需要 MapOutputTrackerMaster 来指示 ShuffleMapTask 输出数据的位置。

#### *submitStage(stage) 如何提交 stage 和 task*

1. 先确定该 stage 的 missingParentStages，使用`getMissingParentStages(stage)`。如果 parentStages 都可能已经执行过了，那么就为空了。
2. 如果 missingParentStages 不为空，那么先递归提交 missing 的 parent stages，并将自己加入到 waitingStages 里面，等到 parent stages 执行结束后，会触发提交 waitingStages 里面的 stage。
3. 如果 missingParentStages 为空，说明该 stage 可以立即执行，那么就调用`submitMissingTasks(stage, jobId)`来生成和提交具体的 task。如果 stage 是 ShuffleMapStage，那么 new 出来与该 stage 最后一个 RDD 的 partition 数相同的 ShuffleMapTasks。如果 stage 是 ResultStage，那么 new 出来与 stage 最后一个 RDD 的 partition 个数相同的 ResultTasks。一个 stage 里面的 task 组成一个 TaskSet，最后调用`taskScheduler.submitTasks(taskSet)`来提交一整个 taskSet。
4. 这个 taskScheduler 类型是 TaskSchedulerImpl，在 submitTasks() 里面，每一个 taskSet 被包装成 manager: TaskSetMananger，然后交给`schedulableBuilder.addTaskSetManager(manager)`。schedulableBuilder 可以是 FIFOSchedulableBuilder 或者 FairSchedulableBuilder 调度器。submitTasks() 最后一步是通知`backend.reviveOffers()`去执行 task，backend 的类型是 SchedulerBackend。如果在集群上运行，那么这个 backend 类型是 SparkDeploySchedulerBackend。
5. SparkDeploySchedulerBackend 是 CoarseGrainedSchedulerBackend 的子类，`backend.reviveOffers()`其实是向 DriverActor 发送 ReviveOffers 信息。SparkDeploySchedulerBackend 在 start() 的时候，会启动 DriverActor。DriverActor 收到 ReviveOffers 消息后，会调用`launchTasks(scheduler.resourceOffers(Seq(new WorkerOffer(executorId, executorHost(executorId), freeCores(executorId)))))` 来 launch tasks。scheduler 就是 TaskSchedulerImpl。`scheduler.resourceOffers()`从 FIFO 或者 Fair 调度器那里获得排序后的 TaskSetManager，并经过`TaskSchedulerImpl.resourceOffer()`，考虑 locality 等因素来确定 task 的全部信息 TaskDescription。调度细节这里暂不讨论。
6. DriverActor 中的 launchTasks() 将每个 task 序列化，如果序列化大小不超过 Akka 的 akkaFrameSize，那么直接将 task 送到 executor 那里执行`executorActor(task.executorId) ! LaunchTask(new SerializableBuffer(serializedTask))`。

