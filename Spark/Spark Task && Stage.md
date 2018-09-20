## Task 如何执行来生成最后的 result

整个 computing chain 根据数据依赖关系自后向前建立，遇到 ShuffleDependency 后形成 stage。在每个
stage 中，每个 RDD 中的 compute() 调用 parentRDD.iter() 来将 parent RDDs 中的 records 一个个 fetch 过来。

> 代码实现:每个 RDD 包含的 getDependency() 负责确立 RDD 的数据依赖，compute() 方法负责接收 parent RDDs
> 或者 data block 流入的 records，进行计算，然后输出 record。经常可以在 RDD 中看到这样的代
> 码 `firstParent[T].iterator(split,context).map(f)`。firstParent 表示该 RDD 依赖的第一个 parent RDD，iterator()表示 parentRDD 中的 records 是一个一个流入该 RDD 的，map(f) 表示每流入一个 recod 就对其进行 f(record) 操作，输出 record。为了统一接口，这段 compute() 仍然返回一个 iterator，来迭代 map(f) 输出的 records。

## Job的提交过程

以count()为例：

1. *org.apache.spark.rdd.RDD#count*

```scala
def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum
```

1. *org.apache.spark.SparkContext#runJob*

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

1. *org.apache.spark.scheduler.DAGScheduler#runJob*

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

> submitJob首先为Job生成一个Job ID，并且生成一个JobWaiter的实例监听Job的执行情况
>
> Job由多个Task组成，只有所有Task都成功完成，Job才标记为成功。若失败，则通过jobFailed方法处理。

1. *org.apache.spark.scheduler.DAGScheduler#submitJob*

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

1. *org.apache.spark.util.EventLoop#post*

```scala
def post(event: E): Unit = {
    eventQueue.put(event)
  }
```

当eventProcessLoop对象投递了JobSubmitted事件之后，对象内的eventThread线程实例对事件进行处理，不断从事件队列中取出事件，调用onReceive函数处理事件，当匹配到JobSubmitted事件后，调用DAGScheduler的handleJobSubmitted函数并传入jobid、rdd等参数来处理Job。

> DAGScheduler::submitJob会创建JobSummitted的event发送给内嵌类eventProcessActor
>
> （在源码1.4中，submitJob函数中，使用DAGSchedulerEventProcessLoop类进行事件的处理）

1. *org.apache.spark.scheduler.DAGScheduler#handleJobSubmitted*

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

## Job提交

#### Driver 端的逻辑如果⽤用代码表⽰:

```scala
finalRDD.action()
=> sc.runJob()

// generate job, stages and tasks
=> dagScheduler.runJob()	//SparkContext%runJob
=> dagScheduler.submitJob()	//SparkContext%submitJob
//DAGScheduler
=> runJob.submitJob -> eventProcessLoop
=> DAGSchedulerEventProcessLoop.JobSubmitted()
=> dagScheduler.handleJobSubmitted()	//handleJobSubmitted
=> finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
=> getOrCreateParentStages(rdd, jobId) -> createShuffleMapStage(shuffleDep, firstJobId)
=> mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
=> dagScheduler.submitStage()	//handleJobSubmitted
=>   missingStages = dagScheduler.getMissingParentStages()	//submitStage
=> dagScheduler.subMissingTasks(readyStage)	//submitStage

// add tasks to the taskScheduler
=> taskScheduler.submitTasks(new TaskSet(tasks))
=> fifoSchedulableBuilder.addTaskSetManager(taskSet)

// send tasks
=> sparkDeploySchedulerBackend.reviveOffers()
=> driverActor ! ReviveOffers
=> sparkDeploySchedulerBackend.makeOffers()
=> sparkDeploySchedulerBackend.launchTasks()
=> foreach task
      CoarseGrainedExecutorBackend(executorId) ! LaunchTask(serializedTask)
```

1. 当⽤用户的 program 调⽤用 val sc = new SparkContext(sparkConf) 时，这个语句会帮助 program 启动诸多有关 driver 通信、job 执⾏行的对象、线程、actor等，该语句确⽴立了 program 的 driver 地位。 

2. Driver program 中的 transformation() 建⽴立 computing chain(⼀一系列的 RDD)，每个 RDD 的 compute() 定义数据来了怎么计算得到该 RDD 中 partition 的结果，getDependencies() 定义 RDD 之间 partition 的数据依赖。

3. 每个 action() 触发⽣生成⼀一个 job，在 dagScheduler.runJob() 的时候进⾏行 stage 划分，在 submitStage() 的时候⽣生成该stage 包含的具体的 ShuffleMapTasks 或者 ResultTasks，然后将 tasks 打包成 TaskSet 交给 taskScheduler，如果taskSet 可以运⾏行就将 tasks 交给 sparkDeploySchedulerBackend 去分配执⾏行。

4. sparkDeploySchedulerBackend 接收到 taskSet 后，会通过⾃自带的 DriverActor 将 serialized tasks 发送到调度器指定的 worker node 上的 CoarseGrainedExecutorBackend Actor上。 

5. Worker 端接收到 tasks 后，执⾏行如下操作 

   ```scala
   coarseGrainedExecutorBackend ! LaunchTask(serializedTask)
    => executor.launchTask()
    => executor.threadPool.execute(new TaskRunner(taskId, serializedTask)) 
   ```

   **executor 将 task 包装成 taskRunner，并从线程池中抽取出一个空闲线程运⾏行 task。一个 CoarseGrainedExecutorBackend 进程有且仅有一个 executor 对象。 **

## Task运行

Executor 收到 serialized 的 task 后，先 deserialize 出正常的 task，然后运行 task 得到其执行结果 directResult，这个结果要送回到 driver 那里。但是通过 Actor 发送的数据包不宜过大，**如果 result 比较大（比如 groupByKey 的 result）先把 result 存放到本地的“内存＋磁盘”上，由 blockManager 来管理，只把存储位置信息（indirectResult）发送给 driver**，driver 需要实际的 result 的时候，会通过 HTTP 去 fetch。如果 result 不大（小于`spark.akka.frameSize = 10MB`），那么直接发送给 driver。

上面的描述还有一些细节：如果 task 运行结束生成的 directResult >  akka.frameSize，directResult 会被存放到由 blockManager 管理的本地“内存＋磁盘”上。**BlockManager 中的 memoryStore 开辟了一个 LinkedHashMap 来存储要存放到本地内存的数据。**LinkedHashMap 存储的数据总大小不超过 `Runtime.getRuntime.maxMemory * spark.storage.memoryFraction(default 0.6)` 。如果 LinkedHashMap 剩余空间不足以存放新来的数据，就将数据交给 diskStore 存放到磁盘上，但前提是该数据的 storageLevel 中包含“磁盘”。

```scala
In TaskRunner.run()
// deserialize task, run it and then send the result to 
=> coarseGrainedExecutorBackend.statusUpdate()
=> task = ser.deserialize(serializedTask)
=> value = task.run(taskId)
=> directResult = new DirectTaskResult(ser.serialize(value))
=> if( directResult.size() > akkaFrameSize() ) 
       indirectResult = blockManager.putBytes(taskId, directResult, MEMORY+DISK+SER)
   else
       return directResult
=> coarseGrainedExecutorBackend.statusUpdate(result)
=> driver ! StatusUpdate(executorId, taskId, result)
```
ShuffleMapTask 和 ResultTask 生成的 result 不一样。**ShuffleMapTask 生成的是 MapStatus**，MapStatus 包含两项内容：一是该 task 所在的 BlockManager 的 BlockManagerId（实际是 executorId + host, port, nettyPort），二是 task 输出的每个 FileSegment 大小。**ResultTask 生成的 result 的是 func 在 partition 上的执行结果。**比如 count() 的 func 就是统计 partition 中 records 的个数。由于 ShuffleMapTask 需要将 FileSegment 写入磁盘，因此需要输出流 writers，这些 writers 是由 blockManger 里面的 shuffleBlockManager 产生和控制的。

```scala
In task.run(taskId)
// if the task is ShuffleMapTask
=> shuffleMapTask.runTask(context)
=> shuffleWriterGroup = shuffleBlockManager.forMapTask(shuffleId, partitionId, numOutputSplits)
=> shuffleWriterGroup.writers(bucketId).write(rdd.iterator(split, context))
=> return MapStatus(blockManager.blockManagerId, Array[compressedSize(fileSegment)])

//If the task is ResultTask
=> return func(context, rdd.iterator(split, context))
```

Driver 收到 task 的执行结果 result 后会进行一系列的操作：首先告诉 taskScheduler 这个 task 已经执行完，然后去分析 result。由于 result 可能是 indirectResult，需要先调用 blockManager.getRemoteBytes() 去 fech 实际的 result，这个过程下节会详解。得到实际的 result 后，需要分情况分析，**如果是 ResultTask 的 result，那么可以使用 ResultHandler 对 result 进行 driver 端的计算（比如 count() 会对所有 ResultTask 的 result 作 sum）**，如果 result 是 ShuffleMapTask 的 MapStatus，那么需要将 MapStatus（ShuffleMapTask 输出的 FileSegment 的位置和大小信息）**存放到 mapOutputTrackerMaster 中的 mapStatuses 数据结构中以便以后 reducer shuffle 的时候查询**。如果 driver 收到的 task 是该 stage 中的最后一个 task，那么可以 submit 下一个 stage，如果该 stage 已经是最后一个 stage，那么告诉 dagScheduler job 已经完成。


```scala
After driver receives StatusUpdate(result)
=> taskScheduler.statusUpdate(taskId, state, result.value)
=> taskResultGetter.enqueueSuccessfulTask(taskSet, tid, result)
=> if result is IndirectResult
      serializedTaskResult = blockManager.getRemoteBytes(IndirectResult.blockId)
=> scheduler.handleSuccessfulTask(taskSetManager, tid, result)
=> taskSetManager.handleSuccessfulTask(tid, taskResult)
=> dagScheduler.taskEnded(result.value, result.accumUpdates)
=> dagSchedulerEventProcessActor ! CompletionEvent(result, accumUpdates)
=> dagScheduler.handleTaskCompletion(completion)
=> Accumulators.add(event.accumUpdates)

// If the finished task is ResultTask
=> if (job.numFinished == job.numPartitions) 
      listenerBus.post(SparkListenerJobEnd(job.jobId, JobSucceeded))
=> job.listener.taskSucceeded(outputId, result)
=>    jobWaiter.taskSucceeded(index, result)
=>    resultHandler(index, result)

// if the finished task is ShuffleMapTask
=> stage.addOutputLoc(smt.partitionId, status)
=> if (all tasks in current stage have finished)
      mapOutputTrackerMaster.registerMapOutputs(shuffleId, Array[MapStatus])
      mapStatuses.put(shuffleId, Array[MapStatus]() ++ statuses)
=> submitStage(stage)
```

## Shuffle Read

### *reducer 怎么知道要去哪里 fetch 数据？*

reducer 首先要知道 parent stage 中 ShuffleMapTask 输出的 FileSegments 在哪个节点。**这个信息在 ShuffleMapTask 完成时已经送到了 driver 的 mapOutputTrackerMaster，并存放到了 mapStatuses: HashMap<stageId, Array[MapStatus]> 里面**，给定 stageId，可以获取该  stage 中 ShuffleMapTasks 生成的 FileSegments 信息 Array[MapStatus]，通过 Array(taskId) 就可以得到某个 task 输出的 FileSegments 位置（blockManagerId）及每个 FileSegment 大小。

当 reducer 需要 fetch 输入数据的时候，会首先调用 blockStoreShuffleFetcher 去获取输入数据（FileSegments）的位置。blockStoreShuffleFetcher 通过调用本地的 MapOutputTrackerWorker 去完成这个任务，MapOutputTrackerWorker 使用 mapOutputTrackerMasterActorRef 来与 mapOutputTrackerMasterActor 通信获取 MapStatus 信息。blockStoreShuffleFetcher 对获取到的 MapStatus 信息进行加工，提取出该 reducer 应该去哪些节点上获取哪些 FileSegment 的信息，这个信息存放在 blocksByAddress 里面。之后，**blockStoreShuffleFetcher 将获取 FileSegment 数据的任务交给 basicBlockFetcherIterator。**

```scala
rdd.iterator()
=> rdd(e.g., ShuffledRDD/CoGroupedRDD).compute()
=> SparkEnv.get.shuffleFetcher.fetch(shuffledId, split.index, context, ser)
=> blockStoreShuffleFetcher.fetch(shuffleId, reduceId, context, serializer)
=> statuses = MapOutputTrackerWorker.getServerStatuses(shuffleId, reduceId)

=> blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = compute(statuses)
=> basicBlockFetcherIterator = blockManager.getMultiple(blocksByAddress, serializer)
=> itr = basicBlockFetcherIterator.flatMap(unpackBlock)
```

basicBlockFetcherIterator 收到获取数据的任务后，会生成一个个 fetchRequest，**每个 fetchRequest 包含去某个节点获取若干个 FileSegments 的任务。**图中展示了 reducer-2 需要从三个 worker node 上获取所需的白色 FileSegment (FS)。总的数据获取任务由 blocksByAddress 表示，要从第一个 node 获取 4 个，从第二个 node 获取 3 个，从第三个 node 获取 4 个。

为了加快任务获取过程，显然要将总任务划分为子任务（fetchRequest），然后为每个任务分配一个线程去 fetch。Spark 为每个 reducer 启动 5 个并行 fetch 的线程（Hadoop 也是默认启动 5 个）。由于 fetch 来的数据会先被放到内存作缓冲，因此一次 fetch 的数据不能太多，Spark 设定不能超过 `spark.reducer.maxMbInFlight＝48MB`。**注意这 48MB 的空间是由这 5 个 fetch 线程共享的**，因此在划分子任务时，尽量使得 fetchRequest 不超过`48MB / 5 = 9.6MB`。如图在 node 1 中，Size(FS0-2) + Size(FS1-2) < 9.6MB 但是 Size(FS0-2) + Size(FS1-2) + Size(FS2-2) > 9.6MB，因此要在 t1-r2 和 t2-r2 处断开，所以图中有两个 fetchRequest 都是要去 node 1 fetch。**那么会不会有 fetchRequest 超过 9.6MB？**当然会有，如果某个 FileSegment 特别大，仍然需要一次性将这个 FileSegment fetch 过来。另外，如果 reducer 需要的某些 FileSegment 就在本节点上，那么直接进行 local read。最后，将 fetch 来的 FileSegment 进行 deserialize，将里面的 records 以 iterator 的形式提供给 rdd.compute()，整个 shuffle read 结束。

```scala
In basicBlockFetcherIterator:

// generate the fetch requests
=> basicBlockFetcherIterator.initialize()
=> remoteRequests = splitLocalRemoteBlocks()
=> fetchRequests ++= Utils.randomize(remoteRequests)

// fetch remote blocks
=> sendRequest(fetchRequests.dequeue()) until Size(fetchRequests) > maxBytesInFlight
=> blockManager.connectionManager.sendMessageReliably(cmId, 
	   blockMessageArray.toBufferMessage)
=> fetchResults.put(new FetchResult(blockId, sizeMap(blockId)))
=> dataDeserialize(blockId, blockMessage.getData, serializer)

// fetch local blocks
=> getLocalBlocks() 
=> fetchResults.put(new FetchResult(id, 0, () => iter))
```

### *reducer 如何将 fetchRequest 信息发送到目标节点？目标节点如何处理 fetchRequest 信息，如何读取 FileSegment 并回送给 reducer？*

rdd.iterator() 碰到 ShuffleDependency 时会调用 BasicBlockFetcherIterator 去获取 FileSegments。BasicBlockFetcherIterator 使用 blockManager 中的 connectionManager 将 fetchRequest 发送给其他节点的 connectionManager。connectionManager 之间使用 NIO 模式通信。其他节点，比如 worker node 2 上的 connectionManager 收到消息后，会交给 blockManagerWorker 处理，blockManagerWorker 使用 blockManager 中的 diskStore 去本地磁盘上读取 fetchRequest 要求的 FileSegments，然后仍然通过 connectionManager 将 FileSegments 发送回去。如果使用了 FileConsolidation，diskStore 还需要 shuffleBlockManager 来提供 blockId 所在的具体位置。如果 FileSegment 不超过 `spark.storage.memoryMapThreshold=8KB` ，那么 diskStore 在读取 FileSegment 的时候会直接将 FileSegment 放到内存中，否则，会使用 RandomAccessFile 中 FileChannel 的内存映射方法来读取 FileSegment（这样可以将大的 FileSegment 加载到内存）。

当 BasicBlockFetcherIterator 收到其他节点返回的 serialized FileSegments 后会将其放到 fetchResults: Queue 里面，并进行 deserialization，所以 **fetchResults: Queue 就相当于在 Shuffle details 那一章提到的 softBuffer。**如果 BasicBlockFetcherIterator 所需的某些 FileSegments 就在本地，会通过 diskStore 直接从本地文件读取，并放到 fetchResults 里面。最后 reducer 一边从 FileSegment 中边读取 records 一边处理。

```scala
After the blockManager receives the fetch request

=> connectionManager.receiveMessage(bufferMessage)
=> handleMessage(connectionManagerId, message, connection)

// invoke blockManagerWorker to read the block (FileSegment)
=> blockManagerWorker.onBlockMessageReceive()
=> blockManagerWorker.processBlockMessage(blockMessage)
=> buffer = blockManager.getLocalBytes(blockId)
=> buffer = diskStore.getBytes(blockId)
=> fileSegment = diskManager.getBlockLocation(blockId)
=> shuffleManager.getBlockLocation()
=> if(fileSegment < minMemoryMapBytes)
     buffer = ByteBuffer.allocate(fileSegment)
   else
     channel.map(MapMode.READ_ONLY, segment.offset, segment.length)
```

每个 reducer 都持有一个 BasicBlockFetcherIterator，一个 BasicBlockFetcherIterator 理论上可以持有 48MB 的 fetchResults。每当 fetchResults 中有一个 FileSegment 被读取完，就会一下子去 fetch 很多个 FileSegment，直到 48MB 被填满。

```scala
BasicBlockFetcherIterator.next()
=> result = results.task()
=> while (!fetchRequests.isEmpty &&
        (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
        sendRequest(fetchRequests.dequeue())
      }
=> result.deserialize()
```

## Cache

作为区别于 Hadoop 的⼀一个重要 feature，cache 机制保证了需要访问重复数据的应⽤用(如迭代型算法和交互式应⽤用)可以运⾏行的更快。与 Hadoop MapReduce job 不同的是 Spark 的逻辑/物理执⾏行图可能很庞⼤大，task 中 computing chain 可能会很⻓长，计算某些 RDD 也可能会很耗时。这时，如果 task 中途运⾏行出错，那么 task 的整个 computing chain 需要重算，代价太⾼高。因此，有必要将计算代价较⼤大的 RDD checkpoint ⼀一下，这样，当下游 RDD 计算出错时，可以直接从checkpoint 过的 RDD 那⾥里读取数据继续算。

### *driver program 设定 rdd.cache() 后，系统怎么对 RDD 进⾏行 cache?*

先不看实现，⾃自⼰己来想象⼀一下如何完成 cache:当 task 计算得到 RDD 的某个 partition 的第⼀一个 record 后，就去判断该 RDD 是否要被 cache，如果要被 cache 的话，将这个 record 及后续计算的到的 records 直接丢给本地 blockManager 的 memoryStore，如果 memoryStore 存不下就交给 diskStore 存放到磁盘。 

实际实现与设想的基本类似，区别在于:将要计算 RDD partition 的时候(⽽而不是已经计算得到第⼀一个 record 的时候)就 去判断 partition 要不要被 cache。如果要被 cache 的话，先将 partition 计算出来，然后 cache 到内存。cache 只使⽤用 memory，写磁盘的话那就叫 checkpoint 了。 

调⽤用 rdd.cache() 后， rdd 就变成 persistRDD 了，其 StorageLevel 为 MEMORY_ONLY。persistRDD 会告知 driver 说⾃自 ⼰己是需要被 persist 的。 

如果用代码表示：

```scala
rdd.iterator()
=> SparkEnv.get.cacheManager.getOrCompute(thisRDD, split, context, storageLevel)
=> key = RDDBlockId(rdd.id, split.index)
=> blockManager.get(key)
=> computedValues = rdd.computeOrReadCheckpoint(split, context)
      if (isCheckpointed) firstParent[T].iterator(split, context) 
      else compute(split, context)
=> elements = new ArrayBuffer[Any]
=> elements ++= computedValues
=> updatedBlocks = blockManager.put(key, elements, tellMaster = true) 
```

当 rdd.iterator() 被调用的时候，也就是要计算该 rdd 中某个 partition 的时候，会先去 cacheManager 那里领取一个 blockId，表明是要存哪个 RDD 的哪个 partition，这个 blockId 类型是 RDDBlockId（memoryStore 里面可能还存放有 task 的 result 等数据，因此 blockId 的类型是用来区分不同的数据）。然后去 blockManager 里面查看该 partition 是不是已经被 checkpoint 了，如果是，表明以前运行过该 task，那就不用计算该 partition 了，直接从 checkpoint 中读取该 partition 的所有 records 放到叫做 elements 的 ArrayBuffer 里面。如果没有被 checkpoint 过，先将 partition 计算出来，然后将其所有 records 放到 elements 里面。最后将 elements 交给 blockManager 进行 cache。

blockManager 将 elements（也就是 partition） 存放到 memoryStore 管理的 LinkedHashMap[BlockId, Entry] 里面。如果 partition 大于 memoryStore 的存储极限（默认是 60% 的 heap），那么直接返回说存不下。如果剩余空间也许能放下，会先 drop 掉一些早先被 cached 的 RDD 的 partition，为新来的 partition 腾地方，如果腾出的地方够，就把新来的 partition 放到 LinkedHashMap 里面，腾不出就返回说存不下。注意 drop 的时候不会去 drop 与新来的 partition 同属于一个 RDD 的 partition。drop 的时候先 drop 最早被 cache 的 partition。（说好的 LRU 替换算法呢？）

### *cacheRDD怎么被读取？*

下次计算(⼀一般是同⼀一 application 的下⼀一个 job 计算)时如果⽤用到 cached RDD，task 会直接去 blockManager 的memoryStore 中读取。具体地讲，当要计算某个 rdd 中的 partition 时候(通过调⽤用 rdd.iterator())会先去 blockManager⾥里⾯面查找是否已经被 cache 了，如果 partition 被 cache 在本地，就直接使⽤用 `blockManager.getLocal()` 去本地memoryStore ⾥里读取。如果该 partition 被其他节点上 blockManager cache 了，会通过 `blockManager.getRemote()` 去其他节点上读取。

**获取 cached partitions 的存储位置：**partition 被 cache 后所在节点上的 blockManager 会通知 driver 上的 blockMangerMasterActor 说某 rdd 的 partition 已经被我 cache 了，这个信息会存储在 blockMangerMasterActor 的 blockLocations: HashMap中。等到 task 执行需要 cached rdd 的时候，会调用 blockManagerMaster 的 getLocations(blockId) 去询问某 partition 的存储位置，这个询问信息会发到 driver 那里，driver 查询 blockLocations 获得位置信息并将信息送回。

**读取其他节点上的 cached partition：**task 得到 cached partition 的位置信息后，将 GetBlock(blockId) 的请求通过 connectionManager 发送到目标节点。目标节点收到请求后从本地 blockManager 那里的 memoryStore 读取 cached partition，最后发送回来。

## CheckPoint

#### *哪些 RDD 需要 checkpoint？*

运算时间很长或运算量太大才能得到的 RDD，computing chain 过长或依赖其他 RDD 很多的 RDD。

实际上，将 ShuffleMapTask 的输出结果存放到本地磁盘也算是 checkpoint，只不过这个 checkpoint 的主要目的是去 partition 输出数据。

#### *什么时候 checkpoint？*

cache 机制是每计算出一个要 cache 的 partition 就直接将其 cache 到内存了。但 checkpoint 没有使用这种第一次计算得到就存储的方法，而是等到 job 结束后另外启动专门的 job 去完成 checkpoint 。**也就是说需要 checkpoint 的 RDD 会被计算两次。因此，在使用 rdd.checkpoint() 的时候，建议加上 rdd.cache()，**这样第二次运行的 job 就不用再去计算该 rdd 了，直接读取 cache 写磁盘。其实 Spark 提供了 rdd.persist(StorageLevel.DISK_ONLY) 这样的方法，相当于 cache 到磁盘上，这样可以做到 rdd 第一次被计算得到时就存储到磁盘上，但这个 persist 和 checkpoint 有很多不同，之后会讨论。

#### *checkpoint 怎么实现？*

RDD 需要经过 [ Initialized --> marked for checkpointing --> checkpointing in progress --> checkpointed ] 这几个阶段才能被 checkpoint。

**Initialized：** 首先 driver program 需要使用 rdd.checkpoint() 去设定哪些 rdd 需要 checkpoint，设定后，该 rdd 就接受 RDDCheckpointData 管理。用户还要设定 checkpoint 的存储路径，一般在 HDFS 上。

**marked for checkpointing：** 初始化后，RDDCheckpointData 会将 rdd 标记为 MarkedForCheckpoint。

**checkpointing in progress：** 每个 job 运行结束后会调用 finalRdd.doCheckpoint()，finalRdd 会顺着 computing chain 回溯扫描，碰到要 checkpoint 的 RDD 就将其标记为 CheckpointingInProgress，然后将写磁盘（比如写 HDFS）需要的配置文件（如 core-site.xml 等）broadcast 到其他 worker 节点上的 blockManager。完成以后，启动一个 job 来完成 checkpoint（使用 `rdd.context.runJob(rdd, CheckpointRDD.writeToFile(path.toString, broadcastedConf))`）。

**checkpointed：**job 完成 checkpoint 后，将该 rdd 的 dependency 全部清掉，并设定该 rdd 状态为 checkpointed。然后，**为该 rdd 强加一个依赖，设置该 rdd 的 parent rdd 为 CheckpointRDD**，该 CheckpointRDD 负责以后读取在文件系统上的 checkpoint 文件，生成该 rdd 的 partition。

有意思的是我在 driver program 里 checkpoint 了两个 rdd，结果只有一个（下面的 result）被 checkpoint 成功，pairs2 没有被 checkpoint，也不知道是 bug 还是故意只 checkpoint 下游的 RDD：

```scala
val data1 = Array[(Int, Char)]((1, 'a'), (2, 'b'), (3, 'c'), 
    (4, 'd'), (5, 'e'), (3, 'f'), (2, 'g'), (1, 'h'))
val pairs1 = sc.parallelize(data1, 3)
val data2 = Array(Int, Char)
val pairs2 = sc.parallelize(data2, 2)
pairs2.checkpoint
val result = pairs1.join(pairs2)
result.checkpoint
```

#### *怎么读取 checkpoint 过的 RDD？*

在 runJob() 的时候会先调用 finalRDD 的 partitions() 来确定最后会有多个 task。rdd.partitions() 会去检查（通过 RDDCheckpointData 去检查，因为它负责管理被 checkpoint 过的 rdd）该 rdd 是会否被 checkpoint 过了，如果该 rdd 已经被 checkpoint 过了，直接返回该 rdd 的 partitions 也就是 Array[Partition]。

当调用 rdd.iterator() 去计算该 rdd 的 partition 的时候，会调用 computeOrReadCheckpoint(split: Partition) 去查看该 rdd 是否被 checkpoint 过了，如果是，就调用该 rdd 的 parent rdd 的 iterator() 也就是 CheckpointRDD.iterator()，CheckpointRDD 负责读取文件系统上的文件，生成该 rdd 的 partition。**这就解释了为什么那么 trickly 地为 checkpointed rdd 添加一个 parent CheckpointRDD。**

#### *cache 与 checkpoint 的区别？*

关于这个问题，Tathagata Das 有一段回答: There is a significant difference between cache and checkpoint. Cache materializes the RDD and keeps it in memory and/or disk（其实只有 memory）. But the lineage（也就是 computing chain） of RDD (that is, seq of operations that generated the RDD) will be remembered, so that if there are node failures and parts of the cached RDDs are lost, they can be regenerated. However, **checkpoint saves the RDD to an HDFS file and actually forgets the lineage completely.** This is allows long lineages to be truncated and the data to be saved reliably in HDFS (which is naturally fault tolerant by replication).

深入一点讨论，rdd.persist(StorageLevel.DISK_ONLY) 与 checkpoint 也有区别。前者虽然可以将 RDD 的 partition 持久化到磁盘，但该 partition 由 blockManager 管理。一旦 driver program 执行结束，也就是 executor 所在进程 CoarseGrainedExecutorBackend stop，blockManager 也会 stop，被 cache 到磁盘上的 RDD 也会被清空（整个 blockManager 使用的 local 文件夹被删除）。而 checkpoint 将 RDD 持久化到 HDFS 或本地文件夹，如果不被手动 remove 掉（**话说怎么 remove checkpoint 过的 RDD？**），是一直存在的，也就是说可以被下一个 driver program 使用，而 cached RDD 不能被其他 dirver program 使用。

## Discussion

Hadoop MapReduce 在执行 job 的时候，不停地做持久化，每个 task 运行结束做一次，每个 job 运行结束做一次（写到 HDFS）。在 task 运行过程中也不停地在内存和磁盘间 swap 来 swap 去。 可是讽刺的是，Hadoop 中的 task 太傻，中途出错需要完全重新运行，比如 shuffle 了一半的数据存放到了磁盘，下次重新运行时仍然要重新 shuffle。Spark 好的一点在于尽量不去持久化，所以使用 pipeline，cache 等机制。用户如果感觉 job 可能会出错可以手动去 checkpoint 一些 critical 的 RDD，job 如果出错，下次运行时直接从 checkpoint 中读取数据。唯一不足的是，checkpoint 需要两次运行 job。

### *Example For CheckPoint*

```scala
    import org.apache.spark.SparkContext
    import org.apache.spark.SparkContext._
    import org.apache.spark.SparkConf
    object groupByKeyTest {
      def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("GroupByKey").setMaster("local")
        val sc = new SparkContext(conf)
        sc.setCheckpointDir("/Users/xulijie/Documents/data/checkpoint")
        val data = Array[(Int, Char)]((1, 'a'), (2, 'b'),
          (3, 'c'), (4, 'd'),
          (5, 'e'), (3, 'f'),
          (2, 'g'), (1, 'h')
        )
        val pairs = sc.parallelize(data, 3)
        pairs.checkpoint
        pairs.count
        val result = pairs.groupByKey(2)
        result.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))
        println(result.toDebugString)
      }
    }
```