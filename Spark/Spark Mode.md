#####Spark运行job有2种模式：

###*`cluster mode `*

> 	Spark driver在 application的master process中运行。如果和YARN集成，则application master process由YARN管理，在YARN中运行。

###*`client mode`*

> 	Spark driver在clinet process中运行。如果集成YARN，application master只负责从YARN请求资源。

###*`Standalone模式下存在的角色`*

> 1. `Client`：客户端进程，负责提交作业到Master。
> 2. `Master`：Standalone模式中主控节点，负责接收Client提交的作业，管理Worker，并命令Worker启动Driver和Executor。
> 3. `Worker`：Standalone模式中slave节点上的守护进程，负责管理本节点的资源，定期向Master汇报心跳，接收Master的命令，启动Driver和Executor。
> 4. `Drive`： 一个Spark作业运行时包括一个Driver进程，也是作业的主进程，负责作业的解析、生成Stage并调度Task到Executor上。包括DAGScheduler，TaskScheduler。
> 5. `Executor`：即真正执行作业的地方，一个集群一般包含多个Executor，每个Executor接收Driver的命令Launch Task，一个Executor可以执行一到多个Task。

###*`作业相关的名词解释`*

> 1. `Stage`：一个Spark作业一般包含一到多个Stage。
> 2. `Task`：一个Stage包含一到多个Task，通过多个Task实现并行运行的功能。
> 3. `DAGScheduler`： 实现将Spark作业分解成一到多个Stage，每个Stage根据RDD的Partition个数决定Task的个数，然后生成相应的Task set放到TaskScheduler中。
> 4. `TaskScheduler`：实现Task分配到Executor上执行。

##*Cluster mode*

##### 1. cluster mode without yarn

> 客户端提交作业给Master
>
> ![](/Users/wsh/Desktop/note/img/cluster_1.jpg)
>
> 1. Master让一个Worker启动Driver（上图中间的worker），即SchedulerBackend。 Worker创建一个DriverRunner线程，DriverRunner启动SchedulerBackend进程。
> 2. Master让其余Worker启动Exeuctor，即ExecutorBackend。Worker创建一个ExecutorRunner线程，ExecutorRunner会启动ExecutorBackend进程。 ExecutorBackend启动后会向Driver的SchedulerBackend注册。
> 3. 在Driver上的SchedulerBackend进程中包含DAGScheduler，它会根据用户程序，生成执行计划，并调度执行。对于每个stage的task，都会被存放到TaskScheduler中，ExecutorBackend向SchedulerBackend汇报的时候把TaskScheduler中的task调度到ExecutorBackend执行。 所有stage都完成后作业结束。

##### 2. cluster mode on yarn

​	spark Driver首先作为一个ApplicationMaster在YARN集群中启动，客户端提交给ResourceManager的每一个job都会在集群的worker节点上分配一个唯一的ApplicationMaster，由该ApplicationMaster管理全生命周期的应用。因为Driver程序在YARN中运行，所以事先不用启动Spark Master/Client，应用的运行结果不能在客户端显示（可以在history server中查看），所以最好将结果保存在HDFS而非stdout输出，客户端的终端显示的是作为YARN的job的简单运行状况。

> 该模式下的特点:
>
> 1. spark driver运行在由YARN管理的application master process(管理所有应用生命周期)当中
> 2. Driver程序在YARN中运行，所以事先不用启动Spark Master/Client
> 3. 应用的运行结果不能在客户端显示（可以在history server中查看），客户端的终端显示的是作为YARN的job的简单运行状况。

该模式的运行过程是：

> ![](/Users/wsh/Desktop/note/img/cluster_2.1.jpg)
>
> ------
>
> ![](/Users/wsh/Desktop/note/img/cluster_2.2.jpg)
>
> ------
>
> ![](/Users/wsh/Desktop/note/img/cluster_2.3.jpg)
>
> 1. 由client向ResourceManager提交请求，并上传jar到HDFS上
>    这期间包括四个步骤：
>    **a). 连接到RM**
>
>    **b). 从RM ASM（ApplicationsManager ）中获得metric、queue和resource等信息。**
>
>    **c). upload app jar and spark-assembly jar**
>
>    **d). 设置运行环境和container上下文（launch-container.sh等脚本)**
>
> 2. ResouceManager向NodeManager申请资源，创建Spark ApplicationMaster（每个SparkContext都有一个ApplicationMaster）
>
> 3. NodeManager启动Spark App Master，并向ResourceManager AsM注册
>
> 4. Spark ApplicationMaster从HDFS中找到jar文件，启动DAGscheduler和YARN Cluster Scheduler
>
> 5. ResourceManager向ResourceManager AsM注册申请container资源（INFO YarnClientImpl: Submitted application）
>
> 6. ResourceManager通知NodeManager分配Container，这时可以收到来自ASM关于container的报告。（每个container的对应一个executor）
>
> 7. Spark ApplicationMaster直接和container（executor）进行交互，完成这个分布式任务。
>
>   **需要注意的是：**
>   a). Spark中的localdir会被yarn.nodemanager.local-dirs替换
>   b). 允许失败的节点数(spark.yarn.max.worker.failures)为executor数量的两倍数量，最小为3.
>   c). SPARK_YARN_USER_ENV传递给spark进程的环境变量
>   d). 传递给app的参数应该通过–args指定。

#####3. 使用Yarn与不使用Yarn的区别

> 用YARN之后的主要区别就是:
>
> 1. 在结构当中添加了node manager和resource manger分别替代了worker和master的工作
> 2. spark driver运行在由YARN管理的application master process当中
> 3. client提交任务给RM，而不是master
> 4. node manager来替代worker的工作，例如启动YARN容器，然后启动executor

## *Client model*

##### 1. client mode without yarn

> job运行过程：
>
> ![](/Users/wsh/Desktop/note/img/client_1.jpg)
>
> 作业执行流程描述：
>
> 1. 客户端启动后直接运行用户程序，启动Driver相关的工作：DAGScheduler和BlockManagerMaster等。
> 2. 客户端的Driver向Master注册。 Master还会让Worker启动Exeuctor。
> 3. Worker创建一个ExecutorRunner线程，ExecutorRunner会启动ExecutorBackend进程。
> 4. ExecutorBackend启动后会向Driver的SchedulerBackend注册。Driver的DAGScheduler解析作业并生成相应的Stage，每个Stage包含的Task通过TaskScheduler分配给Executor执行。 所有stage都完成后作业结束。

##### 2. client mode with yarn

在yarn-client模式下，Driver运行在Client上，通过ApplicationMaster向RM获取资源。本地Driver负责与所有的executor container进行交互，并将最后的结果汇总。结束掉终端，相当于kill掉这个spark应用。一般来说，如果运行的结果仅仅返回到terminal上时需要配置这个。

> ![](/Users/wsh/Desktop/note/img/client_2.jpg)
>
> 客户端的Driver将应用提交给Yarn后，Yarn会先后启动ApplicationMaster和executor，另外ApplicationMaster和executor都 是装载在container里运行，container默认的内存是1G，ApplicationMaster分配的内存是driver- memory，executor分配的内存是executor-memory。同时，因为Driver在客户端，所以程序的运行结果可以在客户端显 示，Driver以进程名为SparkSubmit的形式存在。
> 配置YARN-Client模式同样需要HADOOP_CONF_DIR/YARN_CONF_DIR和SPARK_JAR变量。

client mode on yarn的主要特点可以概括如下：

> 1. spark driver运行在client上
> 2. 本地Driver负责与所有的executor container进行交互，并将最后的结果汇总。
> 3. 结束掉终端，相当于kill掉这个spark应用。