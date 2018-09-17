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