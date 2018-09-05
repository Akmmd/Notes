	Spark 程序必须做的第一件事情是创建一个SparkContext对象，它会告诉 Spark 如何访问集群。

### *`RDD：`*

  它是一个容错且可以执行并行操作的元素的集合。有两种方法可以创建 RDD : 在你的 driver program（驱动程序）中 parallelizing 一个已存在的集合，或者在外部存储系统中引用一个数据集，例如，一个共享文件系统，HDFS，HBase，或者提供 Hadoop InputFormat 的任何数据源。

1. 在已存在的集合上通过调用 SparkContext 的 parallelize 方法来创建并行集合，该集合的元素从一个可以并行操作的 distributed dataset（分布式数据集）中复制到另一个 dataset（数据集）中去。

2. 可以使用 SparkContext 的 textFile 方法来创建文本文件的 RDD。此方法需要一个文件的 URI（计算机上的本地路径），并且读取它们作为一个 lines（行）的集合。

   - 2-1 SparkContext.wholeTextFiles 可以读取包含多个小文本文件的目录, 并且将它们作为一个 (filename, content) pairs 来返回；

   + 2-2 使用 SparkContext 的 sequenceFile[K, V] 方法，其中 K 和 V 指的是文件中 key 和 values 的类型。

