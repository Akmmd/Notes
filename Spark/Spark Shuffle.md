##### &ensp;&ensp;Shuffle的中文解释为“洗牌操作”，可以理解成将集群中所有节点上的数据进行重新整合分类的过程。其思想来源于hadoop的mapReduce,Shuffle是连接map阶段和reduce阶段的桥梁。由于分布式计算中，每个阶段的各个计算节点只处理任务的一部分数据，若下一个阶段需要依赖前面阶段的所有计算结果时，则需要对前面阶段的所有计算结果进行重新整合和分类，这就需要经历shuffle过程。 

##### 	&ensp;&ensp;与MapReduce计算框架一样，Spark的Shuffle实现大致如下图所示，在DAG阶段以shuffle为界，划分stage，上游stage做map task，每个map task将计算结果数据分成多份，每一份对应到下游stage的每个partition中，并将其临时写到磁盘，该过程叫做shuffle write；下游stage做reduce task，每个reduce task通过网络拉取上游stage中所有map task的指定分区结果数据，该过程叫做shuffle read，最后完成reduce的业务逻辑。

> ![](/Users/wsh/Desktop/note/img/spark-shuffle-overview.png)

#####		&ensp;&ensp;在spark中，RDD之间的关系包含窄依赖和宽依赖，其中宽依赖涉及shuffle操作。因此在spark程序的每个job中，都是根据是否有shuffle操作进行阶段（stage）划分，每个stage都是一系列的RDD map操作。

**宽依赖（涉及Shuffle操作）：**

------

**指父RDD的每个分区都有可能被多个子RDD分区使用，子RDD分区==通常对应父RDD所有分区==。**

> ![](/Users/wsh/Desktop/note/img/kuan.png)

宽依赖的函数有：

 ##### 	groupByKey, join(父RDD不是hash-partitioned), partitionBy

**窄依赖：**

**指父RDD的每个分区只被一个子RDD分区使用，子RDD分区通常只对应常数个父RDD分区。**

> ![3958688-04c7a31cd515ed86](/Users/wsh/Desktop/note/img/zhai.png)

窄依赖的函数有：

 	#####		map, filter, union, join(父RDD是hash-partitioned ), mapPartitions, mapValues

##### 比较

> 		**宽依赖往往对应着shuffle操作，需要在运行的过程中将同一个RDD分区传入到不同的RDD分区中，中间可能涉及到多个节点之间数据的传输，而窄依赖的每个父RDD分区通常只会传入到另一个子RDD分区，通常在一个节点内完成；**
>
> 		**当RDD分区丢失时，对于窄依赖来说，由于父RDD的一个分区只对应一个子RDD分区，这样只需要重新计算与子RDD分区对应的父RDD分区就行。这个计算对数据的利用是100%的；**
>	
> 		**当RDD分区丢失时，对于宽依赖来说，重算的父RDD分区只有一部分数据是对应丢失的子RDD分区的，另一部分就造成了多余的计算。宽依赖中的子RDD分区通常来自多个父RDD分区，极端情况下，所有父RDD都有可能重新计算。**

#*Spark ShuffleManager*

------

#####		Spark程序中的Shuffle操作是通过shuffleManage对象进行管理。Spark目前支持的ShuffleMange模式主要有两种：HashShuffleMagnage 和 SortShuffleManage 。

#####		Shuffle操作包含当前阶段的Shuffle Write（存盘）和下一阶段的Shuffle Read（fetch）,两种模式的主要差异是在Shuffle Write阶段

##*HashShuffleMagnage*

------

HashShuffleMagnage是Spark1.2之前版本的默认模式，在集群中的每个executor上

> 	下图说明了未经优化的HashShuffleManager的原理。这里我们先明确一个假设前提：每个Executor只有1个CPU core，也就是说，无论这个Executor上分配多少个task线程，同一时间都只能执行一个task线程。
>
> 　　我们先从shuffle write开始说起。**shuffle write**阶段，主要就是在一个stage结束计算之后，为了下一个stage可以执行shuffle类的算子（比如reduceByKey），而将每个task处理的数据按key进行“分类”。**所谓“分类”，就是对相同的key执行hash算法，从而将相同key都写入同一个磁盘文件中，而每一个磁盘文件都只属于下游stage的一个task。在将数据写入磁盘之前，会先将数据写入内存缓冲中，当内存缓冲填满之后，才会溢写到磁盘文件中去。**
>
> 　　那么每个执行shuffle write的task，要为下一个stage创建多少个磁盘文件呢？很简单，**下一个stage的task有多少个，当前stage的每个task就要创建多少份磁盘文件**。比如下一个stage总共有100个task，那么当前stage的每个task都要创建100份磁盘文件。如果当前stage有50个task，总共有10个Executor，每个Executor执行5个Task，那么每个Executor上总共就要创建500个磁盘文件，所有Executor上会创建5000个磁盘文件。由此可见，未经优化的shuffle write操作所产生的磁盘文件的数量是极其惊人的。
>
> 　　接着我们来说说shuffle read。**shuffle read通常就是一个stage刚开始时要做的事情**。此时该stage的每一个task就需要将上一个stage的计算结果中的所有相同key，从各个节点上通过网络都拉取到自己所在的节点上，然后进行key的聚合或连接等操作。由于shuffle write的过程中，task给下游stage的每个task都创建了一个磁盘文件，因此shuffle read的过程中，每个task只要从上游stage的所有task所在节点上，拉取属于自己的那一个磁盘文件即可。
>
> 　　shuffle read的拉取过程是一边拉取一边进行聚合的。每个shuffle read task都会有一个自己的buffer缓冲，每次都只能拉取与buffer缓冲相同大小的数据，然后通过内存中的一个Map进行聚合等操作。聚合完一批数据后，再拉取下一批数据，并放到buffer缓冲中进行聚合操作。以此类推，直到最后将所有数据到拉取完，并得到最终的结果。
>
> ![](/Users/wsh/Desktop/note/img/hashshuffle.jpeg)

在executor中处理每个task后的结果均会通过buffler缓存的方式写入到多个磁盘文件中，其中文件的个数由shuffle算子的numPartition参数指定（图中partition为3）。因此Shuffle Write 阶段会产生大量的磁盘文件，整个Shuffle Write 阶段的文件总数为： **Write阶段的task数目 * Read阶段的task数目**。 

由于HashShuffleManage方式会产生很多的磁盘文件，Spark对其进行了优化，具体优化点为：

 	1. executor处理多个task的时候只会根据Read阶段的task数目（设为m）生成对应的文件数，具体做法是：处理第一个task时生成m个文件，后续task的结果追加到对应的m个文件中。 
 	2. 考虑到executor的并行计算能力（core数量），处理任务的每个core均会生成m个文件。 
    因此，优化后的HashShuffleManage最终的总文件数： **Write阶段的core数量  * Read阶段的task数目**。

###*优化后的HashShuffleManage*

> 	下图说明了优化后的HashShuffleManager的原理。这里说的优化，是指我们可以设置一个参数，spark.shuffle.consolidateFiles。该参数默认值为false，将其设置为true即可开启优化机制。通常来说，如果我们使用HashShuffleManager，那么都建议开启这个选项。
>
> 　　开启consolidate机制之后，在shuffle write过程中，task就不是为下游stage的每个task创建一个磁盘文件了。此时会出现**shuffleFileGroup**的概念，**每个shuffleFileGroup会对应一批磁盘文件，磁盘文件的数量与下游stage的task数量是相同的。一个Executor上有多少个CPU core，就可以并行执行多少个task**。而第一批并行执行的每个task都会创建一个shuffleFileGroup，并将数据写入对应的磁盘文件内。
>
> 　　**当Executor的CPU core执行完一批task，接着执行下一批task时，下一批task就会复用之前已有的shuffleFileGroup，包括其中的磁盘文件**。也就是说，此时task会将数据写入已有的磁盘文件中，而不会写入新的磁盘文件中。因此，consolidate机制允许不同的task复用同一批磁盘文件，这样就可以有效将多个task的磁盘文件进行一定程度上的合并，从而大幅度减少磁盘文件的数量，进而提升shuffle write的性能。
>
> 　　假设第二个stage有100个task，第一个stage有50个task，总共还是有10个Executor，每个Executor执行5个task。那么原本使用未经优化的HashShuffleManager时，每个Executor会产生500个磁盘文件，所有Executor会产生5000个磁盘文件的。但是此时经过优化之后，每个Executor创建的磁盘文件的数量的计算公式为：CPU core的数量 * 下一个stage的task数量。也就是说，每个Executor此时只会创建100个磁盘文件，所有Executor只会创建1000个磁盘文件。
>
> ![](/Users/wsh/Desktop/note/img/hashshuffleB.jpeg)

###SortShuffleManage 

#####	SortShuffleManager的运行机制主要分成两种，一种是普通运行机制，另一种是bypass运行机制。当shuffle read task的数量小于等于spark.shuffle.sort.bypassMergeThreshold参数的值时（默认为200），就会启用bypass机制。

### *普通运行机制*

------

> 	下图说明了普通的SortShuffleManager的原理。在该模式下，数据会先写入一个**内存数据结构**中，此时根据不同的shuffle算子，可能选用不同的数据结构。**如果是reduceByKey这种聚合类的shuffle算子，那么会选用Map数据结构，一边通过Map进行聚合，一边写入内存；如果是join这种普通的shuffle算子，那么会选用Array数据结构，直接写入内存**。接着，每写一条数据进入内存数据结构之后，就会判断一下，是否达到了某个临界阈值。**如果达到临界阈值的话，那么就会尝试将内存数据结构中的数据溢写到磁盘，然后清空内存数据结构**。
>
> 	**在溢写到磁盘文件之前，会先根据key对内存数据结构中已有的数据进行排序**。排序过后，会分批将数据写入磁盘文件。默认的batch数量是10000条，也就是说，排序好的数据，会以每批1万条数据的形式分批写入磁盘文件。**写入磁盘文件是通过Java的BufferedOutputStream实现的。BufferedOutputStream是Java的缓冲输出流，首先会将数据缓冲在内存中，当内存缓冲满溢之后再一次写入磁盘文件中，这样可以减少磁盘IO次数，提升性能**。
>
> 　　一个task将所有数据写入内存数据结构的过程中，会发生多次磁盘溢写操作，也就会产生多个临时文件。**最后会将之前所有的临时磁盘文件都进行合并，这就是merge过程，此时会将之前所有临时磁盘文件中的数据读取出来，然后依次写入最终的磁盘文件之中**。此外，**由于一个task就只对应一个磁盘文件，也就意味着该task为下游stage的task准备的数据都在这一个文件中，因此还会单独写一份索引文件，其中标识了下游各个task的数据在文件中的start offset与end offset**。
>
> 　　SortShuffleManager由于有一个磁盘文件merge的过程，因此大大减少了文件数量。比如第一个stage有50个task，总共有10个Executor，每个Executor执行5个task，而第二个stage有100个task。由于每个task最终只有一个磁盘文件，因此此时每个Executor上只有5个磁盘文件，所有Executor只有50个磁盘文件。
>
> ![](/Users/wsh/Desktop/note/img/basesortshuffle.jpg)

###*bypass运行机制*

> 	下图说明了bypass SortShuffleManager的原理。bypass运行机制的触发条件如下：
>
>  1. shuffle map task数量小于spark.shuffle.sort.bypassMergeThreshold参数的值。
>  2. 不是聚合类的shuffle算子（比如reduceByKey）。
>
> 　　此时task会为每个下游task都创建一个临时磁盘文件，并将数据按key进行hash然后根据key的hash值，将key写入对应的磁盘文件之中。当然，写入磁盘文件时也是先写入内存缓冲，缓冲写满之后再溢写到磁盘文件的。最后，同样会将所有临时磁盘文件都合并成一个磁盘文件，并创建一个单独的索引文件。
>
> 　　该过程的磁盘写机制其实跟未经优化的HashShuffleManager是一模一样的，因为都要创建数量惊人的磁盘文件，只是在最后会做一个磁盘文件的合并而已。因此少量的最终磁盘文件，也让该机制相对未经优化的HashShuffleManager来说，shuffle read的性能会更好。
>
> 　　**而该机制与普通SortShuffleManager运行机制的不同在于：第一，磁盘写机制不同；第二，不会进行排序。也就是说，启用该机制的最大好处在于，shuffle write过程中，不需要进行数据的排序操作，也就节省掉了这部分的性能开销**。
>
> ![](/Users/wsh/Desktop/note/img/bypass.jpg)

##*sortShuffleManager补充*

> ![](/Users/wsh/Desktop/note/img/spark-shuffle-v3.png)
>
> 	在map阶段(shuffle write)，会按照partition id以及key对记录进行排序，将所有partition的数据写在同一个文件中，该文件中的记录首先是按照partition id排序一个一个分区的顺序排列，每个partition内部是按照key进行排序存放，map task运行期间会顺序写每个partition的数据，并通过一个索引文件记录每个partition的大小和偏移量。这样一来，每个map task一次只开两个文件描述符，一个写数据，一个写索引，大大减轻了Hash Shuffle大量文件描述符的问题，即使一个executor有K个core，那么最多一次性开K*2个文件描述符。
>	
> 	在reduce阶段(shuffle read)，reduce task拉取数据做combine时不再是采用HashMap，而是采用ExternalAppendOnlyMap，该数据结构在做combine时，如果内存不足，会刷写磁盘，很大程度的保证了鲁棒性，避免大数据情况下的OOM。
>	
> 	总体上看来Sort Shuffle解决了Hash Shuffle的所有弊端，但是因为需要其shuffle过程需要对记录进行排序，所以在性能上有所损失。

##*Unsafe Shuffle*

> 		从spark 1.5.0开始，spark开始了钨丝计划(Tungsten)，目的是优化内存和CPU的使用，进一步提升spark的性能。为此，引入Unsafe Shuffle，**它的做法是将数据记录用二进制的方式存储，直接在序列化的二进制数据上sort而不是在java对象上，这样一方面可以减少memory的使用和GC的开销，另一方面避免shuffle过程中频繁的序列化以及反序列化。在排序过程中，它提供cache-efficient sorter，使用一个8 bytes的指针，把排序转化成了一个指针数组的排序，极大的优化了排序性能**。
>
> 	但是使用Unsafe Shuffle有几个限制，shuffle阶段不能有aggregate操作，分区数不能超过一定大小(224−1224−1，这是可编码的最大parition id)，所以像reduceByKey这类有aggregate操作的算子是不能使用Unsafe Shuffle，它会退化采用Sort Shuffle。

##*Sort Shuffle v2*

	从spark-1.6.0开始，把Sort Shuffle和Unsafe Shuffle全部统一到Sort Shuffle中，如果检测到满足Unsafe Shuffle条件会自动采用Unsafe Shuffle，否则采用Sort Shuffle。从spark-2.0.0开始，spark把Hash Shuffle移除，可以说目前spark-2.0中只有一种Shuffle，即为Sort Shuffle。







Shuffle --->   consolidation机制
