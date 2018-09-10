&ensp;&ensp;**从Map Task任务中的map()方法中的最后一步调用即输出中间数据开始，一直到Reduce Task任务开始执行reduce()方法结束，这一中间处理过程就被称为MapReduce的Shuffle**。Shuffle过程分为两个阶段：Map端的shuffle阶段和Reduce端的Shuffle阶段。 

### Shuffle流程

#### *Collect阶段*

&ensp;&ensp;在Map Task任务的业务处理方法map()中，最后一步通过OutputCollector.collect(key,value)或context.write(key,value)输出Map Task的中间处理结果，在相关的collect(key,value)方法中，会调用Partitioner.getPartition(K2 key, V2 value, int numPartitions)方法获得输出的key/value对应的分区号(分区号可以认为对应着一个要执行Reduce Task的节点)，然后将<key,value,partition>暂时保存在内存中的MapOutputBuffe内部的环形数据缓冲区，该缓冲区的默认大小是100MB，可以通过参数io.sort.mb来调整其大小。

&ensp;&ensp;当缓冲区中的数据使用率达到一定阀值后，触发一次Spill操作，将环形缓冲区中的部分数据写到磁盘上，生成一个临时的Linux本地数据的spill文件；然后在缓冲区的使用率再次达到阀值后，再次生成一个spill文件。直到数据处理完毕，在磁盘上会生成很多的临时文件。

&ensp;&ensp;MapOutputBuffe内部存数的数据采用了两个索引结构，涉及三个环形内存缓冲区。下来看一下两级索引结构：

![](https://github.com/Akmmd/Notes/raw/master/img/collect.jpg)

>1. **kvoffsets缓冲区，也叫偏移量索引数组，用于保存key/value信息在位置索引kvindices中的偏移量。当kvoffsets的使用率超过io.sort.spill.percent(默认为80%)后，便会触发一次SpillThread线程的“溢写”操作，也就是开始一次Spill阶段的操作**。
>2. **kvindices缓冲区，也叫位置索引数组，用于保存key/value在数据缓冲区kvbuffer中的起始位置**。
>3. **kvbuffer即数据缓冲区，用于保存实际的key/value的值。默认情况下该缓冲区最多可以使用io.sort.mb的95%，当kvbuffer使用率超过io.sort.spill.percent(默认为80%)后，便会出发一次SpillThread线程的“溢写”操作，也就是开始一次Spill阶段的操作**。

#### *Spill阶段*

&ensp;&ensp;当缓冲区的使用率达到一定阀值后，触发一次“溢写”操作，将环形缓冲区中的部分数据写到Linux的本地磁盘。 

&ensp;&ensp;需要特别注意的是，在将数据写磁盘之前，先要对要写磁盘的数据进行一次排序操作，先按<key,value,partition>中的partition分区号排序，然后再按key排序，再必要的时候，比如说配置了Combiner并且当前系统的负载不是很高的情况下会将有相同partition分区号和key的数据做聚合操作，还有如果设置而对中间数据做压缩的配置则还会做压缩操作。 

#### *Combine阶段*

&ensp;&ensp;待Map Task任务的所有数据都处理完后，会对任务产生的所有中间数据文件做一次合并操作，以确保一个Map Task最终只生成一个中间数据文件。 

#### *Copy阶段*

&ensp;&ensp;默认情况下，当整个MapReduce作业的所有已执行完成的Map Task任务数超过Map Task总数的5%后，JobTracker便会开始调度执行Reduce Task任务。然后Reduce Task任务默认启动mapred.reduce.parallel.copies(默认为5）个MapOutputCopier线程到已完成的Map Task任务节点上分别copy一份属于自己的数据。

&ensp;&ensp;这些copy的数据会首先保存的内存缓冲区中，当内冲缓冲区的使用率达到一定阀值后，则写到磁盘上。

#### *Merge阶段*

&ensp;&ensp;在远程copy数据的同时，Reduce Task在后台启动了两个后台线程对内存和磁盘上的数据文件做合并操作，以防止内存使用过多或磁盘生的文件过多。 

#### *Sort阶段*

&ensp;&ensp;在合并的同时，也会做排序操作。由于各个Map Task已经实现对数据做过局部排序，故此Reduce Task只需要做一次归并排序即可保证copy数据的整体有序性。

&ensp;&ensp;执行完合并与排序操作后，Reduce Task会将数据交给reduce()方法处理。 

