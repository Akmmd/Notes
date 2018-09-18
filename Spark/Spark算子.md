```scala
基于SparkShell的交互式编程
1、map是对RDD中的每个元素都执行一个指定的函数来产生一个新的RDD。任何原RDD中的元素在新RDD中都有且只有一个元素与之对应。

val a = sc.parallelize(1 to 9, 3)  
# x =>*2是一个函数，x是传入参数即RDD的每个元素，x*2是返回值
val b = a.map(x => x*2)
a.collect  
# 结果Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)  
b.collect  
# 结果Array[Int] = Array(2, 4, 6, 8, 10, 12, 14, 16, 18)  

list/key--->key-value

val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", " eagle"), 2)
val b = a.map(x => (x, 1))
b.collect.foreach(println(_))

# /*
# (dog,1)
# (tiger,1)
# (lion,1)
# (cat,1)
# (panther,1)
# ( eagle,1)
# */

val l=sc.parallelize(List((1,'a'),(2,'b')))
var ll=l.map(x=>(x._1,"PV:"+x._2)).collect()
ll.foreach(println)
# (1,PVa)
# (2,PVb)

=================================================================
2、mapPartitions(function) 
map()的输入函数是应用于RDD中每个元素，而mapPartitions()的输入函数是应用于每个分区

package test
import scala.Iterator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestRdd {
  def sumOfEveryPartition(input: Iterator[Int]): Int = {
    var total = 0
    input.foreach { elem =>
      total += elem
    }
    total
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Rdd Test")
    val spark = new SparkContext(conf)
    val input = spark.parallelize(List(1, 2, 3, 4, 5, 6), 2)//RDD有6个元素，分成2个partition
    val result = input.mapPartitions(
      partition => Iterator(sumOfEveryPartition(partition)))//partition是传入的参数，是个list，要求返回也是list，即Iterator(sumOfEveryPartition(partition))
    result.collect().foreach {
      println(_)
      # 6 15,分区计算和
    }
    spark.stop()
  }
}

=================================================================
3、mapValues(function) 
原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素。因此，该函数只适用于元素为KV对的RDD

val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", " eagle"), 2)
val b = a.map(x => (x.length, x))
b.mapValues("x" + _ + "x").collect

# //结果 
# Array( 
# (3,xdogx), 
# (5,xtigerx), 
# (4,xlionx), 
# (3,xcatx), 
# (7,xpantherx), 
# (5,xeaglex) 
# )

# val grouped = mds.groupBy(md => md.matched)
# grouped.mapValues(x => x.size).foreach(println)

=================================================================
4、flatMap(function) 
与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素

val a = sc.parallelize(1 to 4, 2)
val b = a.flatMap(x => 1 to x)//每个元素扩展
b.collect
/*
结果    Array[Int] = Array( 1, 
                           1, 2, 
                           1, 2, 3, 
                           1, 2, 3, 4)
*/
sc.textFile("Path").flatMap(_.split("[,./ \'\"!-]")).map((_, 1)).reduceByKey(_+_).sortBy(_._1, false).savaAsTextFile("Path")

===============================================
5、flatMapValues(function)
val a = sc.parallelize(List((1,2),(3,4),(5,6)))
val b = a.flatMapValues(x=>1 to x)
b.collect.foreach(println(_))
/*结果
(1,1)
(1,2)
(3,1)
(3,2)
(3,3)
(3,4)
(5,1)
(5,2)
(5,3)
(5,4)
(5,5)
(5,6)
*/

val list = List(("mobin",22),("kpop",20),("lufei",23))
val rdd = sc.parallelize(list)
val mapValuesRDD = rdd.flatMapValues(x => Seq(x,"male"))
mapValuesRDD.foreach(println)

输出：

(mobin,22)
(mobin,male)
(kpop,20)
(kpop,male)
(lufei,23)
(lufei,male)

如果是mapValues会输出：【对比区别】

(mobin,List(22, male))
(kpop,List(20, male))
(lufei,List(23, male))

=================================================================
6、reduceByKey(func,numPartitions):按Key进行分组，使用给定的func函数聚合value值, numPartitions设置分区数，提高作业并行度

val arr = List(("A",3),("A",2),("B",1),("B",3))
val rdd = sc.parallelize(arr)
val reduceByKeyRDD = rdd.reduceByKey(_ +_)
reduceByKeyRDD.foreach(println)
sc.stop

# (A,5)
# (A,4)

=================================================================
7、groupByKey(numPartitions):按Key进行分组，返回[K,Iterable[V]]，numPartitions设置分区数，提高作业并行度【value并不是累加，而是变成一个数组】

//省略
val arr = List(("A",1),("B",2),("A",2),("B",3))
val rdd = sc.parallelize(arr)
val groupByKeyRDD = rdd.groupByKey()
groupByKeyRDD.foreach(println)
sc.stop

# (B,CompactBuffer(2, 3))
# (A,CompactBuffer(1, 2))


# 统计key后面的数组汇总元素的个数
scala> groupByKeyRDD.mapValues(x => x.size).foreach(println)
# (A,2)
# (B,2)

=================================================================
8、sortByKey(accending，numPartitions):返回以Key排序的（K,V）键值对组成的RDD，accending为true时表示升序，为false时表示降序，numPartitions设置分区数，提高作业并行度。

//省略sc
val arr = List(("A",1),("B",2),("A",2),("B",3))
val rdd = sc.parallelize(arr)
val sortByKeyRDD = rdd.sortByKey()
sortByKeyRDD.foreach(println)
sc.stop

# (A,1)
# (A,2)
# (B,2)
# (B,3)


# 统计单词的词频
val rdd = sc.textFile("/home/scipio/README.md")
val wordcount = rdd.flatMap(_.split(' ')).map((_,1)).reduceByKey(_+_)
val wcsort = wordcount.map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1))
wcsort.saveAsTextFile("/home/scipio/sort.txt")

# 升序的话，sortByKey(true)

=================================================================
9、cogroup(otherDataSet，numPartitions)：对两个RDD(如:(K,V)和(K,W))相同Key的元素先分别做聚合，最后返回(K,Iterator<V>,Iterator<W>)形式的RDD,numPartitions设置分区数，提高作业并行度

val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
val rdd1 = sc.parallelize(arr, 3)
val rdd2 = sc.parallelize(arr1, 3)
val groupByKeyRDD = rdd1.cogroup(rdd2)
groupByKeyRDD.foreach(println)
sc.stop

# (B,(CompactBuffer(2, 3),CompactBuffer(B1, B2)))
# (A,(CompactBuffer(1, 2),CompactBuffer(A1, A2)))

=================================================================
10、join(otherDataSet,numPartitions):对两个RDD先进行cogroup操作形成新的RDD，再对每个Key下的元素进行笛卡尔积，numPartitions设置分区数，提高作业并行度

//省略
val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
val rdd = sc.parallelize(arr, 3)
val rdd1 = sc.parallelize(arr1, 3)
val groupByKeyRDD = rdd.join(rdd1)
groupByKeyRDD.foreach(println)

# (B,(2,B1))
# (B,(2,B2))
# (B,(3,B1))
# (B,(3,B2))
 
# (A,(1,A1))
# (A,(1,A2))
# (A,(2,A1))
# (A,(2,A2))

=================================================================
11、LeftOutJoin(otherDataSet，numPartitions):"左外连接"，包含左RDD的所有数据，如果右边没有与之匹配的用None表示,numPartitions设置分区数，提高作业并行度

//省略
val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3),("C",1))
val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
val rdd = sc.parallelize(arr, 3)
val rdd1 = sc.parallelize(arr1, 3)
val leftOutJoinRDD = rdd.leftOuterJoin(rdd1)
leftOutJoinRDD.foreach(println)
sc.stop

# (B,(2,Some(B1)))
# (B,(2,Some(B2)))
# (B,(3,Some(B1)))
# (B,(3,Some(B2)))
# (C,(1,None))
# (A,(1,Some(A1)))
# (A,(1,Some(A2)))
# (A,(2,Some(A1)))
# (A,(2,Some(A2)))

=================================================================
12、RightOutJoin(otherDataSet, numPartitions):"右外连接"，包含右RDD的所有数据，如果左边没有与之匹配的用None表示,numPartitions设置分区数，提高作业并行度

//省略
val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"),("C","C1"))
val rdd = sc.parallelize(arr, 3)
val rdd1 = sc.parallelize(arr1, 3)
val rightOutJoinRDD = rdd.rightOuterJoin(rdd1)
rightOutJoinRDD.foreach(println)
sc.stop

# (B,(Some(2),B1))
# (B,(Some(2),B2))
# (B,(Some(3),B1))
# (B,(Some(3),B2))
# (C,(None,C1))
# (A,(Some(1),A1))
# (A,(Some(1),A2))
# (A,(Some(2),A1))
# (A,(Some(2),A2))

=================================================================
13、lookup（）
var rdd1=sc.parallelize(List((1,"a"),(2,"b"),(3,"c")))
# rdd1: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[81] at parallelize at 
rdd1.lookup(1)
# res34: Seq[String] = WrappedArray(a)

=================================================================
14、filter()： filter函数功能是对元素进行过滤，对每个元素应用f函数，返回值为 true 的元素在RDD中保留，返回值为 false 的元素将被过滤掉

val filterRdd = sc.parallelize(List(1,2,3,4,5)).map(_*2).filter(_>5)
filterRdd.collect
# res5: Array[Int] = Array(6, 8, 10)

=================================================================
16、collect()：collect：将RDD分散存储的元素转换为单机上的Scala数组并返回，类似于toArray功能

scala> var rdd1 = sc.makeRDD(1 to 10,2)
# rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[36] at makeRDD at :21
 
scala> rdd1.collect
# res23: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

=================================================================
17、reduce()：根据映射函数f，对RDD中的元素进行二元计算，返回计算结果

scala> var rdd1 = sc.makeRDD(1 to 10,2)
# rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[36] at makeRDD at :21
# 求和
scala> rdd1.reduce(_ + _)
# res18: Int = 55
 
scala> var rdd2 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
# rdd2: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[38] at makeRDD at :21
# 分项求和
scala> rdd2.reduce((x,y) => {
     |       (x._1 + y._1,x._2 + y._2)
     |     })
res21: (String, Int) = (CBBAA,6)

=================================================================
18、count()：返回RDD内元素的个数

scala> var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
# rdd1: org.apache.spark.rdd.RDD[(String, String)] = ParallelCollectionRDD[34] at makeRDD at :21
 
scala> rdd1.count
# res15: Long = 3

=================================================================
19、first()：返回RDD内的第一个元素，first相当于top(1)

scala> var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
# rdd1: org.apache.spark.rdd.RDD[(String, String)] = ParallelCollectionRDD[33] at makeRDD at :21
 
scala> rdd1.first
# res14: (String, String) = (A,1)

=================================================================
21、case：匹配，更多用于 PartialFunction(偏函数)中 {case …}

scala> val aa=List(1,2,3,"asa")
# aa: List[Any] = List(1, 2, 3, asa)
scala> aa. map {
     |   case i: Int => i + 1
     |   case s: String => s.length
     | }
# res16: List[Int] = List(2, 3, 4, 3)
```

###reduceByKey与groupByKey的区别？

------

1. 当采用reduceByKeyt时，Spark可以在每个分区移动数据之前将待输出数据与一个共用的key结合。借助下图可以理解在reduceByKey里究竟发生了什么。 注意在数据对被搬移前同一机器上同样的key是怎样被组合的(reduceByKey中的lamdba函数)。然后lamdba函数在每个区上被再次调用来将所有值reduce成一个最终结果。

2. 当采用groupByKey时，由于它不接收函数，spark只能先将所有的键值对(key-value pair)都移动，这样的后果是集群节点之间的开销很大，导致传输延时。

   因此，在对大数据进行复杂计算时，reduceByKey优于groupByKey。
   另外，如果仅仅是group处理，那么以下函数应该优先于 groupByKey ：
    　　（1）combineByKey 组合数据，但是组合之后的数据类型与输入时值的类型不一样。
    　　（2）foldByKey合并每一个 key 的所有值，在级联函数和“零值”中使用。

#### reduceByKey

> reduceyByKey() 相当于传统的 MapReduce，整个数据流也与 Hadoop 中的数据流基本一样。reduceyByKey() 默认在map 端开启 combine()，因此在 shuffle 之前先通过 mapPartitions 操作进行 combine，得到 MapPartitionsRDD，然后shuffle 得到 ShuffledRDD，然后再进行 reduce(通过 aggregate + mapPartitions() 操作来实现)得到MapPartitionsRDD。

### Coalesce

------

Spark：对RDD的分区进行重新划分；

SQL：COALESCE是一个函数， (expression_1, expression_2, ...,expression_n)依次参考各参数表达式，遇到非null值即停止并返回该值。如果所有的表达式都是空值，最终将返回一个空值。使用COALESCE在于大部分包含空值的表达式最终将返回空值。

#### Coalesce与Repartition的区别

`repartition(numPartitions:Int)和coalesce(numPartitions:Int，shuffle:Boolean=false)`

coalesce()方法的作用是返回指定一个新的指定分区的Rdd。

> 如果是生成一个窄依赖的结果，那么不会发生shuffle。比如：1000个分区被重新设置成10个分区，这样不会发生shuffle。当把父Rdd的分区数量增大时，比如Rdd的分区是100，设置成1000，如果shuffle为false，并不会起作用。这时候就需要设置shuffle为true了，那么Rdd将在shuffle之后返回一个1000个分区的Rdd，数据分区方式默认是采用 hash partitioner。

repartition()方法的源码：

```SCALA
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    coalesce(numPartitions, shuffle = true)
  }
```

从源码可以看出，repartition()方法就是coalesce()方法shuffle为true的情况。

**如果shuff为false时，如果传入的参数大于现有的分区数目，RDD的分区数不变，也就是说不经过shuffle，是无法将RDDde分区数变多的。**

### action 和 transformation 算子

------

#### Transformation具体内容

- map(func) :返回一个新的分布式数据集，由每个原元素经过func函数转换后组成
- filter(func) : 返回一个新的数据集，由经过func函数后返回值为true的原元素组成 
  *flatMap(func) : 类似于map，但是每一个输入元素，会被映射为0到多个输出元素（因此，func函数的返回值是一个Seq，而不是单一元素）
- flatMap(func) : 类似于map，但是每一个输入元素，会被映射为0到多个输出元素（因此，func函数的返回值是一个Seq，而不是单一元素）
- sample(withReplacement, frac, seed) : 
  根据给定的随机种子seed，随机抽样出数量为frac的数据
- union(otherDataset) : 返回一个新的数据集，由原数据集和参数联合而成
- groupByKey([numTasks]) : 
  在一个由（K,V）对组成的数据集上调用，返回一个（K，Seq[V])对的数据集。注意：默认情况下，使用8个并行任务进行分组，你可以传入numTask可选参数，根据数据量设置不同数目的Task
- reduceByKey(func, [numTasks]) : 在一个（K，V)对的数据集上使用，返回一个（K，V）对的数据集，key相同的值，都被使用指定的reduce函数聚合到一起。和groupbykey类似，任务的个数是可以通过第二个可选参数来配置的。
- join(otherDataset, [numTasks]) : 
  在类型为（K,V)和（K,W)类型的数据集上调用，返回一个（K,(V,W))对，每个key中的所有元素都在一起的数据集
- groupWith(otherDataset, [numTasks]) : 在类型为（K,V)和(K,W)类型的数据集上调用，返回一个数据集，组成元素为（K, Seq[V], Seq[W]) Tuples。这个操作在其它框架，称为CoGroup
- cartesian(otherDataset) : 笛卡尔积。但在数据集T和U上调用时，返回一个(T，U）对的数据集，所有元素交互进行笛卡尔积。
- flatMap(func) : 
  类似于map，但是每一个输入元素，会被映射为0到多个输出元素（因此，func函数的返回值是一个Seq，而不是单一元素）

#### Actions具体内容

- reduce(func) : 通过函数func聚集数据集中的所有元素。Func函数接受2个参数，返回一个值。这个函数必须是关联性的，确保可以被正确的并发执行
- collect() : 在Driver的程序中，以数组的形式，返回数据集的所有元素。这通常会在使用filter或者其它操作后，返回一个足够小的数据子集再使用，直接将整个RDD集Collect返回，很可能会让Driver程序OOM
- count() : 返回数据集的元素个数
- take(n) : 返回一个数组，由数据集的前n个元素组成。注意，这个操作目前并非在多个节点上，并行执行，而是Driver程序所在机器，单机计算所有的元素(Gateway的内存压力会增大，需要谨慎使用）
- first() : 返回数据集的第一个元素（类似于take(1)）
- saveAsTextFile(path) : 将数据集的元素，以textfile的形式，保存到本地文件系统，hdfs或者任何其它hadoop支持的文件系统。Spark将会调用每个元素的toString方法，并将它转换为文件中的一行文本
- saveAsSequenceFile(path) : 将数据集的元素，以sequencefile的格式，保存到指定的目录下，本地系统，hdfs或者任何其它hadoop支持的文件系统。RDD的元素必须由key-value对组成，并都实现了Hadoop的Writable接口，或隐式可以转换为Writable（Spark包括了基本类型的转换，例如Int，Double，String等等）
- foreach(func) : 在数据集的每一个元素上，运行函数func。这通常用于更新一个累加器变量，或者和外部存储系统做交互

#### 算子分类

大致可以分为三大类算子: 
1. Value数据类型的Transformation算子，这种变换并不触发提交作业，针对处理的数据项是Value型的数据。 
2. Key-Value数据类型的Transfromation算子，这种变换并不触发提交作业，针对处理的数据项是Key-Value型的数据对。 
3. Action算子，这类算子会触发SparkContext提交Job作业。