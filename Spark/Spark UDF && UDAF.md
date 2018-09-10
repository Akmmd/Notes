UDF:
UDAF:

```scala
/**
  * Spark SQL UDAS：user defined aggregation function
  * UDF: 函数的输入是一条具体的数据记录，实现上讲就是普通的scala函数-只不过需要注册
  * UDAF：用户自定义的聚合函数，函数本身作用于数据集合，能够在具体操作的基础上进行自定义操作
  */
object SparkSQLUDF {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLWindowFunctionOps")
    val sc = new SparkContext(conf)

    val hiveContext = new SQLContext(sc)

    val bigData = Array("Spark", "Hadoop", "Flink", "Spark", "Hadoop", "Flink", "Spark", "Hadoop", "Flink", "Spark", "Hadoop", "Flink")
    val bigDataRDD = sc.parallelize(bigData)

    val bigDataRowRDD = bigDataRDD.map(line => Row(line))
    val structType = StructType(Array(StructField("name", StringType, true)))
    val bigDataDF = hiveContext.createDataFrame(bigDataRowRDD, structType)

    bigDataDF.registerTempTable("bigDataTable")

    /*
     * 通过HiveContext注册UDF，在scala2.10.x版本UDF函数最多可以接受22个输入参数
     */
    hiveContext.udf.register("computeLength", (input: String) => input.length)
    hiveContext.sql("select name,computeLength(name)  as length from bigDataTable").show

    //while(true){}

    hiveContext.udf.register("wordCount", new MyUDAF)
    hiveContext.sql("select name,wordCount(name) as count,computeLength(name) as length from bigDataTable group by name ").show
  }
}

/**
  * 用户自定义函数
  */
class MyUDAF extends UserDefinedAggregateFunction {
  /**
    * 指定具体的输入数据的类型
    * 自段名称随意：Users can choose names to identify the input arguments - 这里可以是“name”，或者其他任意串
    */
  override def inputSchema: StructType = StructType(Array(StructField("name", StringType, true)))

  /**
    * 在进行聚合操作的时候所要处理的数据的中间结果类型
    */
  override def bufferSchema: StructType = StructType(Array(StructField("count", IntegerType, true)))

  /**
    * 返回类型
    */
  override def dataType: DataType = IntegerType

  /**
    * whether given the same input,
    * always return the same output
    * true: yes
    */
  override def deterministic: Boolean = true

  /**
    * Initializes the given aggregation buffer
    * 初始化给定的聚合缓冲区
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  /**
    * 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
    * 本地的聚合操作，相当于Hadoop MapReduce模型中的Combiner
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getInt(0) + 1
  }

  /**
    * 最后在分布式节点进行local reduce完成后需要进行全局级别的merge操作
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
  }

  /**
    * 返回UDAF最后的计算结果
    */
  override def evaluate(buffer: Row): Any = buffer.getInt(0)
}


```

```scala
//定义一个日期范围类

case class DateRange(startDate: Timestamp, endDate: Timestamp) {
  def in(targetDate: Date):
  Boolean = {
    targetDate.before(endDate) && targetDate.after(startDate)
  }

  override def toString(): String = {
    startDate.toLocaleString() + " " + endDate.toLocaleString();
  }
}

//定义UDAF函数,按年聚合后比较,需要实现UserDefinedAggregateFunction中定义的方法

class YearOnYearCompare(current: DateRange) extends UserDefinedAggregateFunction {
  val previous: DateRange = DateRange(subtractOneYear(current.startDate), subtractOneYear(current.endDate))

  println(current)
  println(previous)

  //UDAF与DataFrame列有关的输入样式,StructField的名字并没有特别要求，完全可以认为是两个内部结构的列名占位符。

  //至于UDAF具体要操作DataFrame的哪个列，取决于调用者，但前提是数据类型必须符合事先的设置，如这里的DoubleType与DateType类型

  def inputSchema: StructType = {
    StructType(StructField("metric", DoubleType) ::
      StructField("timeCategory", DateType) ::
      Nil)
  }

  //定义存储聚合运算时产生的中间数据结果的Schema

  def bufferSchema: StructType = {
    StructType(StructField("sumOfCurrent", DoubleType) ::
      StructField("sumOfPrevious", DoubleType) ::
      Nil)
  }

  //标明了UDAF函数的返回值类型

  def dataType: org.apache.spark.sql.types.DataType = DoubleType

  //用以标记针对给定的一组输入,UDAF是否总是生成相同的结果

  def deterministic: Boolean = true

  //对聚合运算中间结果的初始化

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0.0)
    buffer.update(1, 0.0)
  }

  //第二个参数input: Row对应的并非DataFrame的行,而是被inputSchema投影了的行。以本例而言，每一个input就应该只有两个Field的值

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (current.in(input.getAs[Date](1))) {
      buffer(0) = buffer.getAs[Double](0) + input.getAs[Double](0)
    }

    if (previous.in(input.getAs[Date](1))) {
      buffer(1) = buffer.getAs[Double](0) + input.getAs[Double](0)
    }
  }

  //负责合并两个聚合运算的buffer，再将其存储到MutableAggregationBuffer中

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)

    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
  }

  //完成对聚合Buffer值的运算,得到最后的结果

  def evaluate(buffer: Row): Any = {
    if (buffer.getDouble(1) == 0.0) {
      0.0
    } else {
      (buffer.getDouble(0) - buffer.getDouble(1)) / buffer.getDouble(1) * 100
    }
  }

  private def subtractOneYear(date: Timestamp): Timestamp = {
    val prev = new Timestamp(date.getTime)
    prev.setYear(prev.getYear - 1)
    prev
  }
}

								---Example---
object SimpleDemo {
  def main(args: Array[String]): Unit = {
    val dir = "D:/Program/spark/examples/src/main/resources/";
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("sqltest"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //用$符号来包裹一个字符串表示一个Column,定义在SQLContext对象implicits中的一个隐式转换

    //DataFrame的API可以接收Column对象,UDF的定义不能直接定义为Scala函数，而是要用定义在org.apache.spark.sql.functions中的udf方法来接收一个函数。

    //这种方式无需register

    //如果需要在函数中传递一个变量，则需要org.apache.spark.sql.functions中的lit函数来帮助

    //创建DataFrame

    val df = sqlContext.createDataFrame(Seq(
      (1, "张三峰", "广东 广州 天河", 24),
      (2, "李四", "广东 广州 东山", 36),
      (3, "王五", "广东 广州 越秀", 48),
      (4, "赵六", "广东 广州 海珠", 29))).toDF("id", "name", "addr", "age")

    //定义函数

    def splitAddrFunc: String => Seq[String] = {
      _.toLowerCase.split("\\s")
    }

    val longLength = udf((str: String, length: Int) => str.length > length)

    val len = udf((str: String) => str.length)

    //使用函数

    val df2 = df.withColumn("addr-ex", callUDF(splitAddrFunc, new ArrayType(StringType, true), df("addr")))
    val df3 = df2.withColumn("name-len", len($"name")).filter(longLength($"name", lit(2)))

    println("打印DF Schema及数据处理结果")

    df.printSchema()

    df3.printSchema()

    df3.foreach {
      println
    }

    //SQL模型

    //定义普通的scala函数，然后在SQLContext中进行注册，就可以在SQL中使用了。

    def slen(str: String): Int = str.length

    def slengthLongerThan(str: String, length: Int): Boolean = str.length > length

    sqlContext.udf.register("len", slen _)

    sqlContext.udf.register("longLength", slengthLongerThan _)

    df.registerTempTable("user")

    println("打印SQL语句执行结果")

    sqlContext.sql("select name,len(name) from user where longLength(name,2)").foreach(println)

    println("打印数据过滤结果")

    df.filter("longLength(name,2)").foreach(println)

    //如果定义UDAF(User Defined Aggregate Function)

    //Spark为所有的UDAF定义了一个父类UserDefinedAggregateFunction。要继承这个类，需要实现父类的几个抽象方法

    val salesDF = sqlContext.createDataFrame(Seq(
        (1, "Widget Co", 1000.00, 0.00, "AZ", "2014-01-02"),
        (2, "Acme Widgets", 2000.00, 500.00, "CA", "2014-02-01"),
        (3, "Widgetry", 1000.00, 200.00, "CA", "2015-01-11"),
        (4, "Widgets R Us", 5000.00, 0.0, "CA", "2015-02-19"),
        (5, "Ye Olde Widgete", 4200.00, 0.0, "MA", "2015-02-18"))).toDF("id", "name", "sales", "discount", "state", "saleDate")

    salesDF.registerTempTable("sales")

    val current = DateRange(Timestamp.valueOf("2015-01-01 00:00:00"), Timestamp.valueOf("2015-12-31 00:00:00"))

    //在使用上，除了需要对UDAF进行实例化之外，与普通的UDF使用没有任何区别

    val yearOnYear = new YearOnYearCompare(current)

    sqlContext.udf.register("yearOnYear", yearOnYear)

    val dataFrame = sqlContext.sql("select yearOnYear(sales, saleDate) as yearOnYear from sales")

    salesDF.printSchema()

    dataFrame.show()
  }
}
```

