## Hive查询中的Select

#### 1. 基础查询语法

	Hive中的SELECT基础语法和标准SQL语法基本一致，支持WHERE、DISTINCT、GROUP BY、ORDER BY、HAVING、LIMIT、子查询等；

```sql
[WITH CommonTableExpression (, CommonTableExpression)*]  
SELECT [ALL | DISTINCT] select_expr, select_expr, ...
FROM table_reference
[WHERE where_condition]
[GROUP BY col_list]
[CLUSTER BY col_list
  | [DISTRIBUTE BY col_list] [SORT BY col_list]
]
[LIMIT number]
```

#### 2. Order By 和 Sort By

	ORDER BY用于全局排序，就是对指定的所有排序键进行全局排序，使用ORDER BY的查询语句，最后会用一个Reduce Task来完成全局排序。 SORT BY用于分区内排序，即每个Reduce任务内排序。

#### 3. Distrubute By 和 Culster By

	distribute by：按照指定的字段或表达式对数据进行划分，输出到对应的Reduce或者文件中。 
	
	cluster by：除了兼具distribute by的功能，还兼具sort by的排序功能。 

#### 4. 子查询

	子查询和标准SQL中的子查询语法和用法基本一致，需要注意的是，Hive中如果是从一个子查询进行SELECT查询，那么子查询必须设置一个别名。

```sql
SELECT col
FROM (
  SELECT a+b AS col
  FROM t1
) t2
```

```sql
									--WHERE子句中也支持子查询
SELECT *
FROM A
WHERE A.a IN (SELECT foo FROM B);
 
SELECT A
FROM T1
WHERE EXISTS (SELECT B FROM T2 WHERE T1.X = T2.Y)
```

```sql
			--一种将子查询作为一个表的语法，叫做Common Table Expression（CTE）
with q1 as (select * from src where key= '5'),
q2 as (select * from src s2 where key = '4')
select * from q1 union all select * from q2;
 
with q1 as ( select key, value from src where key = '5')
from q1
insert overwrite table s1
select *;
```

## 创建表

	创建表的语法选项特别多，这里只列出常用的选项。
	
	其他请参见Hive官方文档：

<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable>

```sql
CREATE EXTERNAL TABLE t_lxw1234 ( -- 关键字EXTERNAL表示该表为外部表，如果不指定EXTERNAL关键字，则表示内部表

id INT,

ip STRING COMMENT ‘访问者IP’,

avg_view_depth DECIMAL(5,1),

bounce_rate DECIMAL(6,5)

) COMMENT ‘lxw的大数据田地-lxw1234.com’ -- 关键字COMMENT，为表和列添加注释

PARTITIONED BY (day STRING)	-- 表示该表为分区表，分区字段为day,类型为string

ROW FORMAT DELIMITED

FIELDS TERMINATED BY ‘,’

STORED AS textfile

LOCATION 'hdfs://cdh5/tmp/lxw1234/';	-- 关键词LOCATION指定表在HDFS上的存储位置。

						-- 关键字STORED AS
指定表在HDFS上的文件存储格式，可选的文件存储格式有：
'  TEXTFILE //文本，默认值
-- SEQUENCEFILE // 二进制序列文件
-- RCFILE //列式存储格式文件 Hive0.6以后开始支持
-- ORC //列式存储格式文件，比RCFILE有更高的压缩比和读写效率，Hive0.11以后开始支持
-- PARQUET //列出存储格式文件，Hive0.13以后开始支持'

						-- 关键字ROW FORMAT DELIMITED

指定表的分隔符，通常后面要与以下关键字连用：

FIELDS TERMINATED BY ',' -- 指定每行中字段分隔符为逗号

LINES TERMINATED BY '\n' -- 指定行分隔符

COLLECTION ITEMS TERMINATED BY ',' -- 指定集合中元素之间的分隔符

MAP KEYS TERMINATED BY ':' -- 指定数据中Map类型的Key与Value之间的分隔符

举个例子：

create table score(name string, score map<string,int>)

ROW FORMAT DELIMITED

FIELDS TERMINATED BY '\t'

COLLECTION ITEMS TERMINATED BY ','

MAP KEYS TERMINATED BY ':';

要加载的文本数据为：

biansutao ‘数学':80,’语文':89,’英语':95

jobs ‘语文':60,’数学':80,’英语':99
```

### *内部表和外部表最大的区别*

1. 内部表DROP时候==会删除==HDFS上的数据;
2. 外部表DROP时候==不会删除==HDFS上的数据

## *Left Join、Right Join 与 Inner Join*

1. **Left Join**

   ```sql
   Select * from A left join B on A.id = b.id
   ```

   	*Left Join*是以A表的记录为基础的,A可以看成左表,B可以看成右表,left join是以左表为准的. 换句话说,左表(A)的记录将会全部表示出来,而右表(B)只会显示符合搜索条件的记录(例子中为: A.aID = B.bID). B表记录不足的地方均为NULL.

2. **Right Join**

   ```sql
   Select * from A right join B on A.id = B.id
   ```

   	*Right Join*和left join的结果刚好相反,这次是以右表(B)为基础的,A表不足的地方用NULL填充.

3. **Inner Join**

   ```sql
   Select * from A inner join B on A.id = B.id where A.name = 'sao'
   ```

   	*Inner Join*并不以谁为基础,它只显示符合条件的记录.  还有就是inner join 可以结合where语句来使用.

## *优化*





