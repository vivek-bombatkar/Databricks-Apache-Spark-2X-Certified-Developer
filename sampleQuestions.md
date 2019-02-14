## Databricks - Apache Spark™ - 2X Certified Developer - sample questions

perp course:  
- half of the day,     
- good understanding of the exam pattern  
- benefits of certification  
- dos /donts   
- exam material  
- ml / graph - not specific , high level idea, api concept, ex ml pipeline into api,  
- key api understanding  
- cant leave the deak  
- 3 hrs  
- passing 65%, 40 questions  
- free retake , if fail  
- py & scala 2 seperate exam selections.  
- dont stress py / scala, but tuples-rdd, etc  
- graph , ml , stream - only one Q for each  
- no pen , paper. only notes sections  
- practice exam available after registratin for real exam , sample 10 questions  

- 30% Spark architecture  
	deployment mode  
	caching persit, unpersist, storage level  
	partitioning, read from source partition, repartition - coaleasce() vs repartition, # of shuffle partition  
	performance, catalyst, bottleneck  

- 40% sql df   
	R W DF, reader class, writer class  
	T va A, wide vs narrow  
	*joins, types, broadcast, cross joins  
	*UDF  
	*window functions  

- 10% rdd low level api  
	pair rdd, map, flat map,  
	rdd - df conversion  
	*accumulator , accumulator2   
	wide transf, reduceByKey, groupByKey  
- 10% streaming  
	sources , sinks  
	fault tolerance  
	df manupalation  
	watermark  
	checkpoint  
- 5% ml  
	ml pipeline, transform, estimators  
	model selection, evaluator, parameter grids  
	no specific algo logic needed  
- 5% graph  
	creating graphFrame   
	inDegrees, outDegrees, bfs, shortestPath, triangleCount   
	
- Key API  
	sparkSession  
	*Dtataframe/DS  
	*DFReader, DFWritter -  what source built in, csv json - schema needed, parquit schema inbuilt, comparision  
	*column ,row - manupalation,   
	spark.sql.functions - *broadcast   
	
## test exam

- 1.	Which of the following DataFrame operations are wide transformations (that is, they result in a shuffle)?  
 	
*A.		repartition()  
B.		filter()  
*C.		orderBy()  
*D.		distinct()  
E.		drop()  
F.		cache()  


- 2.	Which of the following methods are NOT a DataFrame action?  
 	
*A.		limit()  
B.		foreach()  
C.		first()  
*D.		printSchema()  
E.		show()  
*F.		cache()  


- 3.	Which of the following statements about Spark accumulator variables is NOT true?  
 	
A.		For accumulator updates performed inside actions only, Spark guarantees that each task’s update to the accumulator will be applied only once, meaning that restarted tasks will not update the value. In transformations, each task’s update can be applied more than once if tasks or job stages are re-executed.  
B.		Accumulators provide a shared, mutable variable that a Spark cluster can safely update on a per-row basis.  
C.		You can define your own custom accumulator class by extending org.apache.spark.util.AccumulatorV2 in Java or Scala or pyspark.AccumulatorParam in Python.   
*D.		The Spark UI displays all accumulators used by your application.  

Ref: D is FALSE, as spark ui only displays named accumulators.  
> https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-accumulators.html  
> https://www.edureka.co/blog/spark-accumulators-explained  


- 4.	 Given an instance of SparkSession named spark, review the following code:  
 
```python
import org.apache.spark.sql.functions._

val a = Array(1002, 3001, 4002, 2003, 2002, 3004, 1003, 4006)

val b = spark
  .createDataset(a)
  .withColumn("x", col("value") % 1000)

val c = b
  .groupBy(col("x"))
  .agg(count("x"), sum("value"))
  .drop("x")
  .toDF("count", "total")
  .orderBy(col("count").desc, col("total"))
  .limit(1)
  .show()
```

Which of the following results is correct?

```python 	
*A.		
+-----+-----+
|count|total|
+-----+-----+
|    3| 7006|
+-----+-----+
 
B.		
+-----+-----+
|count|total|
+-----+-----+
|    1| 3001|
+-----+-----+
 
C.		
+-----+-----+
|count|total|
+-----+-----+
|    2| 8008|
+-----+-----+
D.		
+-----+-----+
|count|total|
+-----+-----+
|    8|20023|
+-----+-----+
 ```

- 5. Given an instance of SparkSession named spark, which one of the following code fragments executemost quickly and produce a DataFrame with the specified schema? Assume a variable named schema with the correctly structured StructType to represent the DataFrame's schema has already been initialized.

Sample data:
 

id,firstName,lastName,birthDate,email,country,phoneNumber
1,Pennie,Hirschmann,2017-12-03,ph123@databricks.com,US,+1(123)4567890

Schema:
 
```python
id: integer
firstName: string
lastName: string
birthDate: timestamp
email: string
county: string
phoneNumber: string
 	
A.		
val df = spark.read
   .option("inferSchema", "true")
   .option("header", "true")
   .csv("/data/people.csv")
B.		
val df = spark.read
   .option("inferSchema", "true")
   .schema(schema)
   .csv("/data/people.csv")
C.		
val df = spark.read
   .schema(schema)
   .option("sep", ",")
   .csv("/data/people.csv")
*D.		
val df = spark.read
   .schema(schema)
   .option("header", "true")
   .csv("/data/people.csv")
```

- 6. Consider the following DataFrame:
 
```python
val rawData = Seq(
  (1, 1000, "Apple", 0.76),
  (2, 1000, "Apple", 0.11),
  (1, 2000, "Orange", 0.98),
  (1, 3000, "Banana", 0.24),
  (2, 3000, "Banana", 0.99)
)
val dfA = spark.createDataFrame(rawData).toDF("UserKey", "ItemKey", "ItemName", "Score")

Select the code fragment that produces the following result:
 

+-------+-----------------------------------------------------------------+
|UserKey|Collection                                                       |
+-------+-----------------------------------------------------------------+
|1      |[[0.98, 2000, Orange], [0.76, 1000, Apple], [0.24, 3000, Banana]]|
|2      |[[0.99, 3000, Banana], [0.11, 1000, Apple]]                      |
+-------+-----------------------------------------------------------------+
 	
A.		
import org.apache.spark.sql.expressions.Window
dfA.withColumn(
    "Collection",
    collect_list(struct("Score", "ItemKey", "ItemName")).over(Window.partitionBy("ItemKey"))
  )
  .select("UserKey", "Collection")
  .show(20, false)
 
B.		
dfA.groupBy("UserKey")
  .agg(collect_list(struct("Score", "ItemKey", "ItemName")))
  .toDF("UserKey", "Collection")
  .show(20, false)
 
C.		
dfA.groupBy("UserKey", "ItemKey", "ItemName")
  .agg(sort_array(collect_list(struct("Score", "ItemKey", "ItemName")), false))
  .drop("ItemKey", "ItemName")
  .toDF("UserKey", "Collection")
  .show(20, false)
 
*D.		
dfA.groupBy("UserKey")
  .agg(sort_array(collect_list(struct("Score", "ItemKey", "ItemName")), false))
  .toDF("UserKey", "Collection")
  .show(20, false)

```

- 7. tableA is a DataFrame consisting of 20 fields and 40 billion rows of data with a surrogate key field. tableB is a DataFrame functioning as a lookup table for the surrogate key consisting of 2 fields and 5,000 rows. If the in-memory size of tableB is 22MB, what occurs when the following code is executed:
 
```python
val df = tableA.join(broadcast(tableB), Seq("primary_key"))
``` 	
A.		The broadcast function is non-deterministic, thus a BroadcastHashJoin is likely to occur, but isn't guaranteed to occur.  
*B.		A normal hash join will be executed with a shuffle phase since the broadcast table is greater than the 10MB default threshold and the broadcast command can be overridden silently by the Catalyst optimizer.  
C.		The contents of tableB will be replicated and sent to each executor to eliminate the need for a shuffle stage during the join.  
D.		An exception will be thrown due to tableB being greater than the 10MB default threshold for a broadcast join.  


- 8. Consider the following DataFrame:  
 
```python
import org.apache.spark.sql.functions._

val people = Seq(
    ("Ali", 0, Seq(100)),
    ("Barbara", 1, Seq(300, 250, 100)),
    ("Cesar", 1, Seq(350, 100)),
    ("Dongmei", 1, Seq(400, 100)),
    ("Eli", 2, Seq(250)),
    ("Florita", 2, Seq(500, 300, 100)),
    ("Gatimu", 3, Seq(300, 100))
  )
  .toDF("name", "department", "score")
```

Select the code fragment that produces the following result:
 
```python
+----------+-------+-------+
|department|   name|highest|
+----------+-------+-------+
|         0|    Ali|    100|
|         1|Dongmei|    400|
|         2|Florita|    500|
|         3| Gatimu|    300|
+----------+-------+-------+
 	
A.		
val maxByDept = people
  .withColumn("score", explode(col("score")))
  .groupBy("department")
  .max("score")
  .withColumnRenamed("max(score)", "highest")

maxByDept
  .join(people, "department")
  .select("department", "name", "highest")
  .orderBy("department")
  .dropDuplicates("department")
  .show()
 
B.		
people
  .withColumn("score", explode(col("score")))
  .orderBy("department", "score")
  .select(col("name"), col("department"), first(col("score")).as("highest"))
  .show()
 
*C.		
import org.apache.spark.sql.expressions.Window

val windowSpec = Window.partitionBy("department").orderBy(col("score").desc)

people
  .withColumn("score", explode(col("score")))
  .select(
    col("department"),
    col("name"),
    dense_rank().over(windowSpec).alias("rank"),
    max(col("score")).over(windowSpec).alias("highest")
  )
  .where(col("rank") === 1)
  .drop("rank")
  .orderBy("department")
  .show()
D.		
people
  .withColumn("score", explode(col("score")))
  .groupBy("department")
  .max("score")
  .withColumnRenamed("max(score)", "highest")
  .orderBy("department")
  .show()
```   
	 
	 
- 9.	Which of the following standard Structured Streaming sink types are idempotent and can provide end-to-end exactly-once semantics in a Structured Streaming job?  
 	
A.		Console  
*B.		Kafka  
C.		File  
D.		Memory  


- 10. Which of following statements regarding caching are TRUE?  
 	
*A.		The default storage level for a DataFrame is StorageLevel.MEMORY_AND_DISK.  
*B.		The uncache() method evicts a DataFrame from cache.  
C.		The persist() method immediately loads data from its source to materialize the DataFrame in cache.  
D.		Explicit caching can decrease application performance by interfering with the Catalyst optimizer's ability to optimize some queries.  


Score:	80%  
Result:	Pass  


## more practice question

1. RDD 
***Below are transformations***  
map(func)    
filter(func)  
flatMap(func)  
mapPartitions(func)    
mapPartitionsWithIndex(func)  
sample(withReplacement, fraction, seed)  
union(otherDataset)  
intersection(otherDataset)  
distinct([numPartitions]))  
groupByKey([numPartitions])  
reduceByKey(func, [numPartitions])  
aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions])  
sortByKey([ascending], [numPartitions])  
join(otherDataset, [numPartitions])  
cogroup(otherDataset, [numPartitions])  
cartesian(otherDataset)  
pipe(command, [envVars])  
coalesce(numPartitions)  
repartition(numPartitions)  
repartitionAndSortWithinPartitions(partitioner)  
***cache()  
printSchema()
select()
limit()
coalesce(numPartitions) 
toDF()
toRDD()

***Below are Actions***  
reduce(func)   
collect()  
count()  
first()  
take(n)  
takeSample(withReplacement, num, [seed])  
takeOrdered(n, [ordering])  
saveAsTextFile(path)  
saveAsSequenceFile(path) (Java and Scala)  
saveAsObjectFile(path) (Java and Scala)  
countByKey()  
foreach(func)  

2.  Repartioning dataframe
    - Spark automatically sets the number of partitions of an input file according to its size and for distributed shuffles.  
    - By default spark create one partition for each block of the file in HDFS it is 64MB by default.
    -  we should make RDD with number of ***partition is equal to number of cores in the cluster***   
    - by this all partition will process parallel and resources are also used equally.  
    
    
3. Base RDD
    - val lines = sc.textFile("data.txt") // line #1  
    - The first line defines a base RDD from an external file.  
    
4. cartetion product , cross join 

5. FizzBuzz on 1..100 in spark 

```
    def transform(n: Int): String = {
      if (n % 3 == 0 && n % 5 == 0) "Fizz Buzz"
      else if (n % 3 == 0) "Fizz"
      else if (n % 5 == 0) "Buzz"
      else n.toString
    }
     val fizzBuzzKV = numbers.map(n => {
      (transform(n), 1)
    })

    fizzBuzzKV.collect().foreach(println)

    val fizzBuzzCount = fizzBuzzKV.reduceByKey(_ + _)

    // print
    fizzBuzzCount.sortByKey(numPartitions = 1).foreach(t => println(s"${t._1}  - ${t._2}"))

```

```
val mod3 = numbers.filter(num => num%3 == 0).map(x => (x,"Fizz"))

val mod5 = numbers.filter(num => num%5 == 0).map(x => (x,"Buzz"))

val other = numbers.filter(num => !(num%5 == 0 ||  num%3 == 0)).map(x => (x,x+""))

//combine the three RDDs and use mapreduce to combine Fizz and Buzz for % 15
val combined = mod3.union(mod5).union(other).reduceByKey((s1,s2) => s1+" "+s2)

combined.takeOrdered(50).foreach(x => println(x._2))
```

```
for num in range(1,21):
    string = ""
    if num % 3 == 0:
        string = string + "Fizz"
    if num % 5 == 0:
        string = string + "Buzz"
    if num % 5 != 0 and num % 3 != 0:
        string = string + str(num)
    print(string)
```

```
for num in range(1, 101):
    if num % 15 is 0:
        print("FizzBuzz")
    elif num % 3 is 0:
        print("Fizz")
    elif num % 5 is 0:
        print("Buzz")
    else:
        print(num)
 ```
 
 
6. port, host, opDir, Exception

7. unsupported file format   
Supported : json, parquet, jdbc, orc, libsvm, csv, text

8. With cache(), you use only the default storage level MEMORY_ONLY

9. partitions , shuffal partitons, default parallelism

10. persist()

11. Catalyst , tungstun

12. memory bottleneck - groupByKey, reduceByKey, count, reuce Key Locally

13. akka node size limitation 128 

14. crossJoin

15. df.groupBy(window(df[0],"2 minuts","1 minuts"),df[0]).count()  
        - every 2 min window, sliding every 1 min  
16. lit("foo")

17. coalesce(numPartitions) vs repartition()
    - Returns a new DataFrame that has exactly numPartitions partitions.
    - coalesce uses existing partitions to minimize the amount of data that's shuffled. 
    - repartition creates new partitions and does a full shuffle.  
    - coalesce results in partitions with different amounts of data (sometimes partitions that have much different sizes) 
    - and repartition results in roughly equal sized partitions.
    - Is coalesce or repartition faster?
        - coalesce may run faster than repartition, 
        - but unequal sized partitions are generally slower to work with than equal sized partitions. 
        - You'll usually need to repartition datasets after filtering a large data set. 
        - I've found repartition to be faster overall because Spark is built to work with equal sized partitions.

18. catalyst : Accumalator

19. JVM HEAP MEMORY

20. broadcast join

21. 
