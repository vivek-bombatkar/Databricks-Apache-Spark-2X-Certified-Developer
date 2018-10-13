

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
