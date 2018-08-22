# Spark-all-commands

:q
val data = sc.parallelize(Array(1,2,3,4,5))
data.count
val sqdata = data.map(num => num * num)
sqdata.collect
:q
val data = sc.parallelize(Array(1,2,3,4,5))
val sqdata = data.map(num => num * num)
sqdata.collect
sqdata.count
sqdata.first
:q
sc.version
val data = sc.parallelize(Array(1,2,3,4,5))
data.getNumPartitions
val data = sc.parallelize(Array(1,2,3,4,5), 1)
data.getNumPartitions
val data = sc.parallelize(Array(1,2,3,4,5), 2)
data.isEmpty
val emptyRDD = sc.emptyRDD()
val emptyRDD = sc.emptyRDD
emptyRDD.isEmpty
data.isEmpty
data.collect
val data = sc.parallelize(List(1,2,3,4,5), 2)
val data = sc.parallelize(Map(1 -> 'Hari',2 -> 'Ravi'), 2)
val data = sc.parallelize(Map(1 -> "Hari",2 -> "Ravi"), 2)
val data = sc.parallelize(Map("1" -> "Hari", "2" -> "Ravi"), 2)
val map = Map("KA" -> "Bangalore", "Bhihar" -> "Patna")
map
val data = sc.parallelize(map)
val states = Seq(("KA", "Bangalore"), ("Bhihar","Patna"))
val data = sc.parallelize(states)
val names = Set("Ravi", "Hari", "Siva")
val data = sc.parallelize(names)
val data = sc.parallelize(List(1,2,3,4,5))
data.getNumPartitions
val mydata = data.map(num => num * num)
mydata.getNumPartitions
val mydata = data.map(num => num * num, 2)
val mydata = data.map(num => (num * num), 2)
val mydata = data.map(num => (num * num))
val mydata = data.map(num => (num * num), 2)
val mydata = data.map(num => (num * num))
val data = sc.makeRDD(Array(1,2,3,4,5)4040.)4040.
val data = sc.makeRDD(Array(1,2,3,4,5))
data.collect
data.getNumPartitions
val stocksRDD = sc.textFile("file:///home/viswa/bigdata/spark-2.3.0/stocks")
stocksRDD.getNumPartitions
stocksRDD.first
val projRDD = stocksRDD.map(record => {
val cols = record.split("\\t")
(cols(1), cols(7).toLong)
}
)
projRDD.first
val projRDD = stocksRDD.map(record => (record.split("\\t")(1), record.split("\\t")(7).toLong)
)
def getStockVolume(reocrd:String) : (String, Long) = {
val cols = record.trim().split("\\t")
(cols(1), cols(7).toLong)
}
def getStockVolume(record:String) : (String, Long) = {
val cols = record.trim().split("\\t")
(cols(1), cols(7).toLong)
}
val projRDD = stocksRDD.map(record => getStockVolume(record))
projRDD.first
val projRDD = stocksRDD.map(getStockVolume)
projRDD.first
projRDD.take(scala> val projRDD = stocksRDD.map(getStockVolume)
projRDD.take(10)
val stocksRDD = sc.textFile("file:///home/viswa/bigdata/spark-2.3.0/stocks")
val mydata = sc.wholeTextFiles(file:///home/viswa/bigdata/spark-2.3.0/sales")
;
val mydata = sc.wholeTextFiles("file:///home/viswa/bigdata/spark-2.3.0/sales")
mydata.first
val data = mydata.map(record => record._2)
data.first
data.collect
val data = mydata.map(record => record._2.replace("\"", ""))
data.collect
val data = mydata.map(record => record._2.replaceAll("\"", ""))
data.collect
val data = mydata.map(record => record._2).map(record => record.replaceAll("\"", "")
)
data.collect
val data = mydata.map(record => record._2).map(record => record.replaceAll("\n", ""))
data.collect
val people = sc.textFile("file:///home/viswa/bigdata/spark-2.3.0/sales/people")
people.collect
data.collect
data.saveAsTextFile("file:///home/viswa/bigdata/spark-2.3.0/sales/names")
val data = mydata.map(record => record._2).map(record => record.replaceAll("\n", ""), 1)
val data = mydata.map(record => record._2).map(record => record.replaceAll("\n", "")).coalesce(1)
data.saveAsTextFile("file:///home/viswa/bigdata/spark-2.3.0/sales/names")
data.saveAsObjectFile("file:///home/viswa/bigdata/spark-2.3.0/sales/names")
val mydata = sc.objectFile("file:///home/viswa/bigdata/spark-2.3.0/sales/names")
mydata.firsy
mydata.first
val mydata = sc.objectFile("file:///home/viswa/bigdata/spark-2.3.0/sales/names/part-00000")
mydata.first
val mydata = sc.objectFile("file:///home/viswa/bigdata/spark-2.3.0/sales/names/part-00000")
mydata.first
val mydata = sc.objectFile("file:///home/viswa/bigdata/spark-2.3.0/sales/names/")
val mydata = sc.objectFile("file:///home/viswa/bigdata/spark-2.3.0/sales/names/part-00000")
val data = mydata.map(record => record._2.toString())
val data = mydata.map(record => record.toString())
data.collect
val data = sc.makeRDD(Array(1,2,3,4,5,6,7,8,9))
val even = data.filter(num => num % 2 == 0)
even.collect
val data = sc.makeRDD(Array(Array(1,3,5,7,9), Array(2,4,6,8,10)))
data.collect
val mydata = data.flatMap(record => record)
mydata.collect
sc.makeRDD(Array(Array(1,3,5,7,9), Array(2,4,6,8,10))).flatMap().collect()
sc.makeRDD(Array(Array(1,3,5,7,9), Array(2,4,6,8,10))).flatMap(record => record).collect()
sc.textFile("/home/viswa/bigdata/spark-2.3.0/NOTICE").flatMap(record => record.split(" ")).collect
sc.textFile("file:///home/viswa/bigdata/spark-2.3.0/NOTICE").flatMap(record => record.split(" ")).collect
sc.textFile("file:///home/viswa/bigdata/spark-2.3.0/NOTICE").flatMap(record => record.split(" ")).map(word => word.replaceAll("\\W+", "").collect
)
sc.textFile("file:///home/viswa/bigdata/spark-2.3.0/NOTICE").flatMap(record => record.split(" ")).map(word => word.replaceAll("\\W+", "")).collect
sc.textFile("file:///home/viswa/bigdata/spark-2.3.0/NOTICE").flatMap(record => record.split(" ")).map(word => word.replaceAll("\\W+", "")).filter(record => record.length >= 3).collect
sc.textFile("file:///home/viswa/bigdata/spark-2.3.0/NOTICE").flatMap(record => record.split(" ")).map(word => word.replaceAll("\\W+", "")).filter(record => record.length >= 3).map(word => (word, 1)).collect
sc.textFile("file:///home/viswa/bigdata/spark-2.3.0/NOTICE").flatMap(record => record.split(" ")).map(word => word.replaceAll("\\W+", "")).filter(record => record.length >= 3).map(word => (word, 1)).reduceByKey((a, b) => (a + b)).collect
val conf = sc.getConf
val conf_one = conf.clone()A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost. 
val conf_one = conf.clone()
conf_one == conf
val conf = sc.getConf
conf.contains("spark.app.master")
conf.contains("spark.master")
conf.get("spark.master")
conf.getAll()
conf.getAll
conf.getAllWithPrefix
conf.getAllWithPrefix("spark")
conf.getAppId
conf.get("spark.app.master")
conf.getOption("spark.app.master")
conf.getOption("spark.app.master").isDefined
conf.getOption("spark.app.master").nonEmpty
conf.getOption("spark.driver.port")
conf.getInt("spark.driver.port")
conf.getInt("spark.driver.port", 0)
conf.getInt("spark.abc.port", 0)
conf.getExecutorEnv
sc
sc.version
sc.sparkUser
sc.startTime
sc.getConf
sc.uiWebUrl
sc.statusTracker
sc.setLogLevel("WARN")
sc.master
conf.get("spark.master")
sc.deployMode
sc.appName
sc.applicationId
sc.applicationAttemptId
sc.getAllPools
sc.getConf
sc.getCheckpointDir
sc.getExecutorMemoryStatus
sc.getExecutorStorageStatus
sc.getPoolForName
sc.
;
sc.listFiles
sc.addFile("/home/viswa/bigdata/spark-2.3.0/stocks")
sc.listFiles
sc.addJar("/home/viswa/bigdata/spark-2.3.0/jar/*")
sc.addJar("/home/viswa/bigdata/spark-2.3.0/jar/zookeeper-3.4.6.jar")
sc.addJar("/home/viswa/bigdata/spark-2.3.0/jars/zookeeper-3.4.6.jar")
sc.listJars
sc.addJar("/home/viswa/bigdata/spark-2.3.0/jars/*")
sc.addJar("/home/viswa/bigdata/spark-2.3.0/jars/zookeeper-3.4.6.jar")
sc.listJars
sc.files()
sc.files
sc.binaryFiles
sc.binaryFiles()broadcast
sc.binaryFiles()
val city = sc.broadcast("bangalore")
city.id
city.value
city.unpersist
city.value
city.toString
city.destroy
city.value
sc.defaultParallelism
sc.defaultMinPartitions
sc.setCheckpointDir("/home/viswa/bigdata/sparkdata")
sc.setCheckpointDir("file:///home/viswa/bigdata/sparkdata")
sc.getCheckpointDir
sc.getSchedulingMode
sc.hadoopConfiguration
sc.hadoopConfiguration.get("fs.defaultFS")
sc.hadoopConfiguration.get("dfs.replication")
sc.isLocal
sc.isStopped
sc.jars
sc.range(0,1000, 2)
sc.range(0,1000, 2).collect
sc.setJobDescription("This is Spark-shell job")
sc.setLocalProperty("name", "hari")
sc.getLocalProperty("name")
val rdd1 = sc.parallelize(Array(1,2,3))
val rdd2 = sc.parallelize(Array(4,5,6))
val rdd3 = sc.union(Seq(rdd1, rdd2))
rdd3.collect
val rdd3 = rdd1 ++ rdd2
rdd3.collect
val emptyRDD = sc.emptyRDD
emptyRDD.isEmpty
emptyRDD.collect
q!
quit
exit
ll
help
exit 
def welcome(name:String)(age:Int)
= {
println(name)
}
welcome(export SCALA_HOME=/home/viswa/bigdata/scala-2.11.8
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export SPARK_HOME=/home/viswa/bigdata/spark-2.3.0
export HADOOP_HOME=/home/viswa/bigdata/hadoop-2.7.6
export PATH=$PATH:$SCALA_HOME/bin:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
 welcome(export SCALA_HOME=/home/viswa/bigdata/scala-2.11.8
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export SPARK_HOME=/home/viswa/bigdata/spark-2.3.0
export HADOOP_HOME=/home/viswa/bigdata/hadoop-2.7.6
export PATH=$PATH:$SCALA_HOME/bin:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
welcome("naga")
welcome("naga")_
sc
val dataRDD = sc.ran(export SCALA_HOME=/home/viswa/bigdata/scala-2.11.8
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export SPARK_HOME=/home/viswa/bigdata/spark-2.3.0
export HADOOP_HOME=/home/viswa/bigdata/hadoop-2.7.6
export PATH=$PATH:$SCALA_HOME/bin:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
val dataRDD = sc.range(0, 200, 1)
dataRDD.first
dataRDD.take(2)
sc
val sparkContext = dataRDD.context
sc
sparkContext
sc == sparkContext
dataRDD.dependencies
val mydataRDD = dataRDD.map(record => record * record)
mydataRDD.dependencies
dataRDD.getNumPartitions
mydataRDD.getNumPartitions
val mydataRDD = dataRDD.map(record => (record * record), 2)
val mydataRDD = dataRDD.map(record => (record * record)).coalesce(2)
mydataRDD.getNumPartitions
val mydataRDD = dataRDD.map(record => (record * record)).coalesce(8, true)
val mydataRDD = dataRDD.map(record => (record * record)).coalesce(8)
mydataRDD.getNumPartitions
val mydataRDD = dataRDD.map(record => (record * record)).coalesce(8, true)
mydataRDD.getNumPartitions
dataRDD.collect
dataRDD.getNumPartitions
val data = dataRDD.glom
data.getNumPartitions
data.first
data.count
dataRDD.count
dataRDD.id
dataRDD.name
dataRDD.setName("Data RDD")
dataRDD.name
val mydataRDD = dataRDD.map(record => (record * record)).coalesce(8, true).setName("Squared RDD")
mydataRDD.name
mydataRDD.isCheckpointed
dataRDD.isEmpty
val emptyRDD = dataRDD.filter(num => num > 200)
emptyRDD.isEmpty
dataRDD.min
dataRDD.max
dataRDD.count
dataRDD.mean
mydataRDD.name
mydataRDD.partition
mydataRDD.partitions
val mydataRDD = dataRDD.map(record => (record * record)).coalesce(2).setName("Squared RDD")
mydataRDD.partitions
val mydataRDD = dataRDD.map(record => (record * record)).coalesce(2).setName("Squared RDD")
dataRDD.partitioner
mydataRDD.toDebugString
val namesRDD = sc.makeRDD(Array("naga", "Siva", "Ravi"))
val agesRDD = sc.makeRDD(Array(30,31,33))
namesRDD.zip(agesRDD).collect
val agesRDD = sc.makeRDD(Array(30,31,33, 40))
namesRDD.zip(agesRDD).collect
val namesRDD = sc.makeRDD(Array("naga", "Siva", "Ravi", "Kiran"))
val agesRDD = sc.makeRDD(Array(30,31,33))
namesRDD.zip(agesRDD).collect
val names = Array("Naga", "Ravi")
val ages = Array(30, 33)
names.zip(ages).collect
names.zip(ages).foreach(println)
val ages = Array(30, 33, 40)
names.zip(ages).foreach(println)
val namesRDD = sc.makeRDD(Array("naga", "Siva", "Ravi", "Kiran", "Ravi", "ravi"))
namesRDD.collect
namesRDD.distinct
namesRDD.distinct.collect
namesRDD.map(name => name.toLowerCase()).distinct.collect
val stocksRDD = sc.textFile("/home/viswa/bigdata/stocks")
stocksRDD.getNumPartitions
stocksRDD.first
stocksRDD.map(record => record.split("\\t")(1).asInstanceOf[String].trim)
stocksRDD.map(record => record.split("\\t")(1).asInstanceOf[String].trim).first
stocksRDD.map(record => record.split("\\t")(1).asInstanceOf[String].trim).distinct.count
stocksRDD.map(record => record.split("\\t")(1).asInstanceOf[String].trim).distinct.collect
stocksRDD.map(record => record.split("\\t")(1).asInstanceOf[String].trim).distinct.foreach(println)
stocksRDD.map(record => record.split("\\t")(1).asInstanceOf[String].trim).distinct(true).foreach(println)
stocksRDD.map(record => record.split("\\t")(1).asInstanceOf[String].trim).distinct(1, true).foreach(println)
stocksRDD.map(record => record.split("\\t")(1).asInstanceOf[String].trim).distinct(1).foreach(println)
stocksRDD.map(record => record.split("\\t")(1).asInstanceOf[String].trim).distinct.getNumPartitions
stocksRDD.map(record => record.split("\\t")(1).asInstanceOf[String].trim).distinct(1).foreach(println)
dataRDD.collect
val evenRDD = dataRDD.filter(num => num % 2 == 0)
evenRDD.collect
val numRDD = sc.parallelize(Array(1,2,3,4,5))
numRDD.aggregate(_ + _)
numRDD.aggregate(0)(Math.add(_, _), Math.add(_, _))
numRDD.aggregate(0)(math.add(_, _), math.add(_, _))
numRDD.aggregate(0)(math.sum(_, _), math.sum(_, _))
numRDD.aggregate(0)(math.min(_, _), math.min(_, _))
numRDD.aggregate(0)(math.min(_, _), math.min(_, _)).collect
numRDD.aggregate(0)(math.min(_, _), math.min(_, _))
numRDD.aggregate(0)(math.max(_, _), math.max(_, _))
numRDD.aggregate(0)((a, b) => math.max(a, b), math.max(_, _))
numRDD.aggregate(0)((a, b) => math.max(a, b), (a,b) =>  math.max(a, b))
def add(a:Int, b:Int) : Int = (a + b)
add(10, 20)
numRDD.aggregate(0)((a, b) => add(a, b), (a,b) =>  add(a, b))
val numRDD = sc.parallelize(Array(1,2,3,4,5))
val sqrRDD = numRDD.map(record => record * record)
sqrRDD.collect
val sqrRDD = numRDD.map(record => record * record).cache()
sqrRDD.collect
sqrRDD.top(2)
sqrRDD.first(
)
sqrRDD.unpersist
sqrRDD.unpersist()
val crtRDD = numRDD.cartesian(sqrRDD)
crtRDD.collect
val crtRDD = numRDD.zip(sqrRDD)
crtRDD.collect
val zipRDD = numRDD.zip(sqrRDD)
sc.setCheckpointDir("/home/viswa/bigdata/checkpoint")
zipRDD.checkpoint()
zipRDD.count
zipRDD.getNumPartitions
zipRDD.coalesce(1).getNumPartitions
numRDD.collect
numRDD.countByValue()
val numRDD = sc.parallelize(Array(1,2,3,4,5,2,3,4,2,3))
numRDD.countByValue()
zipRDD.dependencies
numRDD.distinct()res28: Seq[org.apache.spark.Dependency[_]] = List(org.apache
numRDD.distinct().collect
val text = sc.textFile("/home/viswa/bigdata/spark-2.3.0/LICENSE")
text.first
val words = text.flatMap(line => line.split("\\s"))
words.collect
val words = text.flatMap(line => line.split("\\s")).filter(word => word.length > 3)
words.collect
val words = text.flatMap(line => line.split("\\s")).filter(word => word.length > 3).map(record => record.toLowerCase())
words.collect
val words = text.flatMap(line => line.split("\\s")).filter(word => word.length > 3).map(record => record.toLowerCase()).countByValue()
sqrRDD.collect
sqrRDD.fold(0)((a, b) => (a + b))
sqrRDD.reduce((a, b) => (a + b))
 val alph = sc.parallelize(Array('n','a','g','a'))
alph.fold(new StringBuilder())( (sb, a) => (sb.appen(a))
)
alph.fold(StringBuilder())( (sb, a) => (sb.append(a)))
alph.fold(new StringBuilder())( (sb, a) => (sb.append(a)))
alph.fold(new StringBuilder)( (sb, a) => (sb append a ))
alph.fold(new StringBuilder)((sb, a) => (sb append a ))
alph.fold(new StringBuilder){(sb, a) => (sb append a })
alph.fold(new StringBuilder)({(sb, a) => (sb append a })
 val alph = Array('n','a','g','a')
alph.fold(new StringBuilder)((sb, a) => (sb append a ))
alph.fold(new StringBuilder)((sb, a) => (sb append a )).toString
alph.foldLeft(new StringBuilder){ (sb, s) => sb append s }.toString
 val alph = sc.parallelize(Array('n','a','g','a'))
alph.fold(new StringBuilder){ (sb, s) => sb append s }.toString
alph.foldLeft(new StringBuilder){ (sb, s) => sb append s }.toString
sqrRDD.isCheckpointed
zipRDD.isCheckpointed
zipRDD.getCheckpointFile
zipRDD.getStorageLevel
zipRDD.getStorageLevel.description
zipRDD.cache()
zipRDD.getStorageLevel.description
zipRDD.persist(StorageLevel)
import org.apache.spark.storage.StorageLevel
zipRDD.persist(StorageLevel.DISK_ONLY)
zipRDD.unpersist
zipRDD.unpersist()
zipRDD.persist(StorageLevel.DISK_ONLY)
zipRDD.getStorageLevel.description
zipRDD.unpersist()
zipRDD.persist(StorageLevel.DISK_ONLY_2)
zipRDD.getStorageLevel.description
zipRDD.unpersist()
zipRDD.persist(StorageLevel.MEMORY_ONLY)
zipRDD.getStorageLevel.description
zipRDD.unpersist()
zipRDD.persist(StorageLevel.OFF_HEAP)
zipRDD.getStorageLevel.description
zipRDD.unpersist()
numRDD.collect
numRDD.groupBy(record => record)
numRDD.groupBy(record => record).collect
def add(record:(Int, Iterable[Int])) : (Int, Int) = {
val sum = record._2.reduce((a,b) => (a + b))
(record._1, sum)
}
numRDD.groupBy(record => record).map(add)
numRDD.groupBy(record => record).map(add).collect
numRDD.id
zipRDD.id
numRDD.collect
val dataRDD = sc.makeRDD(Array(3,4,5))
numRDD.intersection(dataRDD)
numRDD.intersection(dataRDD).collect
numRDD.intersection(dataRDD).getNumPartitions
numRDD.intersection(dataRDD, 1).getNumPartitions
numRDD.keyBy(record => record)
numRDD.keyBy(record => record).collect
val stocks = sc.textFile("/home/viswa/bigdata/stocks")
stocks.first
stocks.keyBy(record => record.split("\\t")(1))
stocks.keyBy(record => record.split("\\t")(1)).collect
numRDD.localCheckpoint
numRDD.collect
numRDD.min
numRDD.max
numRDD.partitioner
numRDD.groupBy(record => record).map(add).collect
numRDD.groupBy(record => record).map(add).partitioner
numRDD.groupBy(record => record).map(add).partitions
numRDD.collect
numRDD.pipe("wc -l")
numRDD.pipe("wc -l").collect
numRDD.pipe("uniq").collect
numRDD.pipe("unique").collect
numRDD.pipe("uniq").collect
val rdds = numRDD.randomSplit(Array(4.0, 4.0), 1)
rdds.count
rdds.count()
rdds
rdds(0)
rdds(0).foreach(println)
rdds(1).foreach(println)
rdds(2).foreach(println)
val rdds = numRDD.randomSplit(Array(4.0, 4.0), 1)
numRDD.collect
val numRDD = sc.range(1 to 1000)
val numRDD = sc.range(1, 1000, 1)
numRDD.count
numRDD.sample(true, 10, 1)
numRDD.sample(true, 10, 1).count
numRDD.sample(true, 0.1, 1).count
numRDD.collect
numRDD.count
val numRDD = sc.parallelize(Array(1,2,3,4,5,2,3,4,2,3))
numRDD.collect
numRDD.sortBy(record => record).collect
numRDD.sortBy().collect
numRDD.sortBy(record => record).collect
numRDD.subtract(dataRDD).collect
numRDD.subtract(dataRDD).toLocalIterator
numRDD.subtract(dataRDD).toLocalIterator.count
val a = numRDD.subtract(dataRDD).toLocalIterator.count
val a = numRDD.subtract(dataRDD).toLocalIterator
a.toList
numRDD.collect
numRDD.reduce((a,b) => ( a + b))
numRDD.treeReduce((a,b) => ( a + b))
numRDD.treeReduce((a,b) => ( a + b), 1)
numRDD.treeReduce((a,b) => ( a + b), 2)
numRDD.collect
dataRDD.collect
numRDD.union(dataRDD).collect
val stocks = sc.textFile("/home/viswa/bigdata/stocks")
val projRDD = stocks.map(record => {
val cols = record.split("\\t")
(cols(1), cols(7).toLong)
}
)
def getSV(record:String) : Option[(String, Long)] = {
try{
val cols = record.split("\\t")
Some(cols(1), cols(7).toLong)
}catch{
case _:Exception => (record, 99999999L) 
}
def getSV(record:String) : Option[(String, Long)] = {
try{
val cols = record.split("\\t")
Some(cols(1), cols(7).toLong)
}catch{
case _:Exception => None
}
val projRDD = stocks.map(getSV)
val projRDD = stocks.map(getSV).filter(record => record.nonEmpty).map(record => record.get)
projRDD.first
val grpRDD = projRDD.groupByKey()
grpRDD.first
grpRDD.collect
def agg(record:(String, Iterable[Long]) : (String, Long) = {
def agg(record:(String, Iterable[Long])) : (String, Long) = {
val sum = record._2.reduce((a,b) => (a + b))
(record._1, sum)
}
grpRDD.map(agg).collect
projRDD.reduceByKey((a,b) => (a + b))
projRDD.reduceByKey((a,b) => (a + b)).collect
projRDD.toDebugString
projRDD.keys
projRDD.keys().collect
projRDD.keys.collect
projRDD.lookup("CLI")
projRDD.lookup("CLI").reduce((a, b) => (a + b))
projRDD.values
val factor = 10
projRDD.first
projRDD.map(record => (record._1, record._2 * factor))
projRDD.map(record => (record._1, record._2 * factor)).collect
projRDD.map(record => (record._1, record._2 * factor)).getNumPartitions
val factor = sc.broadcast(10)
factor
factor.id
factor.value
factor.toString
factor.destroy
factor.value
val factor = sc.broadcast(10)
factor.value
projRDD.map(record => (record._1, record._2 * factor.value)).collect
val sum = sc.accumulator(0)
val sum = sc.accumulator(0.0)
val sum = sc.accumulator(0)
numRDD.getNumPartitions
numRDD.foreach(record => sum.add(record))
sum.id
sum.value
sum.merge
sum.zero
sum.value
sum.setValue(0)
sum.value
numRDD.foreach(record => sum.add(record))
sum.value
numRDD.collect
var counter = 0
numRDD.foreach(num => counter + num)
counter
numRDD.foreach(num => {
counter = counter + num
println(counter)
})
counter
numRDD.foreach(num => counter + num)
counter
spark
:imports
val people = Seq((), (), (), ()mport org.apache.spark.SparkContext._ (69 terms, 1 are implicit))
 2) import spark.implicits._       (1 types, 67 terms, 37 are implicit)
 3) import spark.sql               (1 terms)
val people = Seq(("naga", 30, "Bangalore"), ("hari", 34, "mysore"), ("ravi", 33, "ooty"), ("raju", 25, "vellore"))
val peopleDF = people.toDF()
peopleDF.printSchema
val peopleDF = people.toDF("name", "age", "place")
val peopleDF = people.toDF("name", "age", "place", "A")
val peopleDF = people.toDF("name", "age", "place")
peopleDF.printSchema
peopleDF.show
peopleDF.select("name")
peopleDF.select("name").show
peopleDF.select("name").distinct
peopleDF.select("name").distinct.show
peopleDF.createOrReplaceTempView("people")
sql("select * from people")
sql("select * from people").show
sql("select * from people where age > 30").show
sql("select * from people where age > 30").select("name").show
peopleDF.show
val peopleRDD = peopleDF.rdd
peopleRDD.foreach(row => println(row))
peopleRDD.foreach(row => println(row.getString(2)))
people
val pRDD = sc.parallelize(people)
val pDF = pRDD.toDF("name", "age", "place")
pDF.show
val stocks = sc.textFile("/home/viswa/bigdata/stocks")
val tableRDD = stocks.map(record => {
val cols = record.split("\\t")
(cols(0), cols(1), cols(2), cols(3).toDouble,cols(4).toDouble, cols(5).toDouble,cols(6).toDouble, cols(7).toLong, cols(8).toDouble)
}
)
tableRDD.first
val schema = StructType(List(StructField("market", StringType, true))
)
import org.apache.spark.sql.types._
val schema = StructType(List(StructField("market", StringType, true))
)
val schema = StructType(List(StructField("market", StringType, true), StructField("stock", StringType, false), StructField("date", StringType, false), StructField("open", DoubleType, false), StructField("high", DoubleType, true), StructField("low", DoubleType, true), StructField("close", DoubleType, true), StructField("volume", LongType, true), StructField("adj_close", DoubleType, true)))
schema
val stocksDF = spark.createDataFrame(tableRDD, schema)
val tableRDD = stocks.map(record => {
val cols = record.split("\\t")
Row(cols(0), cols(1), cols(2), cols(3).toDouble,cols(4).toDouble, cols(5).toDouble,cols(6).toDouble, cols(7).toLong, cols(8).toDouble)
}
)
import org.apache.spark.sql.Row
val tableRDD = stocks.map(record => {
val cols = record.split("\\t")
Row(cols(0), cols(1), cols(2), cols(3).toDouble,cols(4).toDouble, cols(5).toDouble,cols(6).toDouble, cols(7).toLong, cols(8).toDouble)
}
)
tableRDD.first
tableRDD.toDF()
val stocksDF = spark.createDataFrame(tableRDD, schema)
stocksDF.printSchema
stocksDF.head
stocksDF.take(10)
stocksDF.take(10).foreach(row => println(row)
)
stocksDF.take(10).foreach(row => println(row.getString(1)))
case Rating(userId:String, itemId:String, score:Double, rateTime:Long)
case class Rating(userId:String, itemId:String, score:Double, rateTime:Long)
val movieRDD = sc.textFile("/home/viswa/bigdata/ml-100k/u.data")
val ratingRDD = movieRDD.map(item => {
val cols = item.trim().split("\\t")
Rating(cols(0), cols(1), cols(2).toDouble, cols(3).toLong)
}
)
val ratingDF = ratingRDD.toDF()
ratingDF.printSchema
ratingDF.show
ratingDF.show(10)
ratingDF.createOrReplaceTempView("movie")
spark.sql("select avg(score) from movie group userId")
ratingDF.printSchema
spark.sql("select userId, avg(score) from movie group userId")
spark.sql("select userId, avg(score) from movie group by userId")
spark.sql("select userId, avg(score) as avgScorefrom movie group by userId")
spark.sql("select userId, avg(score) as avgScore from movie group by userId") 
spark.sql("select userId, avg(score) as avgScore from movie group by userId").show
spark.sql("select itemId, avg(score) as avgScore from movie group by itemId") 
spark.sql("select itemId, avg(score) as avgScore from movie group by itemId").show
ratingDF.groupBy("userId")
ratingDF.groupBy("userId").agg(avg("score"))
ratingDF.groupBy("userId").agg(avg("score")).show
ratingDF.groupBy("itemId").agg(avg("score")).show
val person = spark.read.json("/home/viswa/bigdata/people.json")
person.printSchema
person.show
val stocks = spark.read.csv("/home/viswa/bigdata/stocks.csv")
stocks.head
val stocks = spark.read.csv("/home/viswa/bigdata/stocks.csv")
stocks.printSchema
val stocks = spark.read.csv("/home/viswa/bigdata/stocks.csv")
val stocksDF = stocks.toDF("market", "stock", "date", "open", "high", "low", "close", "volume", "adj_close")
stocksDF.printSchema
val myDF = stocksDF.map(row => Row(row.getString(0), row.getString(1), row.getString(2), row.getString(3).toDouble, row.getString(4).toDouble, row.getString(5).toDouble, row.getString(6).toDouble, row.getSting(7).toLong, row.getString(8).toDouble)
)
val myDF = stocksDF.map(row => Row(row.getString(0), row.getString(1), row.getString(2), row.getString(3).toDouble, row.getString(4).toDouble, row.getString(5).toDouble, row.getString(6).toDouble, row.getString(7).toLong, row.getString(8).toDouble))
val myDF = stocksDF.map(row => Row(row.getString(0), row.getString(1), row.getString(2), row.getString(3).toDouble, row.getString(4).toDouble, row.getString(5).toDouble, row.getString(6).toDouble, row.getString(7).toLong)
)
val myDF = stocksDF.map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3).toDouble, row.getString(4).toDouble, row.getString(5).toDouble, row.getString(6).toDouble, row.getString(7).toLong, row.getString(8).toDouble))
myDF.show
case class Stock(market:String, stock:String, date:String, open:Double, high:Double, low:Double, close:Double, volume:Long, adj_close:Double)
myDF.show
myDFRow
myDF
val stDF = myDF.as[Stock]
myDF.show
val stDF = myDF.toDF()
stDF.show
stDF.printSchema
val myDF = stocksDF.map(row => Row(row.getString(0), row.getString(1), row.getString(2), row.getString(3).toDouble, row.getString(4).toDouble, row.getString(5).toDouble, row.getString(6).toDouble, row.getString(7).toLong, row.getString(8).toDouble))
val stDF = myDF.as[Stock]
val myDF = stocksDF.map(row => Stock(row.getString(0), row.getString(1), row.getString(2), row.getString(3).toDouble, row.getString(4).toDouble, row.getString(5).toDouble, row.getString(6).toDouble, row.getString(7).toLong, row.getString(8).toDouble))
myDF.show
myDF.as[Row]
val myDF = stocksDF.map(row => Stock(row.getString(0), row.getString(1), row.getString(2), row.getString(3).toDouble, row.getString(4).toDouble, row.getString(5).toDouble, row.getString(6).toDouble, row.getString(7).toLong, row.getString(8).toDouble))
myDF.select("stock", "volume")
myDF.select("stock", "volume").show
myDF.select("stock", "volume").groupBy("stock").agg(sum("volume")).show
myDF.select("stock", "volume").groupByKey().agg(sum("volume")).show
myDF.select("stock", "volume").groupBy("stock").agg(sum("volume")).show
myDF.select("stock", "volume").groupBy("stock").agg(sum("volume") as "aggvolume").orderBy(
)
myDF.select("stock", "volume").groupBy("stock").agg(sum("volume") as "aggvolume").orderBy("aggvolume")
myDF.select("stock", "volume").groupBy("stock").agg(sum("volume") as "aggvolume").orderBy("aggvolume").show
myDF.select("stock", "volume").groupBy("stock").agg(sum("volume") as "aggvolume").orderBy("aggvolume", desc).show
myDF.select("stock", "volume").groupBy("stock").agg(sum("volume") as "aggvolume").orderBy("aggvolume").show
val stock = spark.read.format("csv").option("header", true)
val stock = spark.read.format("csv").option("header", true).load()
val stock = spark.read.format("csv").option("header", true).option("inferSchema", "true").load("/home/viswa/bigdata/stocks.csv")
stock.printSchema
val stock = spark.read.format("csv").option("header", true).load("/home/viswa/bigdata/stocks.csv")
stock.printSchema
val stock = spark.read.format("csv").option("header", true).option("inferSchema", "true").load("/home/viswa/bigdata/stocks.csv")
stock.printSchema
stock.show
:q
val dataDF = Seq((1, "naga", 30)(2, "hari", 40)(3, "ravi", 35))
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),(3, "ravi", 35))
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),(3, "ravi", 35)).toDF()
dataDF.printSchema
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),(3, "ravi", 35)).toDF("sno", "name", "age")
dataDF.printSchema
case class Person(sno:Int, name:String, age:Int)
val dataDF = Seq(Person(1, "naga", 30),Person(2, "hari", 40),Person(3, "ravi", 35)).toDS()
val dataDS = Seq(Person(1, "naga", 30),Person(2, "hari", 40),Person(3, "ravi", 35)).toDS()
dataDS.foreach(person => person.name)
dataDS.foreach(person => println(person.name))
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),(3, "ravi", 35)).toDF("sno", "name", "age")
dataDF.foreach(row => println(row.geString(2)))
dataDF.foreach(row => println(row.getString(2)))
dataDF.foreach(row => println(row.getString(1)))
val dataDF = Seq(Person(1, "naga", 30),Person(2, "hari", 40),Person(3, "ravi", "35")).toDS()
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),(3, "ravi", "35")).toDF("sno", "name", "age")
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),(3, "ravi", 35)).toDF("sno", "name", "age")
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),("3", "ravi", 35)).toDF("sno", "name", "age")
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),(3, "ravi", )).toDF("sno", "name", "age")
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),("3", "ravi", 35)).toDF("sno", "name", "age")
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),(3, "ravi", )).toDF("sno", "name", "age")
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),(3, "ravi", 34)).toDF("sno", "name", "age")
val dataDF = Seq(("1", "naga", 30),(2, "hari", 40),(3, "ravi", 34)).toDF("sno", "name", "age")
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),(3, "", 34)).toDF("sno", "name", "age")
dataDF.show
val dataDF = Seq(Person(1, "naga", 30),Person(2, "hari", 40),Person(3, "ravi", "35")).toDS()
val dataDF = Seq((1, "naga", 30),(2, "hari", 40),("3", "ravi", 35)).toDF("sno", "name", "age")
:q
spark
:imports
help
:help
:imports
val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
val words = lines.map(row => row.getString(0).split(" "))
val words = lines.flatMap(row => row.getString(0).split(" "))
val words = lines.as[String].flatMap(row => row.getString(0).split(" ")).
)
val words = lines.as[String].flatMap(row => row.split(" "))
val words = lines.as[String].flatMap(row => row.split(" ")).groupBy("value").count()
words.show
val query = wordCounts.writeStream.outputMode("complete").format("console").start()
val query = words.writeStream.outputMode("complete").format("console").start()
query.awaitTermination
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
val data = Seq(
  (0.0, Vectors.dense(0.5, 10.0)),
  (0.0, Vectors.dense(1.5, 20.0)),
  (1.0, Vectors.dense(1.5, 30.0)),
  (0.0, Vectors.dense(3.5, 30.0)),
  (0.0, Vectors.dense(3.5, 40.0)),
  (1.0, Vectors.dense(3.5, 40.0))
)
val df = data.toDF("label", "features")
val chi = ChiSquareTest.test(df, "features", "label").head
println(s"pValues = ${chi.getAs[Vector](0)}")
println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
println(s"statistics ${chi.getAs[Vector](2)}")
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
// Create a local StreamingContext with two working thread and batch interval of 1 second.
// The master requires 2 cores to prevent a starvation scenario.
val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
val ssc = new StreamingContext(conf, Seconds(1))
exit
q!
quit
ssc
sc.
;
val ssc = new StreamingContext(sc, Seconds(1))
import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(1))
sc.setLogLevel("WARN")
val ssc = new StreamingContext(sc, Seconds(1))
cls
clearscreen 
val lines = ssc.socketTextStream("localhost", 9999)
ssc.awaitTermination() 
val ssc = new StreamingContext(sc, Seconds(1))
import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(1))
val lines = ssc.socketTextStream("localhost", 9999)
ssc.start() 
val words = lines.flatMap(_.split(" "))
val lines = ssc.socketTextStream("localhost", 9999)
import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(1))
val lines=ssc.soketTextStream("localhost",9999)
val lines=ssc.socketTextStream("localhost",9999)
val words=lines.map(lines=>lines.split(")).map(word=>(words,1))
val words=lines.map(lines=>lines.split(" ")).map(word=>(words,1))
val words=lines.map(lines=>lines.split(" ")).map(word=>(word,1))
val words=lines.map(lines=>lines.split(" ")).map(word=>(word,1)).groupByKey((a,b)=>a+b)
val words=lines.map(lines=>lines.split(" ")).map(word=>(word,1)).groupByKey(_+_)
val words=lines.map(lines=>lines.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
val words=lines.map(lines=>lines.split(" ")).map(word=>(word,1)).groupByKey((a,b)=>(a+b))
val words=lines.map(lines=>lines.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
ssc.start()
val words=lines.map(lines=>lines.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(1))
val lines=ssc.socketTextStream("localhost",9999)
val words=lines.map(lines=>lines.split(" ")).map(word=>(word,1)).reduceByKey(_+_).show()
val words=lines.map(lines=>lines.split(" ")).map(word=>(word,1)).reduceByKey(_+_).collect()
val words=lines.map(lines=>lines.split(" ")).map(word=>(word,1)).reduceByKey(_+_).print()
ssc.start()
val words=lines.map(lines=>lines.split(" ")).map(word=>(word,1)).groupByKey(_+_)
ssc.awaitTermination()xit
sc.textfil();
pwd
history
val a = numRDD.subtract(dataRDD).toLocalIterator.count
his
sc.textfil();
!!
!
;
!history
!:
def history = scala.io.Source.fromFile(System.getProperty("user.home") + "/.scala_history").foreach(print)
history
def history = scala.io.Source.fromFile(System.getProperty("user.home") + "/.scala_history").foreach(print)
history
