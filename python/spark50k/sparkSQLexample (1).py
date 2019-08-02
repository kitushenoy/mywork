from pyspark.sql.functions import col, avg,mean
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SQLContext
conf = (SparkConf()
        .set("spark.rdd.compress","true")
        .set("spark.storage.memoryFraction","1")
        .set("spark.core.connection.ack.wait.timeout","600")
        .set("spark.akka.frameSize","50"))
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
hiveContext = HiveContext(sc)
sqlContext = SQLContext(sc)
# use this for all the data stored in hadoop
#Data_SparkDF=sqlContext.read.format("com.databricks.spark.avro").load("/xyz/pr/Data/tech//")
# use this for one date
Data_SparkDF=sqlContext.read.format("com.databricks.spark.avro").load("/zys/nt1.avro")
#print Data_SparkDF.printSchema()
kk1=Data_SparkDF.select(Data_SparkDF.cc.cast('float').alias('cc'))
#print kk1.show(kk1.count(),False)
#kk2=kk1.na.fill(0)
#kk3=kk2.limit(10)

ll=kk1.groupBy().agg(mean(kk1["cc"]).alias("mean")).collect()
isNull_p=kk1.where(col("cc").isNull()).collect()
print " total null found",len(isNull_p)
print "Mean for the given column is",ll

