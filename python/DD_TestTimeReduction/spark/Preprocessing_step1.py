#spark-submit --master local --executor-memory 6G --executor-cores 4 --driver-cores 3 --driver-memory 3G --packages com.databricks:spark-csv_2.11:1.2.0 --jars spark-avro_2.11-3.0.0.jar

from pyspark import SparkContext, SparkConf  
from pyspark.sql import HiveContext, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, TimestampType
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat import Statistics
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors
from pyspark.storagelevel import StorageLevel
import pandas as pd
import sys
from pyspark.sql.types import StructType,StructField,IntegerType,FloatType,StringType
from datetime import datetime
start_time = datetime.now()
conf = (SparkConf()
        .set("spark.rdd.compress","true")
        .set("spark.storage.memoryFraction","1")
        .set("spark.core.connection.ack.wait.timeout","600")
	.set("spark.yarn.executor.memoryOverhead","3000")
        .set("spark.akka.frameSize","50"))    
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
hiveContext = HiveContext(sc)
sqlContext = SQLContext(sc)
#data = hiveContext.sql("select * from xx.xx where xxpgm = 'xxA.EXE' and product = 'xyz' and dsstartdate LIKE '2017/06%' limit 1000")
#hive -e "set hive.cli.print.header = true; select * from select * from xx.xx where xxpgm = 'xxA.EXE' and product = 'xyz' and dsstartdate LIKE '2017/06%' limit 1000 " | sed 's/[\t]/,/g'  > /home/bdingstprd/beibei//ZBC52ZZAEXE_520_June.csv
data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/kshanbhag/xx_520_june_1000.csv')
#data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/kshanbhag/xxE_520_june_clean1000.csv')
print "Cols at the begining # ::", len(data.columns)
print " Re-order the data columns"
# reorder tests according to tests sequence
#TestOrder = pd.read_csv('Data/test_order_new.csv')
TestOrder = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/kshanbhag/DD_SelectiveTesT/test_order_new.csv')
testOrder=TestOrder.toPandas().values.flatten().tolist()
testOrdder= testOrder[10]
testOrder.append('waferid')
dataNew=data.select(testOrder)
dataNew=dataNew.select([col(c).cast("float") for c in dataNew.columns])
print "# of columns after re-ordering",len(dataNew.columns)

nonConsColumn = []
nConsColumn
ConsColumn = []
dataNew = dataNew.dropna(how='all')
print "# of columns after drop na ::", len(dataNew.columns)
cnt =0 
cntD=0
for col in dataNew.columns:
	cnt+=1
	#if dataN.select(col).rdd.distinct().count() == 1:
	if dataNew.select(col).distinct().count() == 1:
		print "Constant Column",col
		cntD+=1
        	dataNew=dataNew.drop(col)
		ConsColumn.append(col)
		
	else:
		nonConsColumn.append(col)

constcol = pd.DataFrame(ConsColumn, columns=["const_colummn"])
constcol.to_csv('Data/const_col.csv', index=False)
#schema = StructType([StructField("Const",StringType(),True)])
#df=sqlContext.createDataFrame(ConsColumn,schema)
print " #Column number after cleaning constant:", len(dataNew.columns)
print " Start cleaning null, nan,empty"
from pyspark.sql.functions import isnan, when, count, col
#dd=[when(col(c).isNotNull(), c).alias(c) for c in dataNew.columns]
#df_notNan = dataNew.select([when(col(c).isNotNull(),c).alias(c) for c in dataNew.columns])

print "#columns after cleaning null and nan",len(dataNew.columns)
nullCols= []
for c in dataNew.columns:
	if dataNew.select(col(c)).count() == 0:
		
		nullCols.append(c)
		while c in x: x.remove(c)
from pyspark.sql import DataFrame

print " Null columns identified are ",nullCols
'''
# we need not store the whole dataframe
dataNew=reduce(DataFrame.drop, nullCols, dataNew)
'''
print "no of null columns found ",len(nullCols)	

#now data is reordered, start data cleaning
all_missing_pd = pd.DataFrame(nullCols, columns=["all_null_empty_na_values"])
all_missing_pd.to_csv('Data/all_missing_520_may.csv', index=False)

print " All contant and missing value column names"
ConsColumn.extend(nullCols)
all_pd = pd.DataFrame(nonConsColumn, columns=["cleanedcolumns"])
all_pd.to_csv('Data/all_step1_520_june.csv', index=False)
print "#columns after cleaning null and nan",len(dataNew.columns)
#dataNew.write.format('com.databricks.spark.csv').mode('overwrite').save('/user/kshanbhag/xx_520_june_1000_clean.csv',header = 'true')

end_time = datetime.now()
print "Start Time : ",start_time
print "End Time : ",end_time
print('Duration:` {}'.format(end_time - start_time))
