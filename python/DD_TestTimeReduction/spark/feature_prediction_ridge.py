'''

Note: this may not be complete and working code. I was just trying my hands on; on ridge regression approach
Syntax to run this spark code : 
#spark-submit --master local --executor-memory 6G --executor-cores 4 --driver-cores 3 --driver-memory 3G --packages com.databricks:spark-csv_2.11:1.2.0 --jars spark-avro_2.11-3.0.0.jar <filename>


'''

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

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
from pyspark.mllib.linalg import DenseVector
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

import pandas as pd
import sys
from pyspark.sql.types import StructType,StructField,IntegerType,FloatType,StringType
from pyspark.ml.regression import *
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from datetime import datetime
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.mllib.regression import RidgeRegressionModel,RidgeRegressionWithSGD
from pyspark.mllib.evaluation import RegressionMetrics
start_time = datetime.now()

# Load and parse the data
def parsePoint(line):
    #values = line.split()
    return LabeledPoint(float(line[0]),DenseVector([float(x.split(':')[1]) for x in line[1:]]))
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
data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/kshanbhag/DD_SelectiveTest/data_ordered_cleaned_dropcorr1_1M.csv')
dataN=data.where(col('testcol').isNotNull())
dataN1=dataN.select([col(c).cast("double") for c in dataN.columns])
colN=['testcol']
colList=dataN1.columns
newCol=[x for i,x in enumerate(colList) if x != 'testcol']
colN.extend(newCol)
data_1=dataN1.select(colN)
#data_predictor=dataN.select([c for c in df.columns if c not in {'testcol'}])
#data_Response=dataN.select('testcol')
#mask = data[target_test].isna()  # mask out the rows where target test is missing
#indices = [i for i,x in enumerate(colList) if x == 'vf_mode_mv_']
#idx= indices[0]
#features = colList[idx:]
#indx = [i for i,x in enumerate(colList) if x == 'testcol']

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

data_1=data_1.fillna(0)
transformed_df=data_1.rdd.map(lambda row: LabeledPoint(row[0], Vectors.dense(row[1:])))
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(transformed_df.toDF())

# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(transformed_df.toDF())

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = transformed_df.toDF().randomSplit([0.7, 0.3])

print " ********** Start Ridge Regression"
from pyspark.mllib.regression import LabeledPoint
'''
lrm = RidgeRegressionWithSGD.train(transformed_df, iterations=10,
     initialWeights=array([1.0]))
abs(lrm.predict(np.array([0.0])) - 0) < 0.5
abs(lrm.predict(np.array([1.0])) - 1) < 0.5
abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
abs(lrm.predict(sc.parallelize([[1.0]])).collect()[0] - 1) < 0.5
import os, tempfile
path = tempfile.mkdtemp()
lrm.save(sc, path)
sameModel = RidgeRegressionModel.load(sc, path)
abs(sameModel.predict(np.array([0.0])) - 0) < 0.5
abs(sameModel.predict(np.array([1.0])) - 1) < 0.5
abs(sameModel.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
from shutil import rmtree
try:
        rmtree(path)
except:
        pass
data = [
     LabeledPoint(0.0, SparseVector(1, {0: 0.0})),
     LabeledPoint(1.0, SparseVector(1, {0: 1.0})),
     LabeledPoint(3.0, SparseVector(1, {0: 2.0})),
     LabeledPoint(2.0, SparseVector(1, {0: 3.0}))
 ]
lrm = LinearRegressionWithSGD.train(sc.parallelize(data), iterations=10,
     initialWeights=array([1.0]))
abs(lrm.predict(np.array([0.0])) - 0) < 0.5
abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
'''
#rdrg= RidgeRegressionModel.train(trainingData)
rdrgsgd = RidgeRegressionWithSGD.train(trainingData, iterations=10, step=1.0,regParam=0.01, miniBatchFraction=1.0, intercept=True,validateData=True)

#pred=rdrg.predict(testData)
predsgd=rdrgsgd.predict(testData)
                                               
