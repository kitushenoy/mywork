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

data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/kshanbhag/DD_SelectiveTest/ZBC52ZZAEXE_520_june_1000.csv')
data=data.drop('original_file_name')
data=data.drop('avro_file_name')
dataN=data.select([col(c).cast("double") for c in data.columns])
print "Cols at the begining # ::", len(dataN.columns)

colList = data.columns
indices = [i for i,x in enumerate(colList) if x == 'vf_mode_mv_']
idx= indices[0]
#features = colList[idx:]
indx = [i for i,x in enumerate(colList) if x == 'bin_pcs_']
#response=colList[indx[0]]

APP_NAME = "Random Forest Example"
SPARK_URL = "local[*]"
RANDOM_SEED = 13579
TRAINING_DATA_RATIO = 0.7
RF_NUM_TREES = 200
RF_MAX_DEPTH = 500
RF_NUM_BINS = 200

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
transformed_df=dataN.map(lambda row: LabeledPoint(row[indx[0]], Vectors.dense(row[idx:])))
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(transformed_df.toDF())

labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(transformed_df.toDF())

(trainingData, testData) = transformed_df.toDF().randomSplit([0.7, 0.3])

rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures",numTrees=10)

labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                labels=labelIndexer.labels)


pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])


# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)
# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
accuracy=evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))
rfModel = model.stages[2]
print(rfModel)  # summary only
end_time = datetime.now()
print "Start Time : ",start_time
print "End Time : ",end_time
print('Duration: {}'.format(end_time - start_time))
                                                    
