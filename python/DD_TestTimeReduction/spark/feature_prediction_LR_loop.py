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

data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/kshanbhag/DD_SelectiveTest/data_ordered_cleaned_dropcorr1_1000k.csv')
#dataN=data.where(col("xyz_pcs_").isNotNull())
dataN=data.where(col('vzy_mv_').isNotNull())
dataN1=dataN.select([col(c).cast("double") for c in dataN.columns])

#test prediction based on its own prior test (1M)
lenT= len(dataN1.columns)
colList=dataN1.columns
print " Total length of columns is ",lenT
cnt = 0
for i in range(2,lenT):
        cnt+=1
        colN=[]
        target_test = colList[i]
        colN = [colList[i]]
        newCol=[x for i,x in enumerate(colList) if x != colN]
        colN.extend(newCol)
                                                       
	data_1=dataN1.select(colN)

        from pyspark.mllib.linalg import Vectors
        from pyspark.mllib.regression import LabeledPoint

        data_1=data_1.fillna(0)
        transformed_df=data_1.rdd.map(lambda row: LabeledPoint(row[0], Vectors.dense(row[1:])))
        labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(transformed_df.toDF())

        # Automatically identify categorical features, and index them.
        # Set maxCategories so features with > 4 distinct values are treated as continuous.
        featureIndexer =VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(transformed_df.toDF())

        # Split the data into training and test sets (30% held out for testing)
        (train_data, test_data) = transformed_df.toDF().randomSplit([0.7, 0.3])

        print " ********** Start Linear Regression model on Spark : Processing column - > ",target_test
        # Train a GBT model.
        # Import `LinearRegression`
        from pyspark.ml.regression import LinearRegression

        # Initialize `lr`
        lr = LinearRegression(labelCol="label", maxIter=10, regParam=0.3, elasticNetParam=0.8)

        # Fit the data to the model
        linearModel = lr.fit(train_data)
        # Generate predictions
        predicted = linearModel.transform(test_data)

        # Extract the predictions and the "known" correct labels
        predictions = predicted.select("prediction").rdd.map(lambda x: x[0])
        labels = predicted.select("label").rdd.map(lambda x: x[0])

        # Zip `predictions` and `labels` into a list
        predictionAndLabel = predictions.zip(labels).collect()

        # Print out first 5 instances of `predictionAndLabel` 
        print predictionAndLabel[:5]
        # Coefficients for the model
        print linearModel.coefficients.toArray()
        print type(linearModel.coefficients.toArray())
        # Intercept for the model
        print linearModel.intercept

        # record the coeff table
        coeff_table_pd = pd.DataFrame(linearModel.coefficients.toArray(),columns=['coefficients'])
        print "coefffiecients in pandas dataframe",coeff_table_pd
        coeff_table_pd['abs_coef'] = coeff_table_pd['coefficients'].abs()
        coeff_table_pd.sort_values('abs_coef',ascending=False).to_csv("Data/featurePredict_output/" + target_test + ".csv")


        print "plot prediction graph"
        pred_pd=predicted.select("prediction", "label", "features").toPandas()
        test_response_pd = test_data.select('label').toPandas()
        g = (sns.jointplot(test_response_pd.iloc[:,0], pred_pd.iloc[:,0], kind="reg").set_axis_labels("real", "pred"))
        g.savefig("Data/featurePredict_output/" + target_test + "_test.png")
        # For testing purpose do it for the first 5 columns
        if cnt == 5: 
                break


end_time = datetime.now()
print "Start Time : ",start_time
print "End Time : ",end_time
print('Duration: {}'.format(end_time - start_time))
