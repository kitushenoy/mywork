'''
Description : This file is designed to find the correlation of the of parameters for cleaned dd data
author : Kirthi Shanbhag
Created on Sep 21 2017
'''
#spark-submit --master local --executor-memory 6G --executor-cores 4 --driver-cores 3 --driver-memory 3G --packages com.databricks:spark-csv_2.11:1.2.0 --jars spark-avro_2.11-3.0.0.jar

from pyspark import SparkContext, SparkConf  
from pyspark.sql import HiveContext, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, TimestampType,FloatType
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat import Statistics
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors
from pyspark.storagelevel import StorageLevel
import pandas as pd
import numpy as np
import sys
from pyspark.sql.types import StructType,StructField,IntegerType,FloatType,StringType
from pyspark.mllib.stat import Statistics
import matplotlib
matplotlib.use('Agg') # Must be before importing matplotlib.pyplot or pylab!
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("dark")
sns.set_context("talk")
from datetime import datetime
start_time = datetime.now()

def addnewcol(c):
	return c

def parse_interaction_with_key(line):
    return (np.array([x for x in line]).astype(float))
def exec_command(command):
        """
        Execute the command and return the exit status. this will make program to wait
        """
        exit_code = 1
        stdo = ''
        stde = ''
        from subprocess import Popen, PIPE
        try:
                pobj = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
                #pobj.wait()
                stdo, stde = pobj.communicate()
                exit_code = pobj.returncode
        except:
                print "Unexpected error at exec_command:", sys.exc_info()
                import platform
                s = traceback.format_exc()
                logStr = " exec command  error : error\n> stderr:\n%s\n" %s
                error = platform.node()+"-"+logStr
                return (1,error,"")
        return (exit_code, stdo, stde)
conf = (SparkConf()
        .set("spark.rdd.compress","true")
        .set("spark.storage.memoryFraction","1")
        .set("spark.core.connection.ack.wait.timeout","600")
        .set("spark.akka.frameSize","50"))    
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
hiveContext = HiveContext(sc)
sqlContext = SQLContext(sc)

data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/kshanbhag/DD_SelectiveTest/xx_520_june_1000.csv')
rr= open('Data/all_step1_520_june.csv').read().split()
colD=rr[1:]

data=data.drop('original_file_name')
data=data.drop('avro_file_name')
dataN=data.select([col(c).cast("float") for c in colD])
colList = dataN.columns
data_test = dataN.select(colList) # may need to delete the ['bin_pics_'] based on which data setyou are using
data_test= data_test.fillna(0)

vector_data=data_test.map(parse_interaction_with_key)
correlation_matrix = Statistics.corr(vector_data, method="spearman")

pd.set_option('display.max_columns', 50)
col_names=data_test.columns
corr_df = pd.DataFrame(correlation_matrix, 
                    index=col_names, 
                    columns=col_names)


sns.set_style("dark")
sns.set_context("talk")

corrLimit = 1 # threshold can be changes based on need
features = data.columns

picked_features = []
corrLimit = 1 # threshold can be changes based on need
features = data.columns

cntF=tt = 0
indexLn = len(corr_df.index)
cr_pd=pd.DataFrame([],columns=dataN.columns) # first since we already know the columns name, asign that
for n in range(indexLn):
	for ff in dataN.columns:

		if corr_df.loc[corr_df.index[n],ff] == 1.0:
			cr_pd.ix[corr_df.index[n],ff]=1.0 # dynamically assign index and value to given column name
			if corr_df.index[n] == ff:
				print " Correlating itself, hence eliminating "
			else:
				print " ******Correlation of value 1.0 found: [index %s, columnname %s] ****"%(corr_df.index[n],ff)
			tt+=1
		else:
			cntF+=1
			if ff in picked_features:
				pass
			else:
				picked_features.append(ff)	

#len(picked_features) #list of picked features
print " Writing panda dataframe of correlation 1 to filei:Data/CorrelationOf1_allcolumns.csv ",
cr_pd.to_csv('Data/CorrelationOf1_allcolumns.csv')
print " Uploading this fiel to hdfs"
command ="hdfs dfs -put -f Data/CorrelationOf1_allcolumns.csv /user/kshanbhag/DD_SelectiveTest/"
exec_command(command)
                    
print " Count of correlation with 1.0 found are",tt
print "picked Feature length - >",cntF
#adding "A" to bins to make it categorical for future classification
def add_A(string):
    return string + 'A'


#data['bin_pcs_'] = data['bin_pcs_'].apply(lambda x: add_A(str(x)))
data_drop_corr1 = data[picked_features]
#data_drop_corr1.to_csv('Data/data_ordered_cleaned_dropcorr1_1M.csv')
data_drop_corr1.write.format('com.databricks.spark.csv').mode('overwrite').save('/user/kshanbhag/DD_SelectiveTest/data_ordered_cleaned_dropcorr1_1M.csv',header = 'true')


print "...Saving pairwise correlation"
corr_df.to_csv('Data/pairwise_corr_520_june_clean1000.csv')
df = sqlContext.createDataFrame(corr_df)
df.write.format('com.databricks.spark.csv').mode('overwrite').save('/user/kshanbhag/DD_SelectiveTest/pairwise_corr_520_june_clean1000.csv',header = 'true')

print " Graphical rep"
# plot corr matrix
plt.figure()
sns.set(style="white")

# Generate a mask for the upper triangle
# Set up the matplotlib figure
f, ax = plt.subplots(figsize=(11, 9))

# Generate a custom diverging colormap
cmap = sns.diverging_palette(220, 10, as_cmap=True)
# Draw the heatmap with the mask and correct aspect ratio
cdf=corr_df.head(20)

sns.heatmap(cdf,linewidth=0)
plt.setp(ax.get_xticklabels(), rotation=30)
plt.savefig('heatmap.jpg')
command ="hdfs dfs -put -f heatmap.jpg /user/kshanbhag/DD_SelectiveTest/Graphs"
exec_command(command)
# rank top corr test
def get_redundant_pairs(df):
    pairs_to_drop = set()
    cols = df.columns
    for i in range(0, df.shape[1]):
        for j in range(0, i+1):
            pairs_to_drop.add((cols[i], cols[j]))
    return pairs_to_drop

def get_top_abs_correlations(df, n):
    au_corr = df.corr().abs().unstack()
    labels_to_drop = get_redundant_pairs(df)
    au_corr = au_corr.drop(labels=labels_to_drop).sort_values(ascending=False)
    return au_corr[0:n]

print("Top Absolute Correlations")
print(get_top_abs_correlations(corr_df, 100))
top_corr = get_top_abs_correlations(corr_df, 1000)
top_corr.to_csv("top_corr_1000.csv")

# visualize joint plot
df1 =data_test.select('iccs_moni_0_ua_','iccs_moni_1_ua_','bbk_slcprog_p_','wlleak_post_22_na_', 'vth_12pwl31_mh0_med_mv_', 'wlleak_post_33_na_')

col_names=df1.columns
df1_pd=df1.toPandas()

g = sns.jointplot(x="iccs_moni_0_ua_", y="iccs_moni_1_ua_", data=df1_pd,kind="reg")   
g.savefig("corr_1.jpg")
command ="hdfs dfs -put -f corr_1.jpg /user/kshanbhag/DD_SelectiveTest/Graphs"
exec_command(command)
g = sns.jointplot(x="bbk_slcprog_p_", y="wlleak_post_22_na_", data=df1_pd, kind="reg")
g.savefig("corr_2.jpg")
command ="hdfs dfs -put -f corr_2.jpg /user/kshanbhag/DD_SelectiveTest/Graphs"
exec_command(command)
g = sns.jointplot(x="vth_12pwl31_mh0_med_mv_", y="wlleak_post_33_na_", data=df1_pd,kind="reg")  
g.savefig("corr_3.jpg")
nd ="hdfs dfs -put -f corr_3.jpg /user/kshanbhag/DD_SelectiveTest/Graphs"
exec_command(command)



print " ****plot correlation distribution of two test by wafer ***"
data_test_wafer = data_test
df_w=data.select('waferid')
data_test_wafer = data_test_wafer.join(df_w) # add waferid colume
#print data_test_wafer.head()

# corr hist by wafer
#corr_hist = data_test_wafer.groupby('waferid').agg(corr('wlleak_post_22_na_', 'vth_12pwl31_mh0_med_mv_').alias('corr')).toPandas()
corr_hist = data_test_wafer.groupby('waferid').agg(corr('bbk_wlds0vth_3_u4_ev_c_pcs_','ur_crc_0ave_wl9s0__').alias('corr')).toPandas()
from collections import Counter
corr_hist['corr'].hist(by=corr_hist['waferid'],bins = 20, range = (0,1), edgecolor='black', linewidth=1.2,alpha = 0.75)
plt.xlabel("corr of bbk_wlds0vth_3_u4_ev_c_pcs_ and ur_crc_0ave_wl9s0__")
plt.ylabel("number of wafers")
plt.grid(True)
fig1 = plt.gcf()
print "Plotting histogram"
fig1.savefig('hist_wafer_01.jpg')
command ="hdfs dfs -put -f hist_wafer_01.jpg /user/kshanbhag/DD_SelectiveTest/Graphs"
exec_command(command)
print "*** plot correlation distribution of two test by sampling ****"
print " plot test distribution among different bin/category/passfail"

data_test_res=data_test.withColumn("Pass_Fail",when(data_test['bin_pcs_']>=170,'FAIL').otherwise('PASS'))

df_res=data_test_res.toPandas()
category = pd.read_csv('Data/category.csv')
data_test_res = pd.merge(df_res, category, on='bin_pcs_')

print "plotting box plots"

# box plot for test on each bin/catogorypass/fail
plt.subplots()
sns.boxplot(x="bin_pcs_", y=data_test_res.columns[8], data=data_test_res, palette="Blues") #can also add hue
plt.savefig('box1.jpg')
command ="hdfs dfs -put -f box1.jpg /user/kshanbhag/DD_SelectiveTest/Graphs"
exec_command(command)


sns.boxplot(x="TECategory", y=data_test_res.columns[25], data=data_test_res, palette="Blues")
plt.savefig('box2.jpg')
command ="hdfs dfs -put -f box2.jpg /user/kshanbhag/DD_SelectiveTest/Graphs"
exec_command(command)
sns.boxplot(x="Pass_Fail", y=data_test_res.columns[23], data=data_test_res, palette="Blues")
plt.savefig('box3.jpg')
command ="hdfs dfs -put -f box3.jpg /user/kshanbhag/DD_SelectiveTest/Graphs"
exec_command(command)
end_time = datetime.now()
print "Start Time : ",start_time
print "End Time : ",end_time
print('Duration: {}'.format(end_time - start_time))
