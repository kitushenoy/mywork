'''
Description: Program to calculate quantile using hive
Author : Kirthi Shanbhag
Date: June 1 2017
'''

from pyspark import SparkContext, SparkConf
from pyspark.storagelevel import StorageLevel
conf = (SparkConf()
                    .set("spark.rdd.compress","true")
                    .set("spark.storage.memoryFraction","1")
                    .set("spark.core.connection.ack.wait.timeout","600")
                    .set("spark.akka.frameSize","50")
                    .set("spark.driver.maxResultSize", "3g")
                    .set("spark.driver.extraJavaOptions","-Xms2048m -Xmx2048m")
                    .set("spark.yarn.executor.memoryOverhead","3000")
                    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                    .set("spark.kryoserializer.buffer.mb", "2000")
                    .set("spark.kryoserializer.buffer.max.mb", "2000"))
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
from pyspark.sql import HiveContext, SQLContext
from pyspark.sql.types import StructType,StructField,IntegerType,FloatType
import os
import os,re,argparse,inspect
import itertools
import numpy as np
import logging
import pandas as pd
import traceback,sys
import time,glob,shutil
import numpy as np
import sys,os
from datetime import datetime
from debugxx import debug1
from logging.handlers import RotatingFileHandler
'''
sqlContext = SQLContext(sc)
        logger.info(" No of Files to process - Single Die %s",len(fileNames))

        print " >>>> : Start process for 2 Die <<<< "
        ddLevel = "2Die"
        Die2Starttime = datetime.now()
        if debugMode == 0:
                rdd = sc.parallelize(fileNames[:2])
        else:
                rdd = sc.parallelize(fileNames)
        #print "...",fileNames[:1]
'''
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

def hdfsCSVUpload(strFh,loc):
        command = "hdfs dfs -put -f "+strFh+".csv "+loc
        print "command ",command
        a,b,c = exec_command(command)

from pyspark.sql import HiveContext
hive_context = HiveContext(sc)
peak = hive_context.table("xx.dd_4_backupjun28")
vec = [1-1e-2, 1-4.63e-3, 1-2.15e-3, 1-1e-3,1-4.63e-4, 1-2.15e-4, 1-1e-4,1-4.63e-5, 1-2.15e-5,1-1e-5,1-4.63e-6, 1-2.15e-6, 1-1e-6,1-4.63e-7, 1-2.15e-7, 1-1e-7]

peak.registerTempTable("dd_4_backupjun28")
quantile=hive_context.sql("select * from xx.dd_2").toPandas().quantile(vec)

print "quantile",quantile
tmpRawData = "/tmp/ICCMovingAvg/DieLevelRawData"
hdfsbasepath = "/ICCCharacterisation/MovingAvg"

tmpFolder = os.path.dirname(tmpRawData)
hdfsRawDataFolder= os.path.join(hdfsbasepath,"DieLevelRawData")
ddLevel ="2Die"
_strdf = os.path.join(tmpFolder,'Prob_mvAvg_Final'+ddLevel+time.strftime("%d_%m_%y"))
quantile.to_csv(_strdf+".csv", sep='\t')


if os.path.exists(_strdf+".csv"):
        print " Uploading quantile to hdfs ",_strdf+".csv"," to ",hdfsbasepath
        hdfsCSVUpload(_strdf,hdfsbasepath)

