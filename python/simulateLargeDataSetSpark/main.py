#!/usr/anaconda/bin/python
# -*- coding: utf-8 -*-
'''
Created on Mon May 29 2017
@author: Kirthi Shanbhag
Description : This script is designed to calculate the peak current value for simulated values of 1 Die to (2/4/8/16) die
The script first creates the delay file> calculated the moving average for corresponding microSecond
#Syntax to run this program : 
spark-submit --master yarn --executor-memory 21G --executor-cores 4 --driver-cores 3 --driver-memory 2G --conf spark.yarn.executor.memoryOverhead=3000 /home/kshanbhag/xxxCharacteristics/Python/xxxmovingavg.py /CSV spark
# for debug mode uncomment debugmode = 0
# Reference Syntax:
hive command : 
CREATE EXTERNAL TABLE icc.Die_2_final(1microsec FLOAT,5microsec FLOAT,20microsec FLOAT)
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
insert into icc.Die_2_final select * from icc.Die_2

'''
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
from debugicc import debug1
from logging.handlers import RotatingFileHandler
#from reads3 import readS3File

cmd_path = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))

## this is to get path for common directory, assuming code is under data
filePath = os.path.realpath(__file__)
if 'GITdata' in filePath:
        rootDir = filePath[0: filePath.find('GITdata')]
        common_path = os.path.realpath( os.path.join(rootDir,'GITdata/data/COMMON/python'))
else:
        rootDir = filePath[0: filePath.find('data')]
        common_path = os.path.realpath( os.path.join(rootDir,'data/COMMON/python'))

## insert the paths
if cmd_path not in sys.path:
        sys.path.insert(1, cmd_path)
if common_path not in sys.path:
        sys.path.insert(1,common_path)

def s3connection(access_key,secret_key,bucket_name):
        try:
                import boto
                import boto.s3.connection
                conn = boto.connect_s3(
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key,
                port=80,
                host = 'hgdlake',
                is_secure=False,
                calling_format = boto.s3.connection.OrdinaryCallingFormat(),
                )
                b=conn.get_bucket(bucket_name)
                return b
        except:
                s = traceback.format_exc()
                logStr = "s3 donwload error : error\n> stderr:\n%s\n" %s
                print logStr
                raise

def readS3Keys(b,keyDir,debugMode):
        rs=b.list(keyDir)
        generatorFlag = 0
        fileNames = []
        keyCnt=0
        for kk in rs:
                generatorFlag = 1 # The list indeed returned something
                if keyCnt == 2 and int(debugMode) == 0:
                        break
                key = kk.name
                #key = b.get_key(key)
                # Skip X2 files
                if not "X2" in kk.name:
                        fileNames.append(key)
                        keyCnt+=1
        return fileNames

def readCSVFile(fContent):
        return np.array(fContent.replace("[","").replace("]","").decode('unicode_escape').encode('ascii','ignore').replace(","," ").split()).astype(float)

def readS3File(key,access_key,secret_key,bucket_name):
        b = s3connection(access_key,secret_key,bucket_name)
        key = b.get_key(key)
        data=[line.strip().split()[1] for line in key.get_contents_as_string().splitlines()]
        return np.array(data).astype(float)

def addTwoNumpyArray(a,b):
        return np.add(a,b).tolist()

def readFile(key):
        return np.array([line.strip().split()[1] for line in open(key).read().splitlines()]).astype(float)

def createComboOfData(combinationsize,samplesize,fContent):
        data = []
        #ll =[p for p in itertools.product(x, repeat=2)] permutation
        for subset in itertools.product(fContent,repeat=2):
                data.append(subset)
                if len(data) == samplesize:
                        break
        return data

def mvgC(ll,N):
        return map(lambda nn : np.amax(np.convolve(ll, np.ones((nn,))/nn)[(nn-1):]).tolist(),N)
        # another approach .. but not required for our code
	# import pandas as pd
	#PD= pd.Series(ll)
        #return map(lambda k: pd.rolling_mean(PD, k).dropna().max(),N)

def adjustDelay(a,b):
        return [a[len(a)-len(b):],b] if len(a) > len(b) else [a,b[len(b)-len(a):]]

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
        printHDFSResult(a,b,c)

def hdfsJPEGUpload(strFh,loc):

        command = "hdfs dfs -put -f "+strFh+".jpeg "+loc
        print " command hdfs :",command
        a,b,c = exec_command(command)
        printHDFSResult(a,b,c)

def printHDFSResult(a,b,c):

        if b!="":
                logging.info('stdout: %s' %b)
        if c!= "":
                logging.info('stderr: %s' %c)

# Create your own caterisean function; not used but leaving it for future reference 
def cartesianNM(arrays, out=None):
    '''
    Generate a cartesian product of input arrays.
    Parameters
    ----------
    arrays : list of array-like
        1-D arrays to form the cartesian product of.
    out : ndarray
        Array to place the cartesian product in.
    Returns
    -------
    out : ndarray
        2-D array of shape (M, len(arrays)) containing cartesian products
        formed of input arrays.
    Examples
    --------
    >>> cartesian(([1, 2, 3], [4, 5], [6, 7]))
    array([[1, 4, 6],
           [1, 4, 7],
           [1, 5, 6],
           [1, 5, 7],
           [2, 4, 6],
           [2, 4, 7],
           [2, 5, 6],
           [2, 5, 7],
           [3, 4, 6],
           [3, 4, 7],
           [3, 5, 6],
           [3, 5, 7]])
    '''

    arrays = [np.asarray(x) for x in arrays]
    dtype = arrays[0].dtype

    n = np.prod([x.size for x in arrays])
    if out is None:
        out = np.zeros([n, len(arrays)], dtype=dtype)

    m = n / arrays[0].size
    #print " m size is ",m
    #return m
    out[:,0] = np.repeat(arrays[0], m)
    if arrays[1:]:
        cartesianNM(arrays[1:], out=out[0:m,1:])
        for j in xrange(1, arrays[0].size):
            out[j*m:(j+1)*m,1:] = out[0:m,1:]
            #if len(out) == 1:
                #break
    return out

def main():

        ###############
        ## main code ##
        ###############


        print "======================================="
        parser = argparse.ArgumentParser(description='Program to calculate icc moving avergae',usage='example: time spark-submit --master yarn-client iccmovingavgV5.py xxx/US/INPUT/20170530/xxx_MLC_PROG/CSV spark/python --debugmode 0')
        parser.add_argument('keyDir', help='')
        parser.add_argument('-o','--outDir', help='')
        parser.add_argument('-s','--samplesize' ,help='file Size to split')
        parser.add_argument('-c','--combinationsize',help = 'no of combination size')
        parser.add_argument('-m','--debugmode',help='used this to handle few lines and 2-3 files')
        args = parser.parse_args()
        global debugMode
        global tmpFolder
        global tmpRawData

        if args.debugmode:
                if args.debugmode == "1":
                        debugMode = 1
                else:
                        debugMode = 0 # enabling making debug mode 
        else:
                debugMode = 1
        debugMode =0
        if debugMode == 0:
                collectSample = 10
        else:
                collectSample =2000
        if args.keyDir:
                keyDir = args.keyDir
        else:
                keyDir = "xxx/US/"
        if args.outDir:
                args.outDir =""
        else:
                outDir ="/home/kshanbhag/xxx/results"
        if args.samplesize:
                samplesize = int(args.samplesize)
        else:
                samplesize = 1000000
        if args.combinationsize:
                combinationsize = int(args.combinationsize)
        if debugMode == 0:
                tmpRawData = "/home/kshanbhag/DieLevelRawData"
                hdfsbasepath = "/user/kshanbhag/MovingAvg"
        else:
                tmpRawData = "/tmp/xxxMovingAvg/DieLevelRawData"
                hdfsbasepath = "/xxxCharacterisation/MovingAvg"
                                                                 
        tmpFolder = os.path.dirname(tmpRawData)
        hdfsRawDataFolder= os.path.join(hdfsbasepath,"DieLevelRawData")
        print " @@@ Creating tmp folder at %s @@@",tmpFolder
        if not os.path.exists(tmpRawData):
                os.makedirs(tmpRawData)
        logFile = os.path.join(tmpFolder,"iccmovingavg.log")
        logger = logging.getLogger("icclog")
        logging.basicConfig(filename=logFile,level=logging.DEBUG,format='[%(asctime)-15s %(levelname)s] %(message)s',)
        loggingByte=20000000
        loggingBackupCount =500
        handler = RotatingFileHandler(logFile, maxBytes=loggingByte,
                                  backupCount=loggingBackupCount)
        logger.addHandler(handler)
        if int(debugMode) == 0:
                stdoutHandler =logging.StreamHandler()
                logger.addHandler(stdoutHandler)
        logger.info(" Creating tmp folder at %s",tmpFolder)
        cleanStarttime = datetime.now()


        logger.info(">>>> CLEANUP STARTS <<<<")
        logger.info(" Start cleaning hdfs directories for raw files")
        command = "hdfs dfs -rm -r -skipTrash "+hdfsbasepath+"/*"
        a,b,c = exec_command(command)
        logger.info(">>>> CLEANUP END <<<<")
        cleanEndtime = datetime.now()
        logger.info('Durationi cleanup: {}'.format(cleanEndtime - cleanStarttime))
        sparkSubmitCmd = 'time spark-submit --master yarn-client '
        # Define variable initialisation
        newfContent = []
        arry = []
        arryF = []
        cmbN =0
        columns = ['DieType','Prob','1MicroSec','5MicroSec', '10MicroSec']
        #Note: we could create an empty DataFrame (with NaNs) simply by writing:
        print "debug Mode ",debugMode
        if debugMode != 0:
                # Read S3
                keyFile = open(os.path.join(common_path,'obj.key.txt'),'r')
                access_key = keyFile.readline().split('=')[1].strip()
                secret_key = keyFile.readline().split('=')[1].strip()
                keyFile.close()
                bucket_name = "test3-new-bucket"
                logger.info("Creating S3 connection")
                b = s3connection(access_key,secret_key,bucket_name)
                fileNames = readS3Keys(b,keyDir,debugMode)
        else:
                baseDir= "/home/kshanbhag/tmpkshanbhag/CodeNew/movingAvgRDD/"
                fileNames= [baseDir+"file1.csv",baseDir+"file2.csv",baseDir+"file3.csv",baseDir+"file4.csv"]
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
                    .set("spark.kryoserializer.buffer", '512'))
        sc = SparkContext(conf=conf)
        sc.setLogLevel('ERROR')
        from pyspark.sql import HiveContext, SQLContext
        from pyspark.sql.types import StructType,StructField,IntegerType,FloatType
        sqlContext = SQLContext(sc)
        logger.info(" No of Files to process - Single Die %s",len(fileNames))

        print " >>>> : Start process for 2 Die <<<< "
                                                                                             
        dieLevel = "2Die"
        Die2Starttime = datetime.now()
        if debugMode == 0:
                rdd = sc.parallelize(fileNames[:2])
        else:
                rdd = sc.parallelize(fileNames)
        #print "...",fileNames[:1]
        #print "type is",type(b.get_key(fileNames[0]).get_contents_as_string())
        #data =[line.strip().split()[1] for line in b.get_key(fileNames[0]).get_contents_as_string().splitlines()]
        #print "...",np.array(data).astype(float)
        if debugMode != 0:
                rdd1 = rdd.map(lambda x : readS3File(x,access_key,secret_key,bucket_name))
        else:
                rdd1 = rdd.map(lambda x : readFile(x))
        print rdd1.take(1)

        rdd2=rdd1.cartesian(rdd1)
        rdd3 = rdd2.map(lambda dd: adjustDelay(dd[0],dd[1]))
        rdd4=rdd3.map(lambda cc: addTwoNumpyArray(cc[0],cc[1]))
        #rdd4.cache()
        rdd4.persist(StorageLevel.MEMORY_AND_DISK)

        #N = (2,2,3)
        N = (20,100,400)
        rdd5= rdd4.map(lambda mvg:mvgC(mvg,N))
        # resample for 4 die
        #df=pd.DataFrame()
        schema = StructType([StructField("1MicroSec",FloatType(),True),
                     StructField("5MircroSec",FloatType(),True),
                     StructField("10MircroSec",FloatType(),True)])

        print " Converting rdd to df"
        df2=sqlContext.createDataFrame(rdd5,schema)
        #df2.write.mode(saveMode.Overwrite).parquet("/xxxCharacterisation/MovingAvg/.parquet")
        #df2.write.mode(SaveMode.Overwrite).parquet(os.path.join(hdfsRawDataFolder,"MaxMovingAverage2Die"))
        logger.info(" --- Saving the dataframe values to csv --- ")
        #df2.write.save(os.path.join(hdfsRawDataFolder,"MaxMovingAverage"+dieLevel+".parquet"), source="parquet", mode='overwrite',compression="gzip")

        mvgPD2Die=df2.toPandas()
        _strCommon = os.path.join(tmpFolder,'PeakCurrent_mvAvg'+'2Die_'+time.strftime("%d_%m_%y"))
        #_hdfsHist = "histogram"
        #_hdfsCsv = "csv"
        '''
        logger.info(" --- Plotting graphs (Histogram and saving to file/ upload to hdfs) ---" )
        import matplotlib.pyplot as plt
        mvgPD2Die.plot.hist()
        plt.savefig(_strCommon+'.jpeg',dpi=100, bbox_inches='tight')
        hdfsJPEGUpload(_strCommon,hdfsbasepath+"/"+os.path.basename(_strCommon+".jpeg"))
        '''

        # Quantile functions 
        columns = ['DieType','Prob','1MicroSec','5MicroSec', '10MicroSec']
        #Note: we could create an empty DataFrame (with NaNs) simply by writing:

        df_ = pd.DataFrame()

        vec = [1e-2, 4.63e-3, 2.15e-3, 1e-3,4.63e-4, 2.15e-4, 1e-4,4.63e-5, 2.15e-5, 1e-5,4.63e-6, 2.15e-6, 1e-6,4.63e-7, 2.15e-7, 1e-7]
        # DieType','1MicroSec','5MicroSec', '10MicroSec
        df4=mvgPD2Die.quantile(vec)
        df4['DieLevel']=dieLevel
        df4.rename_axis('Prob')
        df_=df_.append(df4)
        print "quantile info for "+dieLevel+"\n",df_
        _strdf = os.path.join(tmpFolder,'Prob_mvAvg'+dieLevel+time.strftime("%d_%m_%y"))
        df4.to_csv(_strdf+".csv", sep='\t')
        if os.path.exists(_strdf+".csv"):
                print " Uploading quantile to hdfs ",_strdf+".csv"," to ",hdfsbasepath
                hdfsCSVUpload(_strdf,hdfsbasepath)
                os.remove(_strdf+".csv")
        Die2Endtime = datetime.now()
        logger.info('Duration '+dieLevel+' Process: {}'.format(Die2Endtime - Die2Starttime))

        logger.info(">>> Start "+dieLevel+" process <<<<")
        Die4Starttime = datetime.now()

        #sample=rdd4.take(collectSample)
        etime = datetime.now()

        rdd4.unpersist()
        #rdd6=sc.parallelize(sample)
        print ",...rdd4",rdd4.take(1)
        rdd6 = sqlContext.createDataFrame(rdd4).limit(1000000).rdd
        print 'time for take approach: {}'.format(etime - Die4Starttime)
        print "4 die take 1",rdd6.take(1)
        sys.exit()
        #readCSVFile
        dieLevel = "4Die"
        rdd7=rdd6.cartesian(rdd6)
        #print "combo is",rdd7.take(1)
        rdd8 = rdd7.map(lambda dd: adjustDelay(dd[0],dd[1]))
        #print rdd8.take(1)
        rdd9=rdd8.map(lambda cc: addTwoNumpyArray(cc[0],cc[1]))
        rdd9.persist(StorageLevel.MEMORY_AND_DISK)
        #logger.info("adjust delay is %s",rdd9.take(1))
        #rdd9.coalesce(1).saveAsTextFile(os.path.join(hdfsRawDataFolder,dieLevel+'.csv'))
        N = (20,100,400)
        rdd10= rdd9.map(lambda mvg:mvgC(mvg,N))
        #df=pd.DataFrame()
        df5=sqlContext.createDataFrame(rdd10,schema)
        #print df3.show()
        #df2.write.mode(saveMode.Overwrite).parquet("/xxxCharacterisation/MovingAvg/.parquet")
        #df5.write.save(os.path.join(hdfsRawDataFolder,"MaxMovingAverage"+dieLevel+".parquet"), source="parquet", mode='overwrite')

        ### new

        mvgPD4Die=df5.toPandas()
        _strCommon = os.path.join(tmpFolder,'PeakCurrent_mvAvg'+dieLevel+'_'+time.strftime("%d_%m_%y"))
        #_hdfsHist = "histogram"
        #_hdfsCsv = "csv"
        '''
        logger.info(" --- Plotting graphs (Histogram and saving to file/ upload to hdfs) ---" )
        import matplotlib.pyplot as plt
        mvgPD4Die.plot.hist()
        plt.savefig(_strCommon+'.jpeg',dpi=100, bbox_inches='tight')
        #logger.info(" >>> Uploading the histogram to HDFS at %s",hst)
        #print "before upload",_strCommon,hst
        hdfsJPEGUpload(_strCommon,hdfsbasepath+"/"+os.path.basename(_strCommon+".jpeg"))
        '''

        # Quantile functions 
        columns = ['DieType','Prob','1MicroSec','5MicroSec', '10MicroSec']
        #Note: we could create an empty DataFrame (with NaNs) simply by writing:

        #df_ = pd.DataFrame()

        vec = [1e-2, 4.63e-3, 2.15e-3, 1e-3,4.63e-4, 2.15e-4, 1e-4,4.63e-5, 2.15e-5, 1e-5,4.63e-6, 2.15e-6, 1e-6,4.63e-7, 2.15e-7, 1e-7]
        # DieType','1MicroSec','5MicroSec', '10MicroSec
        df6=mvgPD2Die.quantile(vec)
        df6['DieLevel']=dieLevel
        df6.rename_axis('Prob')
        df_=df_.append(df6)
        # Redefine Const for this section
        print "quantile info for "+dieLevel+" \n",df_
        _strdf = os.path.join(tmpFolder,'Prob_mvAvg'+dieLevel+time.strftime("%d_%m_%y"))
        df_.to_csv(_strdf+".csv", sep='\t')
        if os.path.exists(_strdf+".csv"):
                print " "+dieLevel+":Uploading quantile to hdfs ",_strdf," to ",hdfsbasepath
                hdfsCSVUpload(_strdf,hdfsbasepath)
                #os.remove(_strdf+".csv")
                                                                                     
        Die4Endtime = datetime.now()
        print 'total time for '+dieLevel+': {}'.format(Die4Endtime - Die4Starttime)
        ### end new

        if debugMode == 0:
                print  "cartesian count for 4 dies",rdd7.count()
        #rdd9.unpersist()
        logger.info(">>> Start "+dieLevel+" process <<<")
        Die8Starttime = datetime.now()

        sample=rdd9.take(collectSample)
        etime = datetime.now()

        rdd9.unpersist()
        rdd11=sc.parallelize(sample)
        print 'time for take approach: {}'.format(etime - Die8Starttime)
        #readCSVFile
        dieLevel = "8Die"
        rdd12=rdd11.cartesian(rdd11)
        #print "combo is",rdd7.take(1)
        rdd13 = rdd12.map(lambda ee: adjustDelay(ee[0],ee[1]))
        #print rdd8.take(1)
        rdd14=rdd13.map(lambda ff: addTwoNumpyArray(ff[0],ff[1]))
        rdd14.persist(StorageLevel.MEMORY_AND_DISK)
        #logger.info("adjust delay is %s",rdd9.take(1))
        #rdd9.coalesce(1).saveAsTextFile(os.path.join(hdfsRawDataFolder,dieLevel+'.csv'))
        N = (20,100,400)
        rdd15= rdd14.map(lambda mvg:mvgC(mvg,N))
        #df=pd.DataFrame()
        df7=sqlContext.createDataFrame(rdd15,schema)
        #print df3.show()
        #df2.write.mode(saveMode.Overwrite).parquet("/xxxCharacterisation/MovingAvg/.parquet")
        #df7.write.save(os.path.join(hdfsRawDataFolder,"MaxMovingAverage"+dieLevel+".parquet"), source="parquet", mode='overwrite')

        ### new

        mvgPD8Die=df7.toPandas()
        _strCommon = os.path.join(tmpFolder,'PeakCurrent_mvAvg'+dieLevel+'_'+time.strftime("%d_%m_%y"))
        #_hdfsHist = "histogram"
        #_hdfsCsv = "csv"
        '''
        logger.info(" --- Plotting graphs (Histogram and saving to file/ upload to hdfs) ---" )
        import matplotlib.pyplot as plt
        mvgPD8Die.plot.hist()
        plt.savefig(_strCommon+'.jpeg',dpi=100, bbox_inches='tight')
        #logger.info(" >>> Uploading the histogram to HDFS at %s",hst)
        #print "before upload",_strCommon,hst
        hdfsJPEGUpload(_strCommon,hdfsbasepath+"/"+os.path.basename(_strCommon+".jpeg"))
        '''

        # Quantile functions 
        columns = ['DieType','Prob','1MicroSec','5MicroSec', '10MicroSec']
        #Note: we could create an empty DataFrame (with NaNs) simply by writing:

        #df_ = pd.DataFrame()

        vec = [1e-2, 4.63e-3, 2.15e-3, 1e-3,4.63e-4, 2.15e-4, 1e-4,4.63e-5, 2.15e-5, 1e-5,4.63e-6, 2.15e-6, 1e-6,4.63e-7, 2.15e-7, 1e-7]
        # DieType','1MicroSec','5MicroSec', '10MicroSec
        df8=mvgPD8Die.quantile(vec)
        df8['DieLevel']=dieLevel
        df8.rename_axis('Prob')
        df_=df_.append(df8)
        # Redefine Const for this section
        _strdf = os.path.join(tmpFolder,'Prob_mvAvg'+dieLevel+time.strftime("%d_%m_%y"))
        df_.to_csv(_strdf+".csv", sep='\t')
        print " qauntile for "+dieLevel+" is : ",df_
        if os.path.exists(_strdf+".csv"):
                print " 8Die:Uploading quantile to hdfs ",_strdf," to ",hdfsbasepath
                hdfsCSVUpload(_strdf,hdfsbasepath)
                #os.remove(_strdf+".csv")
                                                                                                  
        Die8Endtime = datetime.now()
        print 'total time for '+dieLevel+': {}'.format(Die8Endtime - Die8Starttime)

        dieLevel = "16Die"
        logger.info(">>> Start "+dieLevel+" Die process <<<")
        Die16Starttime = datetime.now()

        #sample=rdd14.take(collectSample)
        rdd16 = sqlContext.createDataFrame(rdd14).limit(1000000).rdd
        etime = datetime.now()

        rdd14.unpersist()
        #rdd16=sc.parallelize(sample)
        print 'time for take approach: {}'.format(etime - Die16Starttime)
        #readCSVFile
        rdd17=rdd16.cartesian(rdd16)
        #print "combo is",rdd7.take(1)
        rdd18 = rdd17.map(lambda ee: adjustDelay(ee[0],ee[1]))
        #print rdd8.take(1)
        rdd19=rdd18.map(lambda ff: addTwoNumpyArray(ff[0],ff[1]))
        rdd19.persist(StorageLevel.MEMORY_AND_DISK)
        #logger.info("adjust delay is %s",rdd9.take(1))
        #rdd9.coalesce(1).saveAsTextFile(os.path.join(hdfsRawDataFolder,dieLevel+'.csv'))
        N = (20,100,400)
        rdd20= rdd19.map(lambda mvg:mvgC(mvg,N))
        #df=pd.DataFrame()
        df9=sqlContext.createDataFrame(rdd20,schema)
        #print df3.show()
        #df2.write.mode(saveMode.Overwrite).parquet("/xxxCharacterisation/MovingAvg/.parquet")
        #df9.write.save(os.path.join(hdfsRawDataFolder,"MaxMovingAverage"+dieLevel+".parquet"), source="parquet", mode='overwrite')

        ### new

        mvgPD16Die=df9.toPandas()
        _strCommon = os.path.join(tmpFolder,'PeakCurrent_mvAvg'+dieLevel+'_'+time.strftime("%d_%m_%y"))
        #_hdfsHist = "histogram"
        #_hdfsCsv = "csv"
        '''
        logger.info(" --- Plotting graphs (Histogram and saving to file/ upload to hdfs) ---" )
        import matplotlib.pyplot as plt
        mvgPD16Die.plot.hist()
        plt.savefig(_strCommon+'.jpeg',dpi=100, bbox_inches='tight')
        #logger.info(" >>> Uploading the histogram to HDFS at %s",hst)
        #print "before upload",_strCommon,hst
        hdfsJPEGUpload(_strCommon,hdfsbasepath+"/"+os.path.basename(_strCommon+".jpeg"))

        '''
        # Quantile functions 
        columns = ['DieType','Prob','1MicroSec','5MicroSec', '10MicroSec']
        #Note: we could create an empty DataFrame (with NaNs) simply by writing:

        #df_ = pd.DataFrame()

        vec = [1e-2, 4.63e-3, 2.15e-3, 1e-3,4.63e-4, 2.15e-4, 1e-4,4.63e-5, 2.15e-5, 1e-5,4.63e-6, 2.15e-6, 1e-6,4.63e-7, 2.15e-7, 1e-7]
        
	# DieType','1MicroSec','5MicroSec', '10MicroSec
        df10=mvgPD16Die.quantile(vec)
        df10['DieLevel']=dieLevel
        df10.rename_axis('Prob')
        df_=df_.append(df10)
        # Redefine Const for this section
        _strdf = os.path.join(tmpFolder,'Prob_mvAvg'+dieLevel+time.strftime("%d_%m_%y"))
        df_.to_csv(_strdf+".csv", sep='\t')
        print " df now is ",df_

        if os.path.exists(_strdf+".csv"):
                print " 16Die:Uploading quantile to hdfs ",_strdf," to ",hdfsbasepath
                hdfsCSVUpload(_strdf,hdfsbasepath)
                #os.remove(_strdf+".csv")
        Die16Endtime = datetime.now()
        print 'total time for '+dieLevel+': {}'.format(Die16Endtime - Die16Starttime)
                                                                                          
       rdd19.unpersist()
if __name__ == "__main__":
        start_time = datetime.now()

        #print "Start Time : ",start_time
        try:
                main()
                logger = logging.getLogger("icclog")
        except:
                import traceback
                s = traceback.format_exc()
                serr = ">stderr:\n%s\n" %s
                print serr
        logger = logging.getLogger("icclog")
        end_time = datetime.now()
        print "Start Time : ",start_time
        print "End Time : ",end_time
        print('Duration: {}'.format(end_time - start_time))
        logger.info("Start Time :%s ",start_time)
        logger.info("End Time : %s",end_time)

