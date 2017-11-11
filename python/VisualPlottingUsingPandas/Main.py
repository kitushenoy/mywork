#!/usr/anaconda/bin/python
# -*- coding: utf-8 -*-
'''
 synatx :  spark-submit --master local --executor-memory 6G --executor-cores 4 --driver-cores 3 --driver-memory 3G Main.py R01,R02
 OR
 python Main.py R01,R02,R03 -p 1

Created on Mon Jul 25 2017
@author: Kirthi Shanbhag
Description : This script is designed to Plot graph using memory hole recipe data
synatx :spark-submit --master local --executor-memory 6G --executor-cores 4 --driver-cores 3 --driver-memory 3G Main.py R01,R02
 	OR
	python Main.py R01,R02,R03 -p 1
# debug mode uncomment debugmode = 0
'''

import os,re,argparse,inspect
import itertools
import numpy as np
import logging
import pandas as pd
import traceback,sys
import time,glob,shutil
import numpy as np
import sys,os,time
from datetime import datetime
from logging.handlers import RotatingFileHandler
import matplotlib.pyplot as plt
import glob

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

def removeHeader(rdd):
        from pyspark.sql.types import StructType,StructField,IntegerType,FloatType,StringType
        # Construct the schema from the header 
        header = rdd.first()
        schemaString = map(lambda x: x.replace('\'',''),header)  # get rid of the double-quotes
        print schemaString
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString]
        schema = StructType(fields)

        # Subtract header and use the above-constructed schema:
        Header = rdd.filter(lambda l: "_id" in l) # taxiHeader needs to be an RDD - the string we constructed above will not do the job
        Header.collect() # for inspection purposes only
        NoHeaderRdd = rdd.subtract(Header)
        return(NoHeaderRdd,schema)

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
def main():

        ###############
        ## main code ##
        ###############

        print "======================================="
        parser = argparse.ArgumentParser(description='Program to plot graph',usage='example: python Main.py --debugmode 0')
        parser.add_argument('runnumber', help='give two or more runnumbers')
        parser.add_argument('graphType',help="gives user an option to choose type of graph[must be specified for aaa,bbb,ccc,ddd,eee,ratiobbbVszzz]\nexample: 'bar,scatter,line,line,line,line'")
        parser.add_argument('-m','--debugmode',help='use this if you want to use the program in debug mode')
        parser.add_argument('-p','--programmode',help='use this to indicate if you want to load the data from local or hdfs')
        parser.add_argument('-f', '--file',help="full path of the file; if not given it will assume a file called MMXX.csv in current directory ")
        parser.add_argument('-s', '--show',help="show the plot; 0 = enabled (default),1= disable")
        parser.add_argument('-t','--graphType',help="gives user an option to choose type of graph[must be specified for aaa,bbb,ccc,ddd,eee,ratiobbbVszzz]\nexample: 'bar,scatter,line,line,line,line'",action="store_true")
        print "*********************************************"
        print " P.S.; You can save the graph using save button in the figure; please choose the format to be one of the following \n"
        print " jpeg,jpg,gif,tiff,pdf"
        print "*********************************************"
        args = parser.parse_args()
        if args.debugmode:
                if args.debugmode == "1":
                        debugMode = 1
                else:
                        debugMode =0
        else:
                debugMode = 1

        if args.programmode:
                if args.programmode == 'spark':
                        progMode=0
                else:
                        progMode=1

        else:
                # Always use spark mode; assumiong the data stored in hdfs
                progMode = 0
        if args.file:
                file=args.file

        else:
                file="MMXX.csv"
        if args.show:
                if args.show == '0':
                        showPlot = 0
                else:
                        showPlot = 1
        else:
                showPlot = 0

        if args.graphType:
                graphType=args.graphType.split(",")
                if len(graphType) < 6:
                        print "ERROR - > graph type sequence and length is not specified correctly"
                        sys.exit()
        else:
                graphType = ["line","line","line","line","line","line"]

        graphType1=graphType[0]
        graphType2=graphType[1]
        graphType3=graphType[2]
        graphType4=graphType[3]
        graphType5=graphType[4]
        graphType6=graphType[5]




        import getpass
        homeDir = "/home/"+getpass.getuser()
        if progMode == 0 :
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
                from pyspark.sql import HiveContext, SQLContext
                from pyspark.sql.types import StructType,StructField,IntegerType,FloatType
                sqlContext = SQLContext(sc)

                rdd=sc.textFile(file).map(lambda x: x.encode("ascii", "ignore"))
                print "..rdd",rdd.take(2)
                #rdd,schema = removeHeader(rdd)
                from pyspark.sql.types import StructType,StructField,IntegerType,FloatType,StringType
                rdd.count()
                rdd = rdd.map(lambda x:x.split(','))
                header = rdd.first()
                data = rdd.filter(lambda row : row != header).toDF(header)
                df = data.toPandas()
                df["MaskRemaining"]=df["MaskRemaining"].fillna(0.0).astype(int)
                df["bbb"]=df["bbb"].fillna(0.0).astype(int)
                df["ccc"]=df["ccc"].fillna(0.0).astype(int)
                df["ddd"]=df["ddd"].fillna(0.0).astype(int)
                df["eee"] = df["eee"].fillna(0.0).astype(int)
        else:
                print "Reading raw data from CSV "
                if os.path.exists(file):
                        df = pd.read_csv("MMXX.csv")
                else:
                        print " File %s does not exists",(file)
                        sys.exit()

        print "data is \n",df.info()
        runN
        importum = re.sub(r'\'"', '', args.runnumber).split(",") glob
        print " Plotting graphs for ",runNum

        if len(runNum) < 2:
                print " ERROR : Cannot plot graphs for less than 2 run numbers"
                sys.exit()

        print " As a first step deleting all the earlier plotted graph"

        listFig= []
        listFig.append(glob.glob(homeDir+'/*.png'))
        listFig.append(glob.glob(homeDir+'/*.jpeg'))
        listFig.append(glob.glob(homeDir+'/*.gif'))
        listFig.append(glob.glob(homeDir+'/*.jpg'))
        listFig.append(glob.glob(homeDir+'/*.pdf'))
        listFig.append(glob.glob(homeDir+'/*.tiff'))
        listFig = sum(listFig,[])
        if len(listFig) > 0:
                print " Removing following files",listFig
                [os.remove(n) for n in listFig]

        for cnt,r in enumerate(runNum):
                if cnt == 0 :
                        df2=df.loc[df.RunNo == str(r)]
                else:

                        df2=df2.append(df.loc[df.RunNo == str(r)])
        df2['Ratiobbb_zzz'] = pd.to_numeric(df.bbb)/pd.to_numeric(df.eee)
        df_maskR = df2[['RunNo','Location','MaskRemaining']].pivot(index ='Location',columns='RunNo',values='MaskRemaining')

        df_bbb = df2[['RunNo','Location', 'bbb']].pivot(index='Location',columns='RunNo',values='bbb')
        df_ccc = df2[['RunNo','Location', 'ccc']].pivot(index='Location',columns='RunNo',values='ccc')
        df_ddd = df2[['RunNo','Location', 'ddd']].pivot(index='Location',columns='RunNo',values='ddd')
        df_ratiobbb_zzz = df2[['RunNo','Location', 'Ratiobbb_zzz']].pivot(index='Location',columns='RunNo',values='Ratiobbb_zzz')
        df_etch = df2[['RunNo','Location', 'eee']].pivot(index='Location',columns='RunNo',values='eee')


        import matplotlib.pyplot as plt
        plt.style.use('ggplot')
        from itertools import cycle
        cycol = cycle('bgrcmk').next
        x_ticks_labels = df_ratiobbb_zzz.index.values
        x1=list(range(len(df_ratiobbb_zzz.index)))
        fig, axes = plt.subplots(nrows=3, ncols=2)
        fig.tight_layout() # Or equivalently,  "plt.tight_layout()"
        print df_maskR
        if graphType1 == "scatter":
                loc =df_maskR.index.values
                Xuniques, X = np.unique(loc, return_inverse=True)
                x=X
                df_maskR['Location'] = x1
                for cnt,n in enumerate(runNum):
                        Y=df_maskR[n]
                        df_maskR.plot(kind='scatter',x='Location',y=n,label=n,c=cycol(),ax=axes[0,0])
                        axes[0,0].set_xticks(x)
                        axes[0,0].set_xticklabels(x_ticks_labels, rotation='vertical', fontsize=18)
                        axes[0,0].set_ylabel('Runumber')
        elif graphType1 == "bar":
                df_maskR.plot(kind='bar',ax=axes[0,0])
        else:
                df_maskR.plot(ax=axes[0,0],marker='.',markersize=10)
        axes[0,0].set_title("MaskRemaining")
        label = axes[0,0].xaxis.get_label()
        x_lab_pos, y_lab_pos = label.get_position()
        label.set_position([x_lab_pos, y_lab_pos]) # if you want the label to be right align chnage the arguments to 1.0
        label.set_horizontalalignment('right')
        axes[0,0].xaxis.set_label(label)
                                                                               
        if graphType2 == "scatter":
                loc =df_bbb.index.values
                Xuniques, X = np.unique(loc, return_inverse=True)
                x=X
                df_bbb['Location'] = x1
                for cnt,n in enumerate(runNum):
                        Y=df_bbb[n]
                        if cnt == 0:
                            df_bbb.plot(kind='scatter',x='Location',y=n,label=n,c=cycol(),ax=axes[0,1])
                        else:
                            df_bbb.plot(x='Location',y=n,kind='scatter',label=n,c=cycol(),ax=axes[0,1])
                        axes[0,1].set_xticks(x)
                        # Set ticks labels for x-axis
                        axes[0,1].set_xticklabels(x_ticks_labels, rotation='vertical', fontsize=18)
                        axes[0,1].set_ylabel('Runumber')

        elif graphType2 == "bar":
                df_bbb.plot(kind='bar',ax=axes[0,1])
        else:
                df_bbb.plot(ax=axes[0,1],marker='.',markersize=10)
        axes[0,1].set_title("bbb")

        if graphType3 == "scatter":
                loc =df_ccc.index.values
                Xuniques, X = np.unique(loc, return_inverse=True)
                x=X
                df_ccc['Location'] = x1
                for cnt,n in enumerate(runNum):
                        Y=df_ccc[n]
                        df_ccc.plot(kind='scatter',x='Location',y=n,label=n,c=cycol(),ax=axes[1,0])
                        axes[1,0].set_xticks(x)
                        axes[1,0].set_xticklabels(x_ticks_labels, rotation='vertical', fontsize=18)
                        axes[1,0].set_ylabel('Runumber')

        elif graphType3 == "bar":
                df_ccc.plot(kind='bar',ax=axes[1,0])
        else:
                df_ccc.plot(ax=axes[1,0],marker='.',markersize=10)

        axes[1,0].set_title("ccc")
        if graphType4 == "scatter":
                loc =df_ddd.index.values
                Xuniques, X = np.unique(loc, return_inverse=True)
                x=X
                df_ddd['Location'] = x1
                for cnt,n in enumerate(runNum):
                        Y=df_ddd[n]
                        df_ddd.plot(kind='scatter',x='Location',y=n,label=n,c=cycol(),ax=axes[1,1])
                        axes[1,1].set_xticks(x)
                        axes[1,1].set_xticklabels(x_ticks_labels, rotation='vertical', fontsize=18)
                        axes[1,1].set_ylabel('Runumber')

        elif graphType4 == "bar":
                df_ddd.plot(kind='bar',ax=axes[1,1])
        else:
                df_ddd.plot(ax=axes[1,1],marker='.',markersize=10)
        axes[1,1].set_title("ddd")
        if graphType5 == "scatter":
                loc =df_etch.index.values
                Xuniques, X = np.unique(loc, return_inverse=True)
                x=X
                df_etch['Location'] = x1
                for cnt,n in enumerate(runNum):
                        Y=df_etch[n]
                        df_etch.plot(kind='scatter',x='Location',y=n,label=n,c=cycol(),ax=axes[2,0])

                        axes[2,0].set_xticks(x)
                        axes[2,0].set_xticklabels(x_ticks_labels, rotation='vertical', fontsize=18)
                        axes[2,0].set_ylabel('Runumber')
                                                                                          
        elif graphType5 == "bar":
                df_etch.plot(kind='bar',ax=axes[2,0])
        else:
                df_etch.plot(ax=axes[2,0],marker='.',markersize=10)
        axes[2,0].set_title("eee")
        if graphType6 == "scatter":
                loc =df_ratiobbb_zzz.index.values
                Xuniques, X = np.unique(loc, return_inverse=True)
                x=X
                df_ratiobbb_zzz['Location'] = x1
                for cnt,n in enumerate(runNum):
                        Y=df_ratiobbb_zzz[n]
                        if cnt == 0:
                            df_ratiobbb_zzz.plot(kind='scatter',x='Location',y=n,label=n,c=cycol(),ax=axes[2,1])
                        else:
                            df_ratiobbb_zzz.plot(x='Location',y=n,kind='scatter',label=n,c=cycol(),ax=axes[2,1])
                        axes[2,1].set_xticks(x)
                        axes[2,1].set_xticklabels(x_ticks_labels, rotation='vertical', fontsize=18)
                        axes[2,1].set_ylabel('Runumber')
        elif graphType6 == "bar":

                df_ratiobbb_zzz.plot(kind='bar',ax=axes[2,1])
        else:
                df_ratiobbb_zzz.plot(ax=axes[2,1])
        axes[2,1].set_title("Ratiobbb_zzz")
        if showPlot == 0:
                plt.show()
        import glob
        listFig= []
        listFig.append(glob.glob(homeDir+'/*.png'))
        listFig.append(glob.glob(homeDir+'/*.jpeg'))
        listFig.append(glob.glob(homeDir+'/*.gif'))
        listFig.append(glob.glob(homeDir+'/*.jpg'))
        listFig.append(glob.glob(homeDir+'/*.pdf'))
        listFig.append(glob.glob(homeDir+'/*.tiff'))
        listFig = sum(listFig,[])
        if len(listFig) > 0:
                _str= ""
                for n in listFig:
                        _str=_str+" "+n
                import getpass
                loc="/user/"+getpass.getuser()
                print "Uploading the jpeg to hdfs"
                command = " hdfs dfs -copyFromLocal -f "+_str+" "+loc

                print " command hdfs :",command
                a,b,c = exec_command(command)
                printHDFSResult(a,b,c)
                [os.remove(n) for n in listFig]
if __name__ == "__main__":
        start_time = datetime.now()

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
        logger.info('Duration: {}'.format(end_time - start_time))

