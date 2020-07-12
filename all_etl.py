import pandas as pd
import re
import pyspark
import findspark
import random
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types as t
from datetime import datetime, timedelta

from sql_queries import *

### ----- ----- ----- ----- ----- ###
### Helper functions used in other methods

def parsell(string):
    string = string.strip().lower()

    if string.endswith('w') or string.endswith('s'):
        sign = -1
    else:
        sign = 1

    string = re.sub(r"[^0-9.]", " ", string).strip()

    numeric_ll = float(string)
    return numeric_ll * sign

def parsedt(date, time):
    date = date.strip()
    time = time.strip()
    
    datetime = pd.to_datetime(date + time, format='%Y%m%d%H%M')
    return datetime


def parquet_wr(spark, parquet_filename, df):

    start_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    
    #write dataframe to parquet file
    df.write.mode("overwrite").parquet(parquet_filename + "_" + start_time)

    #read parquet file to Spark dataframe
    df = spark.read.parquet(parquet_filename + "_" + start_time)
    
    
def spark_verify():   
    findspark.init()
    sc = pyspark.SparkContext(appName="Pi")
    num_samples = 10000

    def inside(p):     
      x, y = random.random(), random.random()
      return x*x + y*y < 1

    count = sc.parallelize(range(0, num_samples)).filter(inside).count()

    pi = 4 * count / num_samples
    print(pi)

    sc.stop()

### ----- ----- ----- ----- ----- ###
### Define paths for data import / export

def define_paths():
    """
    Define data locations.
    """
    path_d = {}

    path_d["input_data"] = 'data/'
    path_d["output_data"] = 'data/output_data/'      
    path_d["data_storage"] = 'parquet' 
    path_d["hurdat"] = 'https://www.aoml.noaa.gov/hrd/hurdat/hurdat2.html'
    path_d["nexrad"] = 'data/NEXRAD/KCRP20200101_000431_V06'
       
    return path_d

### ----- ----- ----- ----- ----- ###
### Create Spark session

def create_spark_session():
    """
    Create Spark session.
    """
    print("Create Spark session")
    findspark.init()
    findspark.find()
    
    conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
    sc = pyspark.SparkContext(conf=conf)
    spark = SparkSession(sc)
    
    return spark

