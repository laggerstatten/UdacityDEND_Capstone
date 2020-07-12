import numpy as np
import pandas as pd
import itertools
import pyart
#from sphere import LatLng, CellId
from datetime import datetime, timedelta

#import pandas as pd
#import re
#import pyspark
#import findspark
#import random
#from pyspark import SparkContext, SparkConf
#from pyspark.sql import SparkSession
#from pyspark.sql import types as t
#from datetime import datetime, timedelta

from sql_queries import *
from all_etl import *


### ----- ----- ----- ----- ----- ###
### Initial processing and loading for NEXRAD data

def process_nexrad_data(spark, path_d):
    """
    Load input data (NEXRAD) from input path
    Write / read the data to / from Spark
    Store the data as parquet staging files
    """

    print("Processing NEXRAD data")

    # read NEXRAD file
    #radar = pyart.io.read('data/NEXRAD/KCRP20200101_000431_V06')   
    nexrad_df = pyart.io.read(path_d["nexrad"])       
    
    ### build station data table
    station_name = nexrad_df.metadata['instrument_name']
    product = nexrad_df.metadata['original_container']
    pattern = nexrad_df.metadata['vcp_pattern']
    latitude0 = nexrad_df.latitude['data'][0]
    longitude0 = nexrad_df.longitude['data'][0]
    altitude0 = nexrad_df.altitude['data'][0]
    volume_start = datetime.strptime(nexrad_df.time['units'][14:34], '%Y-%m-%dT%H:%M:%SZ')

    volume_start_x1 = [volume_start + timedelta(seconds=0)]

    nexrad_station_df = pd.DataFrame(
        {'StationName': station_name, 
         'Product': product, 
         'Pattern': pattern, 
         'Latitude': latitude0,
         'Longitude': longitude0,
         'Altitude': altitude0,
         'StartTime': volume_start_x1     
        })

    ### build initial samples table
    merged_lat = list(itertools.chain.from_iterable(nexrad_df.gate_latitude['data']))
    merged_lon = list(itertools.chain.from_iterable(nexrad_df.gate_longitude['data']))
    merged_alt = list(itertools.chain.from_iterable(nexrad_df.gate_altitude['data']))
    merged_refl = list(itertools.chain.from_iterable(nexrad_df.fields['reflectivity']['data']))
    merged_velo = list(itertools.chain.from_iterable(nexrad_df.fields['velocity']['data']))
    time_x1 = [volume_start + timedelta(seconds=s) for s in nexrad_df.time['data']]
    time_xgates = [val for val in time_x1 for _ in range(nexrad_df.ngates)]

    nexrad_sample_df = pd.DataFrame(
        {'GateLat': merged_lat,
         'GateLon': merged_lon,
         'GateAlt': merged_alt,
         'GateTime': time_xgates,
         'Reflectivity': merged_refl,
         'Velocity': merged_velo
        })    
    
    
    # --------------------------------------------------------
    # Read data to Spark
    print("Reading NEXRAD to Spark")
    nexrad_station_schema = t.StructType([
                                t.StructField("StationName", t.StringType(), False),
                                t.StructField("Product", t.StringType(), False),
                                t.StructField("Pattern", t.StringType(), False),
                                t.StructField("Latitude", t.StringType(), False),
                                t.StructField("Longitude", t.StringType(), False),
                                t.StructField("Altitude", t.StringType(), False),       
                                t.StructField("StartTime", t.StringType(), False)        
                            ])

    nexrad_sample_schema = t.StructType([
                                t.StructField('GateLat', t.StringType(), False),
                                t.StructField('GateLon', t.StringType(), False),
                                t.StructField('GateAlt', t.StringType(), False),
                                t.StructField('GateTime', t.StringType(), False),
                                t.StructField('Reflectivity', t.StringType(), False),
                                t.StructField('Velocity', t.StringType(), False)    
                            ])

    nexrad_stations_df_spark = spark.createDataFrame(nexrad_station_df, schema=nexrad_station_schema)
    nexrad_samples_df_spark = spark.createDataFrame(nexrad_sample_df, schema=nexrad_sample_schema)

    
    parquet_wr(spark, path_d["output_data"] + "nexrad_stations_stage.parquet", nexrad_stations_df_spark)
    parquet_wr(spark, path_d["output_data"] + "nexrad_samples_stage.parquet", nexrad_samples_df_spark)

    
    print("NEXRAD processing complete")

    return nexrad_stations_df_spark, nexrad_samples_df_spark



"""
### ----- ----- ----- ----- ----- ###
### process NEXRAD Dimension table

def process_joined_hurdat_data( spark, path_d, hurdat_storms_df_spark, hurdat_tracks_df_spark):
    """
"""
    Load input data
    Join tables
    Write / read the data to / from Spark
    Store the data as parquet dimension files
"""

    
"""
    print("Creating table")

    hurdat_df_spark_joined = hurdat_storms_df_spark\
                        .join(hurdat_tracks_df_spark, \
                        (hurdat_storms_df_spark.StormIdentifier == hurdat_tracks_df_spark.Identifier))   
    
    # Create table
    hurdat_df_spark_joined.createOrReplaceTempView("hurdat_table_DF")
    hurdat_table = spark.sql(hurdat_table_createquery)

    print("HURDAT schema:")
    hurdat_table.printSchema()

    parquet_wr(spark, path_d["output_data"] + "hurdat_table.parquet", hurdat_table)
    
    print("HURDAT table complete")

    return hurdat_table

"""



### ----- ----- ----- ----- ----- ###
### check NEXRAD data quality (nulls, row count)

def check_data_quality( spark, nexrad_table):
    """
    Check data quality for NEXRAD table
    """

    results = { "nexrad_count": 0,
                "nexrad": ""           
              }

    print("Checking NEXRAD table...")

    nexrad_table.createOrReplaceTempView("nexrad_table_DF")
    nexrad_table_check1 = spark.sql(nexrad_table_check1_query)
    nexrad_table_flag1 = nexrad_table_check1.collect()[0][0] > 0
    nexrad_table_check2 = spark.sql(nexrad_table_check2_query)
    nexrad_table_flag2 = nexrad_table_check2.collect()[0][0] < 1

    nexrad_flag = any([nexrad_table_flag1, nexrad_table_flag2])

    if nexrad_flag:
        results['nexrad_count'] = nexrad_table_check2.collect()[0][0]
        results['nexrad'] = "NOK"
    else:
        results['nexrad_count'] = nexrad_table_check2.collect()[0][0]
        results['nexrad'] = "OK" 

    print("NULLS:")
    nexrad_table_check1.show(1)
    print("ROWS:")
    nexrad_table_check2.show(1)

    print("Checking data quality complete")

    return results



    
    
    