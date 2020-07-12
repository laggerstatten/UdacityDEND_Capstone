import pandas as pd
import re
import pyspark
import findspark
import random
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types as t
from datetime import datetime, timedelta
from sphere import RegionCoverer, Cell, LatLng, LatLngRect, CellId

from sql_queries import *
from all_etl import *

### ----- ----- ----- ----- ----- ###
### Initial processing and loading for HURDAT data

def process_hurdat_data(spark, path_d):
    """
    Load input data (HURDAT) from input path
    Write / read the data to / from Spark
    Store the data as parquet staging files
    """

    print("Processing HURDAT data")
    
    # Read CSV
    col_names = ['Date','Time','RecordIdentifier','SystemStatus','Latitude','Longitude','MaxSustWind','MaxPressure',
                 'NE34','SE34','SW34','NW34',
                 'NE50','SE50','SW50','NW50',
                 'NE64','SE64','SW64','NW64']
    hurdat_df = pd.read_csv(path_d["hurdat"], skiprows = 2, low_memory=False, names=col_names)
    
    # Initial cleaning / reshaping
    
    #remove ghost row
    hurdat_df = hurdat_df.drop([0])

    #check if row is convoluted header row (contains ALPHA characters)
    hurdat_df['IsStormHdr'] = ~hurdat_df['Date'].str.isdigit()

    #create empty columns to receive header data
    hurdat_df['StormIdentifier'] = ''
    hurdat_df['StormName'] = ''
    hurdat_df['StormSamples'] = ''
    
    #Iterate over rows to get header data and write to list
    Lidentifier = []
    Lname = []
    Lsamples = []

    identifier = ""
    name = ""
    samples = ""

    for row in hurdat_df.itertuples(index=True):
        if (getattr(row, "IsStormHdr") == True):
            identifier = getattr(row, "Date")
            name = getattr(row, "Time")
            samples = getattr(row, "RecordIdentifier")
        Lidentifier.append(identifier)
        Lname.append(name)
        Lsamples.append(samples)    
    
    #write list data into dataframe
    hurdat_df.StormIdentifier = Lidentifier
    hurdat_df.StormName = Lname
    hurdat_df.StormSamples = Lsamples    
    
    
    #split into storms and tracks
    hurdat_storms_df = hurdat_df[hurdat_df['IsStormHdr'] == True].copy()
    hurdat_storms_df = hurdat_storms_df[['StormIdentifier','StormName','StormSamples']]

    hurdat_tracks_df = hurdat_df[hurdat_df['IsStormHdr'] == False].copy()  
    
    
    # spatial processing
    Tlatitude = [parsell(lat) for lat in hurdat_tracks_df['Latitude']]
    hurdat_tracks_df['Latitude'] = Tlatitude

    Tlongitude = [parsell(lon) for lon in hurdat_tracks_df['Longitude']]
    hurdat_tracks_df['Longitude'] = Tlongitude 
    
    
    
    s2level = 10
    hurdat_tracks_df['S2LL'] = [LatLng.from_degrees(x, y) for x, y in zip(hurdat_tracks_df['Latitude'], hurdat_tracks_df['Longitude'])]
    hurdat_tracks_df['S2CellID'] = [CellId().from_lat_lng(xy) for xy in hurdat_tracks_df['S2LL']]
    hurdat_tracks_df['S2Region'] = [z.parent(s2level) for z in hurdat_tracks_df['S2CellID']]
    
    
    
    #fix datetime in tracks table
    Tdatetime = [parsedt(date, time) for date, time in zip(hurdat_tracks_df['Date'],hurdat_tracks_df['Time'])]
    hurdat_tracks_df['Tdatetime'] = Tdatetime  
    
    
    # --------------------------------------------------------
    # Read data to Spark
    print("Reading HURDAT to Spark")
    hurdat_storm_schema = t.StructType([
                                t.StructField("StormIdentifier", t.StringType(), False),
                                t.StructField("StormName", t.StringType(), False),
                                t.StructField("StormSamples", t.StringType(), False),
                            ])

    hurdat_track_schema = t.StructType([
                                t.StructField('Date', t.StringType(), False),
                                t.StructField('Time', t.StringType(), False),       
                                t.StructField('RecordIdentifier', t.StringType(), False),
                                t.StructField('SystemStatus', t.StringType(), False),
                                t.StructField('Latitude', t.StringType(), False),
                                t.StructField('Longitude', t.StringType(), False),
                                t.StructField('MaxSustWind', t.StringType(), False),
                                t.StructField('MaxPressure', t.StringType(), False),                   
                                t.StructField('NE34', t.StringType(), False),
                                t.StructField('SE34', t.StringType(), False),
                                t.StructField('SW34', t.StringType(), False),
                                t.StructField('NW34', t.StringType(), False),    
                                t.StructField('NE50', t.StringType(), False),
                                t.StructField('SE50', t.StringType(), False),
                                t.StructField('SW50', t.StringType(), False),
                                t.StructField('NW50', t.StringType(), False),
                                t.StructField('NE64', t.StringType(), False),
                                t.StructField('SE64', t.StringType(), False),
                                t.StructField('SW64', t.StringType(), False),
                                t.StructField('NW64', t.StringType(), False),
                                t.StructField('IsStormHdr', t.StringType(), False),             
                                t.StructField("Identifier", t.StringType(), False),
                                t.StructField("Name", t.StringType(), False),
                                t.StructField("Samples", t.StringType(), False),
                                t.StructField("S2LL", t.StringType(), False),
                                t.StructField("S2CellID", t.StringType(), False),        
                                t.StructField("S2Region", t.StringType(), False),        
                                t.StructField('Tdatetime', t.DateType(), False)
                                ])

    hurdat_storms_df_spark = spark.createDataFrame(hurdat_storms_df, schema=hurdat_storm_schema)
    hurdat_tracks_df_spark = spark.createDataFrame(hurdat_tracks_df, schema=hurdat_track_schema)

    
    parquet_wr(spark, path_d["output_data"] + "hurdat_storms_stage.parquet", hurdat_storms_df_spark)
    parquet_wr(spark, path_d["output_data"] + "hurdat_tracks_stage.parquet", hurdat_tracks_df_spark)

    
    print("HURDAT processing complete")

    return hurdat_storms_df_spark, hurdat_tracks_df_spark, hurdat_tracks_df


### ----- ----- ----- ----- ----- ###
### process HURDAT Dimension table

def process_joined_hurdat_data( spark, path_d, hurdat_storms_df_spark, hurdat_tracks_df_spark):
    """
    Load input data
    Join tables
    Write / read the data to / from Spark
    Store the data as parquet dimension files
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

### ----- ----- ----- ----- ----- ###
### check HURDAT data quality (nulls, row count)

def check_data_quality( spark, hurdat_table):
    """
    Check data quality for HURDAT table
    """

    results = { "hurdat_count": 0,
                "hurdat": ""           
              }

    print("Checking HURDAT table...")

    hurdat_table.createOrReplaceTempView("hurdat_table_DF")
    hurdat_table_check1 = spark.sql(hurdat_table_check1_query)
    hurdat_table_flag1 = hurdat_table_check1.collect()[0][0] > 0
    hurdat_table_check2 = spark.sql(hurdat_table_check2_query)
    hurdat_table_flag2 = hurdat_table_check2.collect()[0][0] < 1

    hurdat_flag = any([hurdat_table_flag1, hurdat_table_flag2])

    if hurdat_flag:
        results['hurdat_count'] = hurdat_table_check2.collect()[0][0]
        results['hurdat'] = "NOK"
    else:
        results['hurdat_count'] = hurdat_table_check2.collect()[0][0]
        results['hurdat'] = "OK" 

    print("NULLS:")
    hurdat_table_check1.show(1)
    print("ROWS:")
    hurdat_table_check2.show(1)

    print("Checking data quality complete")

    return results



    
    
    