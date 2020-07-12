hurdat_table_createquery = """
    SELECT          StormIdentifier  AS storm_id, 
                    StormName AS storm_name, 
                    StormSamples AS sample_count, 
                    Datetime AS datetime, 
                    S2CellID AS s2_cell_id,
                    S2Region AS s2_region,
                    RecordIdentifier AS record_id,
                    SystemStatus AS system_status, 
                    Latitude AS latitude, 
                    Longitude AS longitude, 
                    MaxSustWind AS max_sust_wind, 
                    MaxPressure AS max_pressure, 
                    NE34 AS NE34, 
                    SE34 AS SE34, 
                    SW34 AS SW34, 
                    NW34 AS NW34, 
                    NE50 AS NE50, 
                    SE50 AS SE50, 
                    SW50 AS SW50, 
                    NW50 AS NW50, 
                    NE64 AS NE64, 
                    SE64 AS SE64, 
                    SW64 AS SW64, 
                    NW64 AS NW64                       
    FROM hurdat_table_DF             AS hurdat
"""

hurdattime_table_createquery = """
    SELECT DISTINCT  dt             AS datetime, 
                     year(dt)       AS year,
                     month(dt)      AS month,
                     day(dt)        AS day, 
                     hour(dt)       AS hour,                      
                     minute(dt)     AS minute
    FROM time_table_DF
"""

# Check that key fields have valid values (no nulls or empty)
hurdat_table_check1_query = """
    SELECT  COUNT(*)
    FROM hurdat_table_DF
    WHERE   storm_id IS NULL OR storm_id == ""
"""

# Check that table has > 0 rows
hurdat_table_check2_query = """
    SELECT  COUNT(*)
    FROM hurdat_table_DF
"""

######
######

nexrad_table_createquery = """
    SELECT          GateLat AS latitude, 
                    GateLon AS longitude, 
                    GateAlt AS altitude, 
                    GateTime AS time, 
                    Reflectivity AS reflectivity, 
                    Velocity AS velocity,                  
    FROM nexrad_table_DF             AS nexrad
    ORDER BY time
"""



# Check that key fields have valid values (no nulls or empty)
nexrad_table_check1_query = """
    SELECT  COUNT(*)
    FROM nexrad_table_DF
    WHERE   GateLat IS NULL OR GateLat == "" OR
            GateLon IS NULL OR GateLon == ""
"""

# Check that table has > 0 rows
nexrad_table_check2_query = """
    SELECT  COUNT(*)
    FROM nexrad_table_DF
"""