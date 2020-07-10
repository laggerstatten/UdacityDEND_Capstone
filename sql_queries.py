hurdat_table_createquery = """
    SELECT DISTINCT RecordIdentifier AS track_id, 
                    StormIdentifier  AS storm_id, 
                    StormName AS storm_name, 
                    StormSamples AS sample_count, 
                    Date AS date, 
                    Time AS time, 
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
    ORDER BY track_id
"""


# Check that key fields have valid values (no nulls or empty)
hurdat_table_check1_query = """
    SELECT  COUNT(*)
    FROM hurdat_table_DF
    WHERE   track_id IS NULL OR track_id == "" OR
            storm_id IS NULL OR storm_id == ""
"""

# Check that table has > 0 rows
hurdat_table_check2_query = """
    SELECT  COUNT(*)
    FROM hurdat_table_DF
"""