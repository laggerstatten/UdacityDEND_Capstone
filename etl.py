from all_etl import *
from hurdat_etl import *
from nexrad_etl import *
from sql_queries import *

def main():
    
    # Verify Spark by using findspark and computing Pi
    spark_verify()
    
    print("ETL pipeline initiated")
    
    # Create list to receive data QC results
    results_all = []
    
    # Define paths to import / export data
    path_d = define_paths()
    
    # Create Spark session using findspark
    spark = create_spark_session()

    # Process all HURDAT input files
    hurdat_df_spark = process_hurdat_data(spark, path_d)
    hurdat_storms_df_spark = hurdat_df_spark[0]
    hurdat_tracks_df_spark = hurdat_df_spark[1]
    
    # Join storm table to track table on strom identifier
    hurdat_table = process_joined_hurdat_data( spark, path_d, hurdat_storms_df_spark, hurdat_tracks_df_spark)
    
    # Create datetime heirarchy table
    time_table = process_hurdat_time_data( spark, path_d, hurdat_tracks_df_spark)

    # Create spatial heirarchy table
    hurdat_tracks_df = hurdat_df_spark[2]
    hurdat_space_df_spark = process_hurdat_space_data( spark, path_d, hurdat_tracks_df)

    # Process all NEXRAD input files
    nexrad_df_spark = process_nexrad_data(spark, path_d)
    nexrad_stations_df_spark = nexrad_df_spark[0]
    nexrad_samples_df_spark = nexrad_df_spark[1]

    
    
    hurdat_results = check_hurdat_data_quality( spark, hurdat_table)
    results_all.append(hurdat_results)    
    
    nexrad_results = check_nexrad_data_quality( spark, nexrad_samples_df_spark)
    results_all.append(nexrad_results)

    print(results_all)

    print("ETL pipeline complete")

if __name__ == "__main__":
    main()