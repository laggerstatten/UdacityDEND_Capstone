# Project Title
### Data Engineering Capstone Project

#### Project Summary
--describe your project at a high level--

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up



### Step 1: Scope the Project and Gather Data

#### Scope 
*Explain what you plan to do in the project in more detail. What data do you use? What is your end solution look like? What tools did you use?*

> This data can be used for historic reconstruction of hurricanes. This would be used in the underlying database for analytics and developing tropical weather models.

> The data is spatially and temporally indexed, so a user can retrieve records within a given space and time window. 

> The main tools I used are Spark for data processing, ARM's pyart package to process radar files, and SidewalkLabs/Google's S2 package for spatial indexing.

> https://medium.com/@ligz/installing-standalone-spark-on-windows-made-easy-with-powershell-7f7309799bc7
> https://arm-doe.github.io/pyart/
> https://www.sidewalklabs.com/blog/s2-cells-and-space-filling-curves-keys-to-building-better-digital-map-tools-for-cities/


#### Describe and Gather Data 
*Describe the data sets you're using. Where did it come from? What type of information is included?*

HURDAT
> NOAA's Hurricane Research Division maintains the HURDAT project which analyzes tropical storm tracks and strengths. The data includes the geographic position of each storm at 6-hour intervals. This data includes wind speed and air pressure for the storm at each of these readings.

> At the time of this project, the HURDAT database includes about 52 thousand rows (storm track points).

> https://www.aoml.noaa.gov/hrd/data_sub/re_anal.html

NEXRAD
> The National Weather Servive operates NEXRAD Dopplar Radar stations across the US, which operate continuously through the day. The primary products of Dopplar Radar measurements are the reflectivity and velocity of the atmosphere.

> A single NEXRAD file contains 11 million records, representing reflecivity/ velocity measurements for one radar station at one point in time. Thousands of NEXRAD files are generated per day, so this project only uses a single file for proof of concept.

> https://www.ncdc.noaa.gov/data-access/radar-data/noaa-big-data-project
> https://s3.amazonaws.com/noaa-nexrad-level2/index.html
> https://www.nsstc.uah.edu/users/brian.freitag/AWS_Radar_with_Python.html




### Step 2: Explore and Assess the Data
#### Explore the Data 
*Identify data quality issues, like missing values, duplicate data, etc.*

HURDAT
* The CSV data on the webpage is a convoluted table with header rows interspersed within table. These child rows need to be associated with the header above them.
* The latitude and longitude values are in mixed character format (e.g. 90.5W) and need to be converted to a decimal number (e.g. -90.5).
* The date and time are in two separate columns and need to be combined into a datetime field.

NEXRAD
* Reflectivity and velocity data is in azimuth-elevation (polar) coordinates, rather than lat-lon (Cartesian) coordinates
* Data is in a 6480 by 1832 masked matrix, and needs to be reshaped to have one measurement per row
* This is a sparse data set, which only records value if these is measured reflectivity / velocity to record, and empty values are masked.
* The timestamps for each row index off of the radar scan start time, and this start time embedded in text and needs to be extracted.

#### Cleaning Steps
*Document steps necessary to clean the data*

* HURDAT header rows are flagged, and iterated over table to associate child rows with parent row.
* Mixed character latitude / longitude values are parsed and converted to numeric values.
* Split date and time fields are parsed and converted to a single datetime field.
* Polar coordinates are tranformed to grid coordinates.
* Rectangular arrays are reshaped to narrow arrays.
* Masked values are filtered out to reduce number of rows loaded.
* NEXRAD scan start time is extracted from text, converted to datetime, and applied to time deltas for each record.


* converted lat / lon to S2 cell -- not a cleaning task


   
### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
*Map out the conceptual data model and explain why you chose that model*
> The chosen star data model best allows the user to retrieve the fact tables' climate/weather data (tropical storm tracks with wind speed and pressure, reflectivity and velocity measurements) by their association with the dimension tables: spatial index and temporal index. 

HURDAT
* fact table: track point: windspeeds, pressure, lat, lon, S2 cell, time, storm
* dim table: storm: storm info
* dim table: spatial: S2 cell, centroid, parent cell
* dim table: time: datetime heirarchy

NEXRAD
* fact table: sample: reflectivity, velocity, lat, lon, S2 cell, alt, time, station
* dim table: station: station info    
* dim table: spatial: S2 cell, centroid, parent cell
* dim table: time: datetime heirarchy

#### 3.2 Mapping Out Data Pipelines
*List the steps necessary to pipeline the data into the chosen data model*

Broad outline:
1. import packages, SQL snippets, and methods
2. verify Spark can run in environment
3. define paths to input data
4. create Spark session
5. process input files, store data as parquet files, output is storms dataframe, tracks dataframe
6. join resulting dataframes, store data as parquet files, output is joined table
7. perform data QC checks on table


### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
*Build the data pipelines to create the data model.*




#### 4.2 Data Quality Checks
*Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:*
 * *Integrity constraints on the relational database (e.g., unique key, data type, etc.)*
 * *Unit tests for the scripts to ensure they are doing the right thing*
 * *Source/Count checks to ensure completeness*
 
*Run Quality Checks*



#### 4.3 Data dictionary 
*Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.*




#### Step 5: Complete Project Write Up
* *Clearly state the rationale for the choice of tools and technologies for the project.*
> The main tools I used are Spark for data processing, ARM's pyart package to process radar files, and SidewalkLabs/Google's S2 package for spatial indexing.
> Pandas and Spark are easy to use tools for reading, restructuring, and building tables to contain large amounts of tabular data, such as storm tracks and Dopplar radar measurements.
> The pyart package is the most mature package with ability to digest CF/Radial netCDF files such as those used by the National Weather Service to store NEXRAD data.
> The S2 package is the most mature package for spatial indexing at multiple scales. Sidewalk Labs is a subsidiary of Alphabet (Google), and S2 cells are even used in Pokemon Go. 


* *Propose how often the data should be updated and why.*
> answer here


* *Write a description of how you would approach the problem differently under the following scenarios:*
 * *The data was increased by 100x.*
> This project only reads a single NEXRAD file. In reality, there are over 150 NEXRAD stations, and each produces 140 to 290 files per day. If this pipeline were used to actually process that much data, it should be moved to a cloud environment rather than local installation. Clustered Spark could be used for parallel processing.
 * *The data populates a dashboard that must be updated on a daily basis by 7am every day.*
> NEXRAD files are continuously produced during the day, so they should be processed as soon as they are available in order to level out processor demand during the day. After processing the data should be stored in a cloud database for easy access. The spatial and temporal indexing of the data allows for easier data retrieval.
 * *The database needed to be accessed by 100+ people.*
> The storage and ETL pipeline should be moved from the local environment to a cloud environment. Each user should define a spatial and temporal window for the data they wish to access. This way the data can be partitioned in an efficient manner, placing data with spatial/temporal index within that window closest to that user.
















