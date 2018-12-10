-----------------------
Taxi:
-----------------------


-----------------------
Screenshots:
-----------------------
There are screenshots in the screenshots/taxi/ directory showing
the output of running code in order to analyze green taxi data (in particular)
the processes for analyzing yellow cab and FHV data are virtually identical
so screenshots are not provided...



-----------------------
Data Source:
-----------------------
http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml



-----------------------
Data Ingest:
-----------------------
* For Green Cab Data (screenshot: "ingest"):
> curl -o green_tripdata_2018-06.csv https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-06.csv

* For Yellow Cab Data:
> curl -o yellow_tripdata_2018-05.csv https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-05.csv

* For FHV:
> curl -o fhv_tripdata_2017-11.csv https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2017-11.csv

All of these commands get one month of data, I ran each of these mutliple times to get all the data I needed.

* For taxi-zones (used for ETL processes):
> curl -o taxi-zone.csv https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

Finally, load all data into dumbo, e.g.
> hdfs dfs -put green* data/green/





-----------------------
Data ETL
-----------------------
See under etl_code/taxi/nyc-taxi/main/java/ :
IdToNeighborhoodJob, LocalTimeJob, LocalTimeMapper, LocalTimeReducer

I used maven to build/package all of my MapReduce source files,
so in order to run any of the below commands, please run (on dumbo):
> cd etl_code/taxi/nyc-taxi
> mvn clean package

Step 1:
The following removes unnecessary columns and removes malformed data (screenshot: "cleaning"):
> cd nyc-taxi
> hadoop jar target/nyc-taxi-1.0.jar LocationTimeJob data/green/*.csv data/green/cleaned

Usage:
hadoop jar target/nyc-taxi-1.0.jar LocationTimeJob <input path> <output path>


Step 2:
The following adds borough and location/neighborhood information (screenshot: "addBoro"):
> hadoop jar target/nyc-taxi-1.0.jar IdToNeighborhoodJob data/green/cleaned data/green/withBoro data/taxi_zone.csv

Usage:
hadoop jar target/nyc-taxi-1.0.jar IdToNeighborhoodJob <input path> <output path> <taxi zone path>




-----------------------
Data Profiling
-----------------------
See under etl_code/taxi/nyc-taxi/main/java/ :
DataProfiler

The following outputs <k, v> pairs counting the # of occurrences of a particular key
for a specified column of the data (screenshot: "data_prof"):
> hadoop jar target/nyc-taxi-1.0.jar DataProfiler data/green/*.csv data/green/profile 1

Usage:
hadoop jar target/nyc-taxi-1.0.jar IdToNeighborhoodJob <input path> <output path> <col ind>

Where <col ind> is the 0-based index of the column you want to profile



-----------------------
Data Iterations
-----------------------
See under etl_code/taxi/nyc-taxi/main/java/old/ :

This directory contains code from previous iterations of the process,
most of which doesn't compile anymore




-----------------------
Data Joining
-----------------------
See under etl_code/taxi/nyc-spark/ :
DataSchema, JoinWeatherAndFHV, JoinWeatherAndGreen, JoinWeatherAndYellow

For the next steps, I used sbt to build/package all of my spark source files,
so in order to run any of the below commands, please run (on dumbo):
> module load sbt
> cd etl_code/taxi/nyc-spark
> sbt package


The following joins taxi (Green cab) and weather data (screenshot: "join1" and "join2"):

> cd nyc-spark
> spark2-submit --class JoinWeatherAndGreen --master yarn target/scala-2.11/nyc-spark_2.11-0.1.jar data/green/withBoro data/weatherdata data/green/joined/

Usage:
spark2-submit --class JoinWeatherAndGreen --master yarn target/scala-2.11/nyc-spark_2.11-0.1.jar <input path> <weather data> <output path>

Use JoinWeatherAndYellow and JoinWeatherAndFHV for yellow cab and FHV resp.





-----------------------
Linear Regression
-----------------------
See under etl_code/taxi/nyc-spark/ :
PredGreen, RFGreen

The following creates a prediction model and saves it to some directory (screenshot: "linear_reg1" and "linear_reg2"):

> cd nyc-spark
> spark2-submit --class RFGreen --master yarn target/scala-2.11/nyc-spark_2.11-0.1.jar data/green/joined data/lr/output
Usage:
spark2-submit --class RFGreen --master yarn target/scala-2.11/nyc-spark_2.11-0.1.jar <input path> <output path>

The following loads a prediction model and predicts the usage for a given input data:
An example of the input is given under source_code/nyc-spark/test_data.csv
(screenshot green_pred1, green_pred2, green_pred3)

> cd nyc-spark
> hdfs dfs -put test_data.csv data/lr
> spark2-submit --class PredGreen target/scala-2.11/nyc-spark_2.11-0.1.jar data/lr/output data/lr/test_data.csv data/lr/output_data

Usage:
spark2-submit --class PredGreen --master yarn target/scala-2.11/nyc-spark_2.11-0.1.jar <model path> <input path> <output path>

As of right now, only green taxi works, and predictions are entirely accurate, sorry about that!





-----------------------
Impala Queries
-----------------------
See under etl_code/taxi/nyc-impala/ :
fhv.sql, greentaxi.sql, yellowtaxi.sql

These files contain SQL commands used to create tables/views and queries for the taxi data
These commands should be in the order in which they were run, see the inline comments

Screenshots (and what they show):
create_table - create table from joined data
create_view - create view with date fields
snow_by_boro - taxi usage on days where it did snow, grouped by borough
no_snow_by_boro - taxi usage on days where it didn't snow, grouped by borough
num_of_snow_days - count # of snow days
by_avg_temp - taxi usage vs. average temp
by_avg_temp_brooklyn - taxi usage vs. average temp for a particular borough (Brooklyn)

