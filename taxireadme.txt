Taxi Data:

Screenshots:
-----------------------
There are screenshots in the screenshots/taxi/ directory showing
output of running code in order to analyze green taxi data

Source:
http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

Data Ingest:
-----------------------
* For Green Cab Data (screenshot: "ingest"):
> curl -o green_tripdata_2018-06.csv https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-06.csv

* For Yellow Cab Data:
> curl -o yellow_tripdata_2018-05.csv https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-05.csv

* For FHV:
> curl -o fhv_tripdata_2017-11.csv https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2017-11.csv

All of these commands get one month of data, I ran each of these mutliple times to get all the data I needed.

* For taxi-zones (used for ETL):
> curl -o taxi-zone.csv https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
---------- End Data Ingest -------------

Maven
------------------------
I used maven to build/package all of my MapReduce source files,
so in order to run any of the below commands, please run (on dumbo):
> module load mvn
> cd nyc-taxi
> mvn clean package

Data Cleaning/Profiling
------------------------
The following removes unnecessary columns and removes malformed data (screenshot: "cleaning"):
> cd nyc-taxi
> hadoop jar target/nyc-taxi-1.0.jar LocationTimeJob data/green/*.csv data/green/cleaned

Usage:
hadoop jar target/nyc-taxi-1.0.jar LocationTimeJob <input path> <output path>

The following adds borough and location/neighborhood information (screenshot: "addBoro"):
> cd nyc-taxi
> hadoop jar target/nyc-taxi-1.0.jar IdToNeighborhoodJob data/green/cleaned data/green/withBoro data/taxi_zone.csv

Usage:
hadoop jar target/nyc-taxi-1.0.jar IdToNeighborhoodJob <input path> <output path> <taxi zone path>
---------- End Data Cleaning/Profiling -------------

Spark
-------------------------
I used sbt to build/package all of my spark source files,
so in order to run any of the below commands, please run (on dumbo):
> module load sbt
> cd nyc-spark
> sbt package

Data Joining
------------------------
The following joins taxi (Green cab) and weather data (screenshot: "join1" and "join2"):

> cd nyc-spark
> spark2-submit --class JoinWeatherAndGreen --master yarn target/scala-2.11/nyc-spark_2.11-0.1.jar data/green/withBoro data/weatherdata data/green/joined/

Usage:
spark2-submit --class JoinWeatherAndGreen --master yarn target/scala-2.11/nyc-spark_2.11-0.1.jar <input path> <weather data> <output path>

Use JoinWeatherAndYellow and JoinWeatherAndFHV for yellow cab and FHV resp.
---------- End Data Joining -------------

Random Forest
------------------------
The following create a prediction model and saves it to some directory (screenshot: "rf_green"):

> cd nyc-spark
> spark2-submit --class RFGreen --master yarn target/scala-2.11/nyc-spark_2.11-0.1.jar <input path> <output path>
Usage:
spark2-submit --class RFGreen --master yarn target/scala-2.11/nyc-spark_2.11-0.1.jar <input path> <output path>

The following loads a prediction model and predicts the usage for a given date under given conditions:
(Follow the prompts :D to generate a usage prediction)

> cd nyc-spark
> spark2-submit --class PredGreen --master yarn target/scala-2.11/nyc-spark_2.11-0.1.jar <model path>

Usage:
spark2-submit --class PredGreen --master yarn target/scala-2.11/nyc-spark_2.11-0.1.jar <model path>

As of right now, only green taxi works, sorry about that!
---------- End Random Forest -------------

Impala Queries
------------------------
See nyc-impala/ for the SQL commands used to create tables/views and queries for the taxi data
Screenshots (and what they show):
create_table - create table from joined data
create_view - create view with date fields
snow_by_boro - taxi usage on days where it did snow, grouped by borough
no_snow_by_boro - taxi usage on days where it didn't snow, grouped by borough
num_of_snow_days - count # of snow days
by_avg_temp - taxi usage vs. average temp
by_avg_temp_brooklyn - taxi usage vs. average temp for a particular borough (Brooklyn)
---------- End Impala Queries -------------
