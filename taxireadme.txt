Taxi Data:

Source:
http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

Data Ingest:
-----------------------
* For Green Cab Data:
> curl -o green_tripdata_2018-06.csv https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-06.csv

* For Yellow Cab Data:
> curl -o yellow_tripdata_2018-05.csv https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-05.csv

* For FHV:
> curl -o fhv_tripdata_2017-11.csv https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2017-11.csv

All of these commands get one month of data, I ran each of these mutliple times to get all the data I needed.

* For taxi-zones (used for ETL):
> curl -o taxi-zone.csv https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
---------- End Data Ingest -------------


I used maven to build/package all of my source files,
so in order to run any of the below commands please run (on dumbo):
> module load mvn
> cd nyc-taxi
> mvn clean compile package

Data Cleaning/Profiling
------------------------


I used sbt to build/package all of my source files,
so in order to run any of the below commands please run (on dumbo):
> module load sbt
> cd nyc-spark
> sbt package

Data Joining
------------------------

Random Forest
------------------------

Impala Queries
------------------------
See nyc-impala/ for the SQL used to create tables/views and queries