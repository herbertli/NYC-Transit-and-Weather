# Analyzing NYC Taxi Data

## About
Part of a Real-time and Big Data Analytics Project.

**General Idea**: Analyze trends in public transit data (subway and taxi) and its correlation with the weather.
This repository contains ETL, cleaning, and profiling jobs for NYC Taxi Data.

## Questions
1. When it is raining, how much more people use taxis?
2. When it is snowing, how much more people use taxis?
3. When it is hot/cold, how much more people use taxis?
4. How does the weather affect taxi trip distance? 
5. Look at these questions limited to certain areas...
    * Are there certain hotspots where there are more people getting on taxis?
    * Are there certain hotspots where there are more people getting off taxis?
    * Do these hotpots change according to the weather?


## Data Links:
* [Trip Data](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)
* [Taxi Zone Maps and Lookup Tables](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)
 
## Data Ingest:

* For Green Cab:
> curl -o green_tripdata_2018-06.csv https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-06.csv

* For Yellow Cab:
> curl -o yellow_tripdata_2018-05.csv https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-05.csv

* For FHV:
> curl -o fhv_tripdata_2017-11.csv https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2017-11.csv

Move to HDFS using: 
> hdfs dfs -put data/

Where data/ contains sub-folders containing data for yellow/green cabs and FHV

## Data Profiling:

## Data Schema:
