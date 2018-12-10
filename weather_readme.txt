Weather Data:

Source:
-------------------------
https://www.ncdc.noaa.gov/cdo-web/datasets#GHCND

Directories and Files:
-------------------------

Raw Data:
raw_data/weather_input.txt

Data Ingest Steps:
data_ingest/weather/

Schema:
data_schemas/weather_schema.txt

Cleaning Code:
etl_code/weather/WeatherCleaner.java

Profiling Code:
profiling_code/weather/WeatherProfiler.java

Screenshots:
There are screenshots in the screenshots/weather/clean_profile directory showing
how to compile and run the weather cleaner and profiler and find the outputs of those

Clean/Profile Steps:
-------------------------

View Input Data:
hdfs dfs -cat weather/weather_input.txt

Compile:
javac -classpath $(yarn classpath) -d . WeatherCleaner.java
javac -classpath $(yarn classpath) -d . WeatherProfiler.java
jar -cvf weather.jar *.class

Run Cleaner:
hadoop jar weather.jar WeatherCleaner weather/weather_input.txt weather/weather_cleaner_output

View Cleaner Output:
hdfs dfs -cat weather/weather_cleaner_output/part-r-00000

Run Profiler:
hadoop jar weather.jar WeatherProfiler weather/weather_cleaner_output/part-r-00000 project/weather_profiler_output

View Profiler Output:
hdfs dfs -cat weather/weather_profiler_output/part-r-00000