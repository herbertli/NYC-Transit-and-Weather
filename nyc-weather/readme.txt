Step 1 - Set Up:

copy weather_input.txt and Weather.java to local file system on Dumbo cluster
hdfs dfs -put weather_input.txt project

Step 2 - Clean/Profile Data:

javac -classpath $(yarn classpath) -d . Weather.java
jar -cvf weather.jar *.class
hadoop jar weather.jar Weather project/weather_input.txt project/weather_cleaner_output project/weather_profiler_output
