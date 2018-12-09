Turnstile Data:

Screenshots:
-----------------------
> There are screenshots in the screenshots/turnstile/ directory showing
output of running code alongside an example of the final dataset.

> Source:
http://web.mta.info/developers/turnstile.html
---------- End Screenshots -------------

Data Ingest:
-----------------------
> The data for the 2017 data can be downloaded from a source which is presented in a consolidated manner. I ran a web-scrapper to get some basic information added. The initial form is - 

C/A,UNIT,SCP,STATION,LINENAME,DIVISION,DATE,TIME,DESC,ENTRIES,EXITS                                                               
A002,R051,02-00-00,59 ST,NQR456W,BMT,12/31/2016,03:00:00,REGULAR,0005991546,0002028378                                            
A002,R051,02-00-00,59 ST,NQR456W,BMT,12/31/2016,07:00:00,REGULAR,0005991565,0002028389                                            
A002,R051,02-00-00,59 ST,NQR456W,BMT,12/31/2016,11:00:00,REGULAR,0005991644,0002028441                                            
A002,R051,02-00-00,59 ST,NQR456W,BMT,12/31/2016,15:00:00,REGULAR,0005991971,0002028502                                            
A002,R051,02-00-00,59 ST,NQR456W,BMT,12/31/2016,19:00:00,REGULAR,0005992418,0002028543                                            
A002,R051,02-00-00,59 ST,NQR456W,BMT,12/31/2016,23:00:00,REGULAR,0005992638,0002028572                                            
A002,R051,02-00-00,59 ST,NQR456W,BMT,01/01/2017,03:00:00,REGULAR,0005992718,0002028585                                            
A002,R051,02-00-00,59 ST,NQR456W,BMT,01/01/2017,07:00:00,REGULAR,0005992730,0002028594                                            
A002,R051,02-00-00,59 ST,NQR456W,BMT,01/01/2017,11:00:00,REGULAR,0005992776,0002028636                                            
A002,R051,02-00-00,59 ST,NQR456W,BMT,01/01/2017,15:00:00,REGULAR,0005992980,0002028680  
---------- End Data Ingest -------------

PuTTy
------------------------
> The code was run inside an SSH-ed terminal environment, with the help of the Dumbo cluster for the MapReduce part.

Data Cleaning/Profiling/Joining
------------------------
There were several steps to cleaning and profiling the data. They were mainly run on MapReduce, with the final steps on python.

> Initially the data was summed for the entries for each turnstile. Screenshots have been provided for the same. The code to run the same is given below - 

export HADOOP_LIPATH=/opt/cloudera/parcels/CDH-5.11.1-1.cdh5.11.1.p0.4/lib
javac -classpath $HADOOP_LIPATH/hadoop/*:$HADOOP_LIPATH/hadoop-0.20-mapreduce/*:$HADOOP_LIPATH/hadoop-hdfs/* *.java
jar cvf WordCount.jar *.class
hadoop jar EntryCount.jar EntryCount /user/ad4025/turnstile_Data2017 /user/ad4025/output

> Then the entries and exits were added and subtracted to get the total business and netflow respectively. The compile line for this is - 

hadoop jar StatsCount.jar StatsCount /user/ad4025/businessornetflow /user/ad4025/output

> Finally I ran a python script for trimming whitespaces and just cleaning data and adding the weather data.

python DataMerge.py

> An example of what the final set looks like is -

DateFrom;DateTo;StationName;Line;Business;Rain;PresenceRain;DepthSnow;SnowFall;PresenceSnow;TempAvg;TempMax;TempMin;WindSpeed;Fog;Thunder;Hail;Haze
2017-01-01 07:00;2017-01-01 11:00;25ST;R;393;0.00;0;0.0;0.0;0;46;49;39;10.1;0;0;0;0
2017-01-01 07:00;2017-01-01 11:00;KINGSTONAV;3;623;0.00;0;0.0;0.0;0;46;49;39;10.1;0;0;0;0
2017-01-01 07:00;2017-01-01 11:00;2AV;F;1589;0.00;0;0.0;0.0;0;46;49;39;10.1;0;0;0;0
2017-01-01 07:00;2017-01-01 11:00;50ST;1;2411;0.00;0;0.0;0.0;0;46;49;39;10.1;0;0;0;0
2017-01-01 07:00;2017-01-01 11:00;KINGSHWY;BQ;1686;0.00;0;0.0;0.0;0;46;49;39;10.1;0;0;0;0
2017-01-01 07:00;2017-01-01 11:00;111ST;7;1641;0.00;0;0.0;0.0;0;46;49;39;10.1;0;0;0;0
---------- End Data Cleaning/Profiling/Joining -------------



---------- Tableau Data Visualization -------------
> I finally use Tableau for a graph for Data visualization. This is to easily mark the trends in data, ie, spot whether the usage of subways drops for rainfalls or increases for snow etc.

> These graphs will be attached to the slides and on the final code.

> We can also draw some conclusions from these graphs and can potentially make comparisons to the taxi data as well.
---------- End Tableau Data Visualization -------------