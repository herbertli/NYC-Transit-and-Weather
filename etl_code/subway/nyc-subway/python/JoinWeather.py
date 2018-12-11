import os
import sys
import datetime

f1 = open("/home/ad4025/rbda/project/part2/part3/part4/part5/part6/business_weather.txt")

x1 = f1.readlines()

for a in x1:
    a1 = a.split(";")
    datetime1 = a1[0] +" "+a1[1]
    datetime2 = a1[2] +" "+a1[3]
    print(datetime1+";"+datetime2+a[33:])
    f3 = open("/home/ad4025/rbda/project/part2/part3/part4/part5/part6/business_weather2.txt","a")
    f3.write(datetime1+";"+datetime2+a[33:]);
    f3.close();
