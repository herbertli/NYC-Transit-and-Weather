import os
import sys
import datetime

f1 = open("/home/ad4025/rbda/project/part2/part3/part4/part5/newweatherdata")
f2 = open("/home/ad4025/rbda/project/part2/part3/part4/part5/final_netflow.txt")

x1 = f1.readlines()
x2 = f2.readlines()
for a1 in x1:
    a_1 = a1.split(";")
    date = a_1[0]
    for a2 in x2:
        a_2 = a2.split(";")
        if date==a_2[0]:
	    #print(a2+";"+a_1[1]+";"+a_1[2]+";"+a_1[3]+";"+a_1[4]+";"+a_1[5]+";"+a_1[6]+";"+a_1[7]+";"+a_1[8]+";"+a_1[9]+";"+a_1[10]+";"+a_1[11]+";"+a_1[12]+";"+a_1[13])
            print(a2+a1[11:])
            f3 = open("/home/ad4025/rbda/project/part2/part3/part4/part5/netflow_weather.txt","a")
	    f3.write(str(a2).strip()+str(a1[11:]));
            f3.close();
	else:
	    continue
            #f3 = open("/home/ad4025/rbda/project/part2/part3/part4/part5/business_weather.txt","a")
            #f3.write(a2);
            #f3.close();

	    	
















        
    
