
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
import numpy as np
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
import re
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import udf

def parseTime(text):
    month = {
        'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
        'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12
    }
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
      int(text[7:11]),month[text[3:6]],int(text[0:2]),int(text[12:14]),int(text[15:17]),int(text[18:20])
    )
    
##reference--https://opensource.com/article/19/5/log-data-apache-spark
sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)
#--------A  Loading log file
base_df = spark.read.text("access.log")
#base_df.show(2, truncate=False)
base_df.printSchema()

#REMOTE HOST
host = r'(^\S+\.[\S+\.]+\S+)\s'
#REMOTE TIMESTAMP
ts = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} (\+|-)\d{4})]'
date=r'(\d{2}/\w{3}/\d{4})'
#REQUEST METHODS
method = r'\"(\S+)\s(\S+)\s*(\S*)\"'
#STATUS CODE
status = r'\s(\d{3})\s'
#status CODE 
content = r'\s(\d+)\s\"'
#CREATE DATAFRAME TABLE
logs_df = base_df.select(regexp_extract('value', host, 1).alias('host'),
                         regexp_extract('value', date, 1).alias('date'),
                         regexp_extract('value', ts, 1).alias('timestamp'),
                         regexp_extract('value', method, 1).alias('method'),
                         regexp_extract('value', status, 1).cast('integer').alias('status'),
                         regexp_extract('value', content, 1).cast('integer').alias('length'))

#-----------C  Cleaning data

udftime = udf(parseTime)
logs_df = logs_df.select('*', udftime(logs_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp')
logs_df.show(10, truncate=False)

original=logs_df.count()
# REMOVE ROWS CONTAINING NULL VALUES
header=['host','time','method','status','length']
for i in header:
    logs_df = logs_df[logs_df[i].isNotNull()] 
new=logs_df.count()
#print (no of rows containing null values)
print("Number of bad Rows :",original-new)
print(original)
print(new)

#taskd start

#a
status_freq_df = (logs_df
                     .groupBy('status')
                     .count()
                     .sort('status')
                     .cache())
print("HTTP status analysis:")
status_freq_df.show()

#b

status_freq_pd_df = (status_freq_df
                         .toPandas())

fig = plt.figure()
ax = fig.add_axes([0,0,1,1])
ax.axis('equal')
val = status_freq_pd_df['count'].to_list()
lab = status_freq_pd_df['status'].to_list()

ax.pie(val, labels = lab,autopct='%1.2f%%')
plt.show()


#c

host_sum_df =(logs_df
               .groupBy('host')
               .count())

print("Frequent Hosts:")
host_sum_df.show()

#d

unique_host_count = (logs_df
                     .select('host')
                     .distinct()
                     .count())

print("Unique hosts")
print(unique_host_count)

#e

host_day_distinct_df = logs_df.select(logs_df.host,
                             logs_df.date.alias('day')).distinct()
                        
daily_hosts_df = (host_day_distinct_df
                     .groupBy('day')
                     .count()
                     .sort("day"))

print("Unique hosts per day:")
daily_hosts_df.show()

#f

daily_hosts_df = daily_hosts_df.toPandas()

plt.plot(daily_hosts_df["day"], daily_hosts_df["count"])
plt.show()

#g

error_df = logs_df.filter((logs_df["status"] >= 400 ) & (logs_df["status"]< 600)).cache()

hosts_error_count_df = (error_df
                          .groupBy("host")
                          .count()
                          .sort("count", ascending=False)
                          .limit(5))

hosts_error_count_df = hosts_error_count_df.toPandas()
print("Failed HTTP Clients")
print(hosts_error_count_df['host'].to_string(index = False))

#h

hour = F.udf(lambda x: x.hour, IntegerType())
hours = logs_df.withColumn("hour", hour("time"))
hourf = hours.filter(hours["date"] == "22/Jan/2019")

hourf_distinct = (hourf.groupBy('hour').count().sort("hour"))
hourf_distinct= hourf_distinct.toPandas()
plt.plot(hourf_distinct["hour"],hourf_distinct["count"],label = "Total requests")

#i

houre = error_df.withColumn("hour", hour("time"))
houre = houre.filter(houre["date"] == "22/Jan/2019")
houre_distinct = (houre.groupBy('hour').count().sort("hour"))
houre_distinct= houre_distinct.toPandas()
plt.plot(houre_distinct["hour"],houre_distinct["count"],label = "Error requests")
plt.legend()
plt.show()

print("Response length statistics:")

min_count = logs_df.select("length").rdd.min()[0]
max_count = logs_df.select("length").rdd.max()[0]

print("Minimum length ",min_count)
print("Maximum length ",max_count)








