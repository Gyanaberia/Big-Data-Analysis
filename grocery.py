from pyspark.sql import SparkSession
import csv
sp=SparkSession.builder.appName("lab7").getOrCreate()

def toCSVLine(data):
    temp=[data[0][0],data[0][1],data[1]]
    return ','.join(str(d) for d in temp)

file="groceries.csv"

with open(file,'r') as csvfile:
    csvr=csv.reader(csvfile)
    values=[]
    for row in csvr:
        temp=list(filter(lambda x: x!='',row))
        if len(temp)>1:
            for i in range(len(temp)-1):
                for j in range(i+1,len(temp)):
                    values.append((temp[i],temp[j]))
#print(values)
outRDD=sp.sparkContext.parallelize(values)
#part c
blah=outRDD.map(lambda x: (x, 1)).groupByKey().mapValues(len)
print(blah.takeOrdered(5, key = lambda x: -x[1]))

lines = blah.map(toCSVLine).collect()
with open("count.csv", 'w+') as csvfile:
    csvw = csv.writer(csvfile)
    for i in lines:
        #print(i)
        i=i.split(",")
        csvw.writerow(i)
#lines.saveAsTextFile("./count.csv")
sp.stop()
