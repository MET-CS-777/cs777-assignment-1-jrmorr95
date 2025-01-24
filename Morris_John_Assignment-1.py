from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    
    rdd = sc.textFile(sys.argv[1])
    split_rdd = rdd.map(lambda x: x.split(','))
    #Task 1
    taxilinesCorrected = split_rdd.filter(correctRows)
    medallion_driver_rdd=taxilinesCorrected.map(lambda x: (x[0], x[1]))
    med_driver_distinct=medallion_driver_rdd.distinct()
    med_driver_distinct_1= med_driver_distinct.map(lambda x: (x[0],1))
    med_driver_distinct_count=med_driver_distinct_1.reduceByKey(lambda x, y : x+y )

    top=med_driver_distinct_count.top(10,lambda x:x[1])
    top_taxis_array = [(x[0],x[1]) for x in top]

    sc.parallelize(top_taxis_array).coalesce(1).saveAsTextFile(sys.argv[2])


    #Task 2
    driver_money_time = taxilinesCorrected.map(lambda x: (x[1], float(x[16]) / (float(x[4]) * 60)))
    driver_money_rank=driver_money_time.reduceByKey(lambda x,y: x+y)
    driver_money_rank_sorted=driver_money_rank.sortBy(lambda x:x[1], ascending=False)
    top_10_drivers_and_counts = driver_money_rank_sorted.take(10)
    top_10_drivers_array = [(x[0],x[1])  for x in top_10_drivers_and_counts]



    #savings output to argument
    sc.parallelize(top_10_drivers_array).coalesce(1).saveAsTextFile(sys.argv[3])

    #Task 3 - Optional 
    #Your code goes here

    #Task 4 - Optional 
    #Your code goes here


    sc.stop()
