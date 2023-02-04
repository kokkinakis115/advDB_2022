from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os, sys, time

#create spark session
spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("Spark Session Started")

spark.conf.set("spark.executor.instances", 1)

#initialize dataset
q1=spark.read.parquet("../data/yellow_tripdata_2022-prwtoi6.parquet")

#add column for month
q1new = q1.withColumn("month", month("tpep_pickup_datetime"))

start = time.time()

#sql query
q1new.createOrReplaceTempView("data")
query1 = spark.sql("select * from data where month == 3 and DOLocationID == 12 order by Tip_amount desc limit 1")
query1.show()

time_elapsed = time.time() - start
print("Time elapsed: ", time_elapsed) 
