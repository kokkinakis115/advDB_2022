from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os, sys, time
import shutil
import glob

#create spark session
spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
hdfs_path = "hdfs://192.168.0.1:9000/data/"

print("Spark Session Started")

#spark.conf.set("spark.executor.instances", 1)

#initialize dataset
q1 = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-prwtoi6.parquet")
zone_lookups_df = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_path + "taxi+_zone_lookup.csv")

#add column for month
q1new = q1.withColumn("month", month("tpep_pickup_datetime"))

#sql query
q1new.createOrReplaceTempView("data")
zone_lookups_df.createOrReplaceTempView("zone_lookups")
query1 = spark.sql("""
SELECT data.* 
FROM data, zone_lookups 
WHERE data.month == 3 and zone_lookups.Zone == "Battery Park" and data.DOLocationID == zone_lookups.LocationID
ORDER BY Tip_amount desc limit 1""")

start = time.time()
#query1.show()
query1.write.option("header","true").option("delimiter",",").csv("q1_result")
time_elapsed = time.time() - start
print("Time elapsed: ", time_elapsed) 
spark.stop()
files = glob.glob("./q1_result/*.csv")
shutil.move(files[0], "q1_resulv.csv")
shutil.rmtree("./q1_result")
