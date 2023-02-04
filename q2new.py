from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import os, sys, time

#create spark session
spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("Spark Session Started")

hdfs_path = "hdfs://192.168.0.1:9000/data/"
#spark.conf.set("spark.executor.instances", 1)

#initialize dataset
q2 = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-prwtoi6.parquet")
q2=q2.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))

#add column for month
q2new = q2.withColumn("month", month("tpep_pickup_datetime"))

#sql query
q2new.createOrReplaceTempView("data")

query2 = spark.sql("""SELECT *
FROM data
INNER JOIN (SELECT month, MAX(Tolls_amount) as maxtolls FROM data GROUP BY month) as max_tolls_table
ON data.Tolls_amount = max_tolls_table.maxtolls"""
)

start = time.time()
query2.show()
time_elapsed = time.time() - start

print("Time elapsed: ", time_elapsed)

spark.stop()

