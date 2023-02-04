from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os, sys, time

hdfs_path = "hdfs://192.168.0.1:9000/"

#create spark session
spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("Spark Session Started")

#spark.conf.set("spark.executor.instances", 1)

#initialize dataset
#q2=spark.read.parquet("./data/yellow_tripdata_2022-prwtoi6.parquet")
q2 = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-prwtoi6.parquet")

q2=q2.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))

#add column for month
q2new = q2.withColumn("month", month("tpep_pickup_datetime"))

#sql query
q2new.createOrReplaceTempView("data")

maxtolls = spark.sql("""SELECT *, row_number() OVER (PARTITION BY month ORDER BY Tolls_amount desc) as row_nr 
FROM data"""
) 
maxtolls.createOrReplaceTempView("maxtolls")

query2 = spark.sql("""SELECT *
FROM maxtolls
WHERE Tolls_amount != 0 and row_nr == 1""") #edw logika prepei na valoume maxtolls == 1 gia na paroume to prwto ordered row alla den mou diavazei to alias maxtolls panw

start = time.time()
query2.show()
time_elapsed = time.time() - start

print("Time elapsed: ", time_elapsed) 

spark.stop()

