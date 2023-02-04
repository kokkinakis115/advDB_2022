from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os, sys, time

hdfs_path = "hdfs://192.168.0.1:9000/data/"

#create spark session
spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("Spark Session Started")

#spark.conf.set("spark.executor.instances", 1)

#initialize dataset
#q2=spark.read.parquet("./data/yellow_tripdata_2022-prwtoi6.parquet")
q3 = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-prwtoi6.parquet")
q3=q3.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))

#add column for month
q3new = q3.withColumn("fortnight", (month(col("tpep_pickup_datetime"))-1)*2 + floor(dayofmonth(col("tpep_pickup_datetime"))/15))

#sql query
q3new.createOrReplaceTempView("data")
query3 = spark.sql("""SELECT fortnight, AVG(Trip_distance) as Average_Distance, AVG(Fare_amount) as Average_Fare
FROM data
WHERE PULocationID != DOLocationID
GROUP BY fortnight 
ORDER BY fortnight""")

start = time.time()
query3.show()
time_elapsed = time.time() - start

print("Time elapsed: ", time_elapsed) 

spark.stop()
