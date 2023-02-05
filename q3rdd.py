
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os, sys, time
import shutil
import glob

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

hdfs_path = "hdfs://192.168.0.1:9000/data/"

#create spark session
spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("Spark Session Started")

#initialize dataset
q3 = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-prwtoi6.parquet")
q3 = q3.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))

#add column for fortnight
q3 = q3.withColumn("fortnight", (month(col("tpep_pickup_datetime"))-1)*2 + floor(dayofmonth(col("tpep_pickup_datetime"))/16))
 
q3_rdd = q3.rdd

#map
rdd_res = q3_rdd.filter(lambda x: x.DOLocationID != x.PULocationID)\
.map(lambda x: (int(x.fortnight), (float(x.total_amount), float(x.trip_distance), 1)))\
.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))\
.mapValues(lambda x: (x[0]/x[2], x[1]/x[2]))  


start = time.time()

print()
for x in rdd_res.collect():
    print(x)

time_elapsed = time.time() - start

print("Time elapsed: ", time_elapsed)

spark.stop()
