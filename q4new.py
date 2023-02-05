from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import time
import shutil
import glob

hdfs_path = "hdfs://192.168.0.1:9000/data/"

#create spark session
spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("Spark Session Started")

#initialize dataset
q4 = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-prwtoi6.parquet")
q4 =q4.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))

#add column for weekday and timezone
q4 = q4.withColumn("weekday", dayofweek(col("tpep_pickup_datetime")))
q4 = q4.withColumn("timezone", hour(col("tpep_pickup_datetime")))

#sql query
q4.createOrReplaceTempView("data")
query_help = spark.sql("""SELECT weekday, timezone, Average_Passenger_Count, row_number() OVER (PARTITION BY weekday ORDER BY Average_Passenger_Count DESC) as row_nr
FROM (
    SELECT weekday, timezone, SUM(Passenger_count) as Average_Passenger_Count
    FROM data
    GROUP BY weekday, timezone)""")

query_help.createOrReplaceTempView("newdata")
query4 = spark.sql(""" SELECT weekday, timezone, Average_Passenger_Count
FROM newdata
WHERE row_nr <= 3
ORDER BY weekday, row_nr""")

start = time.time()
query4.write.option("header","true").option("delimiter",",").csv("q4_result")
time_elapsed = time.time() - start
print("Time elapsed: ", time_elapsed) 

query4.show(21)
spark.stop()

files = glob.glob("./q4_result/*.csv")
shutil.move(files[0], "q4_resulv.csv")
shutil.rmtree("./q4_result")
