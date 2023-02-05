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
q5 = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-prwtoi6.parquet")
q5 = q5.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))

#add column for month, day_of_month and tip_percentage
q5 = q5.withColumn("day_of_month",dayofmonth(col("tpep_pickup_datetime")))
q5 = q5.withColumn("month", month(col("tpep_pickup_datetime")))
q5 = q5.withColumn("tip_perc", col("Tip_amount")/col("Fare_amount"))

#sql query
q5.createOrReplaceTempView("data")
query_help = spark.sql(""" SELECT day_of_month, month, Average_Tip_Percentage, row_number() OVER (PARTITION BY month ORDER BY Average_Tip_Percentage DESC) as row_nr
FROM (
    SELECT day_of_month, month, AVG(tip_perc) as Average_Tip_Percentage
    FROM data
    GROUP BY day_of_month, month)""")

query_help.createOrReplaceTempView("newdata")
query5 = spark.sql(""" SELECT *
FROM newdata
WHERE row_nr <= 5
ORDER BY month""")

start = time.time()
query5.show(21)
time_elapsed = time.time() - start
print("Time elapsed: ", time_elapsed) 

spark.stop()
