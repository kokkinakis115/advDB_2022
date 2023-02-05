from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pandas
import time
import shutil
import glob

#create spark session
spark = SparkSession.builder.master("spark://192.168.0.1:7077").config("spark.dynamicAllocation.enabled", "false").getOrCreate()
print("Spark Session Started")

hdfs_path = "hdfs://192.168.0.1:9000/data/"

#initialize dataset
q2 = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-prwtoi6.parquet")
q2=q2.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))

#add column for month
q2 = q2.withColumn("month", month("tpep_pickup_datetime"))

#sql query
q2.createOrReplaceTempView("data")

query2 = spark.sql("""
SELECT data.*
FROM data INNER JOIN (SELECT month, MAX(Tolls_amount) as maxtolls 
FROM data GROUP BY month ) as max_tolls_table
ON data.Tolls_amount = max_tolls_table.maxtolls
ORDER BY data.month""")

start = time.time()
query2.write.option("header","true").option("delimiter",",").csv("q2_result")
time_elapsed = time.time() - start
print("Time elapsed: ", time_elapsed) 

query2.show()
spark.stop()

files = glob.glob("./q2_result/*.csv")
df = pandas.concat([pandas.read_csv(f) for f in files ], ignore_index=True)
df.to_csv("q2_result.csv", index=True)
#files = glob.glob("./q2_result2/*.csv")
#shutil.move(files[0], "q2_result.csv")
shutil.rmtree("./q2_result")
#shutil.rmtree("./q2_result2")
