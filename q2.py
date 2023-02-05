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
q2 = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-prwtoi6.parquet")

q2=q2.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))

#add column for month
q2 = q2.withColumn("month", month("tpep_pickup_datetime"))

#sql query
q2.createOrReplaceTempView("data")

maxtolls = spark.sql("""SELECT *, row_number() OVER (PARTITION BY month ORDER BY Tolls_amount desc) as row_nr
FROM data"""
)
maxtolls.createOrReplaceTempView("maxtolls")

query2 = spark.sql("""SELECT *
FROM maxtolls
WHERE Tolls_amount != 0 and row_nr == 1""")

start = time.time()
query2.write.option("header","true").option("delimiter",",").csv("q2_result")
time_elapsed = time.time() - start
print("Time elapsed: ", time_elapsed) 

query2.show()
spark.stop()

files = glob.glob("./q2_result/*.csv")
shutil.move(files[0], "q2_resulv.csv")
shutil.rmtree("./q2_result")