from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, sum, avg, max, month, dayofmonth, expr, asc, desc
from pyspark.sql.window import Window
import time, datetime

# Create a Spark Session
spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("Spark session created")
hdfs_path = "hdfs://192.168.0.1:9000/data/"

# Read Taxi Trips
taxi_trips_df = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-prwtoi6.parquet")

# Filter tuples from Month 1 to 6
taxi_trips_df = taxi_trips_df.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))
taxi_trips_rdd = taxi_trips_df.rdd
taxi_trips_df.createOrReplaceTempView("taxi_trips")
taxi_trips_df.printSchema()

# Read Zone Lookups
zone_lookups_df = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_path + "taxi+_zone_lookup.csv")
zone_lookups_rdd = zone_lookups_df.rdd
zone_lookups_df.createOrReplaceTempView("zone_lookups")
zone_lookups_df.printSchema()
