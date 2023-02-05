from pyspark.sql import SparkSession
import glob
import pandas

# create a Spark session
spark = SparkSession.builder.appName("ParquetConcatenation").getOrCreate()
# Concat .parquet files (from month 1 to 6)
files = glob.glob("./*.parquet")
df = pandas.concat([pandas.read_parquet(f) for f in files], ignore_index=True)
df.to_parquet("yellow_tripdata_2022-prwtoi7.parquet", index=True)
# stop the Spark session
spark.stop()

