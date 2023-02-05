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
q3 = spark.read.option("header", "true").option("inferSchema", "true").parquet(hdfs_path + "yellow_tripdata_2022-prwtoi6.parquet")
q3=q3.filter((month(col("tpep_pickup_datetime")) >= 1) & (month(col("tpep_pickup_datetime")) <= 6))

#add column for fortnight
q3 = q3.withColumn("fortnight", (month(col("tpep_pickup_datetime"))-1)*2 + floor(dayofmonth(col("tpep_pickup_datetime"))/16))

#sql query
q3.createOrReplaceTempView("data")
query3 = spark.sql("""SELECT fortnight, AVG(Trip_distance) as Average_Distance, AVG(Total_amount) as Average_Fare
FROM data
WHERE PULocationID != DOLocationID
GROUP BY fortnight
ORDER BY fortnight""")

start = time.time()
query3.write.option("header","true").option("delimiter",",").csv("q3_result")
time_elapsed = time.time() - start
print("Time elapsed: ", time_elapsed) 

query3.show()
spark.stop()

files = glob.glob("./q3_result/*.csv")
shutil.move(files[0], "q3_resulv.csv")
shutil.rmtree("./q3_result")