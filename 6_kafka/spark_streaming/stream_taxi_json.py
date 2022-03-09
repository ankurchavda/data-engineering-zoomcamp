from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, FloatType, StructField, StructType
from pyspark.sql.functions import from_json, col

spark = SparkSession \
    .builder \
    .appName("Stream Taxi Data") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

records = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "yellow_taxi_ride.json") \
    .load()

records_df = records.selectExpr("CAST(value AS STRING)")

# vendor_count = records.groupBy('vendorId').count()
schema = StructType([
    StructField("vendorId", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("topic", FloatType(), True)
])

taxi_rides_df = records_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

taxi_rides_df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()