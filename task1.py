from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("RideSharingTask1") \
    .getOrCreate()

# Step 2: Define schema
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Step 3: Read streaming data from socket
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Step 4: Parse JSON
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Step 5: Write parsed data to CSV
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "outputs/task1") \
    .option("checkpointLocation", "/tmp/checkpoints_task1") \
    .start()

query.awaitTermination()