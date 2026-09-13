from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark session
spark = SparkSession.builder \
    .appName("API Attack Detection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "api_logs") \
    .load()

# Convert binary to string
json_df = df.selectExpr("CAST(value AS STRING)")

# Define schema
schema = StructType([
    StructField("ip", StringType()),
    StructField("endpoint", StringType()),
    StructField("timestamp", DoubleType())
])

# Parse JSON
parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Count requests per IP
agg_df = parsed_df.groupBy("ip").count()

# WRITE TO CSV (VERY IMPORTANT)
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

