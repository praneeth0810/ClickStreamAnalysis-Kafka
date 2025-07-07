from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import from_json, col, to_date, hour

# Kafka Configuration
KAFKA_BROKER = " "
USER_CLICK_TOPIC = "user_click"

# Initialize SparkSession (without Hive support)
spark = SparkSession.builder \
    .appName("Process User Click Events - S3 Athena") \
    .getOrCreate()

# Define schema for user click events
user_click_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("page", StringType(), True),
    StructField("device", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", LongType(), True)  # epoch milliseconds
])

# Step 1: Read from Kafka
raw_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", USER_CLICK_TOPIC) \
    .load()

# Step 2: Parse and transform
user_click_df = raw_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), user_click_schema).alias("data")) \
    .select(
        col("data.event_type"),
        col("data.user_id"),
        col("data.page"),
        col("data.device"),
        col("data.location"),
        (col("data.timestamp") / 1000).cast(TimestampType()).alias("event_time")
    ) \
    .withColumn("dt", to_date("event_time")) \
    .withColumn("hr", hour("event_time"))

# Step 3: Write to S3 in partitioned Parquet
output_path = "s3:/clickstreamactivity/user_click" 
checkpoint_path = "s3://clickstreamactivity/checkpoints/user_click"

query = user_click_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .option("compression", "snappy") \
    .partitionBy("dt", "hr") \
    .trigger(processingTime="60 seconds") \
    .start()

query.awaitTermination()
