from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, ArrayType
from pyspark.sql.functions import from_json, col, explode, to_date, hour

# Kafka Config
KAFKA_BROKER = ""
ORDER_DETAILS_TOPIC = "order_details_topic"

# Initialize Spark Session (no Hive needed)
spark = SparkSession.builder \
    .appName("Process Order Details Events - S3 Athena") \
    .getOrCreate()

# Define schemas
order_items_schema = StructType([
    StructField("item_id", LongType(), True),
    StructField("item_name", StringType(), True),
    StructField("price", DoubleType(), True)
])

order_details_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("timestamp", LongType(), True),  # epoch in ms
    StructField("items", ArrayType(order_items_schema), True)
])

# Step 1: Read from Kafka
raw_order_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", ORDER_DETAILS_TOPIC) \
    .load()

# Step 2: Parse JSON and convert timestamp
order_details_df = raw_order_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), order_details_schema).alias("data")) \
    .select(
        col("data.event_type"),
        col("data.order_id"),
        col("data.user_id"),
        col("data.order_amount"),
        col("data.transaction_id"),
        col("data.payment_method"),
        (col("data.timestamp") / 1000).cast(TimestampType()).alias("event_time"),
        col("data.items")
    )

# Step 3: Flatten items array
flattened_df = order_details_df \
    .withColumn("item", explode(col("items"))) \
    .select(
        col("event_type"),
        col("order_id"),
        col("user_id"),
        col("order_amount"),
        col("transaction_id"),
        col("payment_method"),
        col("event_time"),
        col("item.item_id").alias("item_id"),
        col("item.item_name").alias("item_name"),
        col("item.price").alias("item_price")
    ) \
    .withColumn("dt", to_date("event_time")) \
    .withColumn("hr", hour("event_time"))

# Step 4: Write to S3 in Parquet
output_path = "s3:/clickstreamactivity/user_click/order_details"
checkpoint_path = "s3://clickstreamactivity/checkpoints/order_details"

query = flattened_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .option("compression", "snappy") \
    .partitionBy("dt", "hr") \
    .trigger(processingTime="60 seconds") \
    .start()

query.awaitTermination()
