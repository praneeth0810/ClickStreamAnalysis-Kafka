from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, hour

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Join User Click + Order Details - Athena S3") \
    .getOrCreate()

# S3 input locations
user_click_path = "s3:/clickstreamactivity/user_click"
order_details_path = "s3:/clickstreamactivity/user_click/order_details"

# Step 1: Read from Parquet on S3
user_click_df = spark.read.parquet(user_click_path)
order_details_df = spark.read.parquet(order_details_path)

# Step 2: Join on user_id to add location
joined_df = order_details_df \
    .join(user_click_df.select("user_id", "location"), on="user_id", how="left") \
    .select(
        order_details_df["user_id"],
        user_click_df["location"],
        order_details_df["event_type"],
        order_details_df["order_id"],
        order_details_df["order_amount"],
        order_details_df["transaction_id"],
        order_details_df["payment_method"],
        order_details_df["event_time"],
        order_details_df["item_id"],
        order_details_df["item_name"],
        order_details_df["item_price"]
    )

# Step 3: Add partition columns
final_df = joined_df \
    .withColumn("dt", to_date("event_time")) \
    .withColumn("hr", hour("event_time"))

# Step 4: Write to S3 (Parquet, partitioned)
output_path = "s3://clickstreamactivity/order_details_enhanced/"

final_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("compression", "snappy") \
    .partitionBy("dt", "hr") \
    .save(output_path)
