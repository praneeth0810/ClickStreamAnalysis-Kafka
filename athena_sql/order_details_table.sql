CREATE EXTERNAL TABLE IF NOT EXISTS order_details (
  event_type STRING,
  order_id STRING,
  user_id BIGINT,
  order_amount DOUBLE,
  transaction_id STRING,
  payment_method STRING,
  event_time TIMESTAMP,
  item_id BIGINT,
  item_name STRING,
  item_price DOUBLE
)
PARTITIONED BY (dt DATE, hr INT)
STORED AS PARQUET
LOCATION "s3:/clickstreamactivity/user_click/order_details";
