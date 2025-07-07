CREATE EXTERNAL TABLE IF NOT EXISTS user_click (
  event_type STRING,
  user_id BIGINT,
  page STRING,
  device STRING,
  location STRING,
  event_time TIMESTAMP
)
PARTITIONED BY (dt DATE, hr INT)
STORED AS PARQUET
LOCATION "s3:/clickstreamactivity/user_click";
