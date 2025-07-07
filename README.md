# 📈 Clickstream Analytics Pipeline (Kafka → Spark → S3 → Athena)

This project builds a real-time data pipeline that captures user behavior and purchase activity, processes the data, stores it in the cloud, and makes it queryable using serverless SQL.

---

## 🚀 What This Project Does

- Simulates **user click events** and **order placed events** using a Kafka producer
- Streams data from Kafka using **Apache Spark Structured Streaming**
- Writes partitioned **Parquet files** to **Amazon S3** (compressed with Snappy)
- Enriches order data with user click context (like location)
- Makes all data queryable using **Amazon Athena**

---

## 🔁 Workflow Overview

```text
[ Python Producer (Faker) ]
            ⬇
     [ Kafka Topics ]
            ⬇
[ Spark Streaming Jobs (user + order) ]
            ⬇
[ Partitioned Parquet on S3 ]
            ⬇
   [ Athena External Tables ]
            ⬇
     [ SQL-based Insights ]
```

---

## 🔧 Tech Stack

- **Kafka** – event streaming and ingestion
- **Spark (Structured Streaming)** – real-time data processing
- **Amazon S3** – cloud storage for Parquet files
- **Athena** – serverless querying engine
- **Glue Catalog** – (optional) for table metadata
- **Python + Faker** – mock event generation

---

## ✅ Final Outcomes

- 💾 **Raw user and order events** stored in S3, partitioned by date and hour  
- 🔗 **Joined data** (orders enriched with user location) for analytics  
- 🔍 **Athena tables** created over S3 to support ad hoc SQL queries  
- 📊 **Sample business use cases** implemented via SQL, such as:
  - Average Order Value by Payment Method
  - Most Popular Items sold
  - Most Repeated purchases per customer
  - Top-spending users in a week
  - Total Order Volume by City

This setup gives you a production-like simulation of a clickstream pipeline, with zero infrastructure for querying and full compatibility with modern BI tools.

---
