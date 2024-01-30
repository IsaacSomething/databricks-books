# Databricks notebook source
# MAGIC %md
# MAGIC # Stream Stream Joins
# MAGIC
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook|2.04-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)

# COMMAND ----------

# MAGIC %run ../resources/local-setup

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window

def batch_upsert(micro_batch_df, batch_id):
    window = Window.partitionBy("order_id", "customer_id").orderBy(col("_commit_timestamp").desc())

    (
        micro_batch_df.filter(col("_change_type").isin(["insert", "update_postimage"]))
        .withColumn("rank", rank().over(window))
        .filter("rank = 1")
        .drop("rank", "_change_type", "_commit_version")
        .withColumnRenamed("_commit_timestamp", "processed_timestamp")
        .createOrReplaceTempView("ranked_updates")
    )

    query = """
        MERGE INTO customer_orders c
        USING ranked_updates r
        ON c.order_id = r.order_id AND c.customer_id = r.customer_id
            WHEN MATCHED AND c.processed_timestamp < r.processed_timestamp
                THEN UPDATE SET *
            WHEN NOT MATCHED
                THEN INSERT *
    """

    micro_batch_df.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customer_orders
# MAGIC (order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP, processed_timestamp TIMESTAMP)

# COMMAND ----------

def process_customers_orders():
    orders_df = spark.readStream.table("orders_silver")

    cdf_customers_df = (
        spark.readStream.option("readChangeData", True)
        .option("startingVersion", 2)
        .table("customers_silver")
    )

    query = (
        orders_df.join(cdf_customers_df, ["customer_id"], "inner")
        .writeStream
        .foreachBatch(batch_upsert)
        .option("checkpointLocation", f"{checkpoint_path}/customers_orders")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

# COMMAND ----------

process_customers_orders()

# COMMAND ----------

customer_orders_df = spark.table("customer_orders")
display(customer_orders_df)

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

process_customers_orders()

# COMMAND ----------

customer_orders_df = spark.table("customer_orders")
display(customer_orders_df)
