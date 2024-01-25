# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming Deduplication
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook|04-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)
# MAGIC
# MAGIC Eleminate duplicates will working with structured streaming, applying deduplication at the silver layer, with the bronze layer returning a history of the true state of the data source preventing data lose and minimizing data latency for data ingestion
# MAGIC

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

# MAGIC %run ../resources/local-setup

# COMMAND ----------

(
    spark.read
    .table("bronze")
    .filter("topic = 'orders'")
    .count()
)

# COMMAND ----------

from pyspark.sql.functions import col

bronze_df = spark.table("bronze")
duplication_count = (bronze_df
    .groupBy(bronze_df.columns)
    .count()
    .filter(col("count") > 1)
    .count()
)

display(dup_count)

# COMMAND ----------

# MAGIC %md
# MAGIC We have 300 duplicate records that need to be removed.

# COMMAND ----------

from pyspark.sql.functions import col, from_json

json_schema = "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

batch_total = (
    spark.read.table("bronze")
    .filter("topic = 'orders'")
    .select(from_json(col("value").cast("string"), json_schema).alias("v"))
    .select("v.*")
    .dropDuplicates(["order_id","order_timestamp"])
    .count()
)

print(batch_total)

# COMMAND ----------

# MAGIC %md
# MAGIC And 1200 records, so about 20% of the records are duplicates.
# MAGIC
# MAGIC We can also use `dropDuplicates` in structured streaming.

# COMMAND ----------

deduplication_bronze_df = (
    spark.readStream.table("bronze")
    .filter("topic = 'orders'")
    .select(from_json(col("value").cast("string"), json_schema).alias("v"))
    .select("v.*")
    .withWatermark("order_timestamp", "30 seconds")
    .dropDuplicates(["order_id","order_timestamp"])
)

# COMMAND ----------

def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("orders_microbatch")

    sql_query = """
        MERGE INTO orders_silver a
        USING orders_microbatch b
        ON a.order_id = b.order_id AND a.order_timestamp = b.order_timestamp
        WHEN NOT MATCHED THEN INSERT *
    """

    microBatchDF.sparkSession.sql(sql_query)
    # microBatchDF._jdf.sparkSession.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS orders_silver
# MAGIC (order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>)

# COMMAND ----------

query = (
    deduplication_bronze_df.writeStream
    .foreachBatch(upsert_data)
    .option("checkpointLocation",f"{checkpoint_path}/orders_silver")
    .trigger(availableNow=True)
    .start()
    )

query.awaitTermination()

# COMMAND ----------

streaming_total = spark.read.table("orders_silver").count()

print(f"Batch total: {batch_total}")
print(f"Streaming total: {streaming_total}")
