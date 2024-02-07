# Databricks notebook source
# MAGIC %md
# MAGIC # Materialized Gold Tables
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook|2.05-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)

# COMMAND ----------

# MAGIC %run ../resources/local-setup

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS countries_stats_vw AS (
# MAGIC     SELECT country, date_trunc("DD", order_timestamp) order_date, count(order_id) orders_count, sum(quantity) book_count
# MAGIC     FROM customer_orders
# MAGIC     GROUP BY country, date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **Delta caching** is a feature in Databricks were subsequent will return faster as result is cached for the currently active cluster (*not persisted*)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM countries_stats_vw
# MAGIC WHERE country = "France"

# COMMAND ----------

from pyspark.sql.functions import window, count, avg

query = (
    spark.readStream.table("book_sales")
    .withWatermark("order_timestamp", "10 minutes") #this doesnt make sense to add this to a microbatch operation that takes like 2 seconds to finish
    .groupBy(window("order_timestamp", "5 minutes").alias("time"),"author")
    .agg(
        count("order_id").alias("orders_count"),
        avg("quantity").alias("avg_quantity")
    )
    .writeStream
    .option("checkpointLocation", f"{checkpoint_path}/author_stats")
    .trigger(availableNow=True)
    .table("author_stats")
)

query.awaitTermination()

# COMMAND ----------

author_stats_df = spark.table("author_stats")
display(author_stats_df)
