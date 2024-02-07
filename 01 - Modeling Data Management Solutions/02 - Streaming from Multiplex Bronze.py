# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming From Multiplex Bronze
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook|1.02-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)
# MAGIC
# MAGIC Pass data of a single topic from the multiplex bronze table into a newly created silver orders table
# MAGIC
# MAGIC ![](/files/tcuthbert/overview.png)

# COMMAND ----------

# MAGIC %run ../resources/local-setup

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

bronze_df = spark.table("bronze")
display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cast binary columns **key** and **value** so that they are readable as text values

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(key as STRING), cast(value AS STRING)
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC Return 1 value to be used as in `schema_of_json` in order to get the schema for the `books`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, cast(value AS STRING) FROM bronze LIMIT 1

# COMMAND ----------

from pyspark.sql.functions import col

bronze_values_df = (
    spark.table("bronze")
    .withColumn("value", col("value").cast("string"))
    .withColumn("key", col("key").cast("string"))
    .limit(1)
)

json_string = bronze_values_df.toJSON().collect()[0]
print(json_string)


# COMMAND ----------

# MAGIC %md
# MAGIC Use the values above to with `schema_of_json` to get the schema of the values

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT schema_of_json('{"book_id":"B01","title":"The Soul of a New Machine","author":"Tracy Kidder","price":49,"updated":"2021-11-07 17:11:33.507"}') AS schema

# COMMAND ----------

# MAGIC %md
# MAGIC Create a view table of how we would like the file table to be added

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "orders"
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Kill the stream above
# MAGIC
# MAGIC Lets now convert this logic to a streaming read process. <br />
# MAGIC **First** convert out static table into a streaming temporary view. This allows us to write streaming queries with Spark SQL

# COMMAND ----------

spark.readStream.table("bronze").createOrReplaceTempView("bronze_tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can use the same SQL but reference the `bronze_tmp` view which has been created as a stream from the `bronze` table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze_tmp
# MAGIC   WHERE topic = "orders"
# MAGIC )

# COMMAND ----------

# MAGIC %md 
# MAGIC We can create the logic for the query in a `TEMPORARY VIEW` so that we can pass it back to Python code by referencing it as a Dataframe

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_silver_tmp AS
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze_tmp
# MAGIC   WHERE topic = "orders"
# MAGIC )

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC Then create the `orders_silver` table with a `writeStream` as a micro-batch operation

# COMMAND ----------

query = (
    spark.table("orders_silver_tmp")
      .writeStream.option("checkpointLocation", f"{checkpoint_path}/orders_silver")
      .trigger(availableNow=True)
      .table("orders_silver")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Lets now combine everything we have done and express this complete process with pyspark in one cell

# COMMAND ----------

from pyspark.sql.functions import from_json, col

json_schema = "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (
    spark.readStream.table("bronze")
    .filter("topic = 'orders'")
    .select(from_json(col("value").cast("string"),json_schema).alias("v"))
    .select("v.*")
    .writeStream
    .option("checkpointLocation", f"{checkpoint_path}/orders_silver")
    .trigger(availableNow=True)
    .table("orders_silver")
)

query.awaitTermination()

# COMMAND ----------

orders_silver_df = spark.table("orders_silver")
display(orders_silver_df)
