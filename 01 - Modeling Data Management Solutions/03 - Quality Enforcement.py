# Databricks notebook source
# MAGIC %md
# MAGIC # Quality Enforcement
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook|1.03-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)
# MAGIC
# MAGIC Use table constraints to ensure the quailty of the data. Check constraints apply boolean filters to columns and prevent data violating these constraints from being written

# COMMAND ----------

# MAGIC %run ../resources/local-setup

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Alter the table with a constraint to check that the timestamp is greater than 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT timestamp_within_range CHECK (order_timestamp >= '2020-01-01')

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Check the constraint by adding a value less than 2020. The `INSERT` will fail completely as the transaction is ACID compliant 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_silver
# MAGIC VALUES 
# MAGIC ('1', '2022-02-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC ('2', '2019-02-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC ('3', '2023-02-01 00:00:00.000', 'C00001', 0, 0, NULL)

# COMMAND ----------

# MAGIC %md 
# MAGIC We can try add a new constraint, checking that the quantity is greater than 0, however this will fail as there are already values in the table that meet this constraint. The contstraint will not be added to the table

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT valid_quantity CHECK (quantity > 0)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

orders_silver_quantity_zero_df = spark.table("orders_silver").filter("quantity <= 0")
display(orders_silver_quantity_zero_df)

# COMMAND ----------

from pyspark.sql.functions import from_json, col

json_schema = "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (
    spark.readStream.table("bronze")
    .filter("topic = 'orders'")
    .select(from_json(col("value").cast("string"),json_schema).alias("v"))
    .select("v.*")
    .filter("quantity > 0")
    .writeStream
    .option("checkpointLocation", f"{checkpoint_path}/orders_silver")
    .trigger(availableNow=True)
    .table("orders_silver")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md 
# MAGIC Remove the `timestamp_within_range` constraint with `DROP CONTSTRAINT`

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver DROP CONSTRAINT timestamp_within_range

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_silver

# COMMAND ----------

dbutils.fs.rm(f"{checkpoint_path}/orders_silver", True)
