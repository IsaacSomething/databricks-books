# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake CDF (*Change Data Feed*)
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook|2.03-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)
# MAGIC
# MAGIC Is used to propergate incremental changes to down stream tables in a multi-hop architecture, automatically generating CDC (*Change Data Capture*) feeds out of Delta Lake tables. It records row-level changes for all the data written into a Delta table, adding metadata of the operation (*insert, update, delete*).
# MAGIC  - **`_change_type`:** operation
# MAGIC  - **`_commit_version`:** Delta table version
# MAGIC  - **`_commit_timestamp`:** when the operation was applied
# MAGIC
# MAGIC We can query the change data with `table_changes`
# MAGIC
# MAGIC `SELECT * FROM table_changes("<table_name>", <start_version>/<start_timestamp>, <end_version*>/<end_timestamp*>)`
# MAGIC
# MAGIC
# MAGIC **CDF is not enabled by default** 
# MAGIC <br />
# MAGIC You can enable it with
# MAGIC
# MAGIC  - `CREATE TABLE <table_name> (...) TBLPROPERTIES (delta.enableChangeDataFeed = true)`
# MAGIC  - `ALTER TABLE <table_name> SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`
# MAGIC
# MAGIC For all newly created Delta tables you can set it in a notebook or on a cluster with <br />
# MAGIC `spark.databricks.delta.properties.defaults.enableChangeDataFeed = true`
# MAGIC
# MAGIC CDF follows the same retention policy of the table, so when running `VACUUM` the CSF data will also be deleted.
# MAGIC
# MAGIC | When to use CDF | When **not** to use CDF |
# MAGIC | ---- | ---- |
# MAGIC | Tables include updated and/or deletes | Tables are append-only |
# MAGIC | Small fraction of records updated in each batch | Most records in the table updated in each batch |

# COMMAND ----------

# MAGIC %run ../resources/local-setup

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Enable CDF on the existing `customers_silver` table with `delta.enableChangeDataFeed = true`

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customers_silver
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_silver

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes("customers_silver", 2)

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

# COMMAND ----------

cdf_df = (
    spark.readStream
    .format("delta")
    .option("readChangeData",True)
    .option("startingVersion", 2)
    .table("customers_silver")
)

display(cdf_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC files = dbutils.fs.ls("dbfs:/user/hive/warehouse/.../customers_silver/_change_data)
# MAGIC display(files)
# MAGIC ```
