# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake CDF (*Change Data Feed*)
# MAGIC
# MAGIC Is used to propergate incremental changes to down stream tables in a multi-hop architecture, automatically generating CDC (*Change Data Capture*) feeds out of Delta Lake tables. It records row-level changes for all the data written into a Delta table, adding metadata of the operation (*insert, update, delete*).
# MAGIC  - **Change Type:** operation
# MAGIC  - **Timestamp:** when the operation was applied
# MAGIC  - **Version:** Delta table version
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
