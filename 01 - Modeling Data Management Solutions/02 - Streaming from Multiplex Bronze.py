# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming From Multiplex Bronze
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook/02-white?style=for-the-badge&logo=databricks)

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

database_name = f'{spark.sql("SELECT current_user()").first()[0].split("@")[0]}-udemy-professional' 
spark.sql("USE CATALOG dvt_databricks")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{database_name}`")
spark.sql(f"USE SCHEMA `{database_name}`")

checkpoint_path = "dbfs/mnt/demo-datasets/bookstore/checkpoints"
