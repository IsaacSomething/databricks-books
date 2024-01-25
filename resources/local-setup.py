# Databricks notebook source
# MAGIC %md
# MAGIC ![Static Badge](https://img.shields.io/badge/Local-Setup-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)

# COMMAND ----------

checkpoint_path = "dbfs/mnt/demo-datasets/bookstore/checkpoints"
database_name = f'{spark.sql("SELECT current_user()").first()[0].split("@")[0]}-udemy-professional' 

spark.sql("USE CATALOG dvt_databricks")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{database_name}`")
spark.sql(f"USE SCHEMA `{database_name}`")

print(f"Set variable: 'checkpoint_path' to '{checkpoint_path}'")
print(f"Using catalog: dvt_databricks")
print(f"Using schema: {database_name}")
