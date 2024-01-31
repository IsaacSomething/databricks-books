# Databricks notebook source
# MAGIC %md
# MAGIC ![Static Badge](https://img.shields.io/badge/Local-cleanup-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)

# COMMAND ----------

name = spark.sql("SELECT current_user()").first()[0].split("@")[0]
dbutils.fs.rm(f"/mnt/${name}", True)
print(f"Removed folder 'mnt/{name}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG default;
# MAGIC DROP TABLE IF EXISTS 
