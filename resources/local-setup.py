# Databricks notebook source
# MAGIC %md
# MAGIC ![Static Badge](https://img.shields.io/badge/Local-Setup-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)

# COMMAND ----------

name = spark.sql("SELECT current_user()").first()[0].split("@")[0]
database_name = f'{name}-udemy-professional'
db_name = f"`{database_name}`"
dataset_bookstore = f'dbfs:/mnt/{name}/bookstore'
checkpoint_path = f'{dataset_bookstore}/checkpoints'

spark.sql("USE CATALOG dvt_databricks")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{database_name}`")
spark.sql(f"USE SCHEMA `{database_name}`")

print(f"Using catalog: dvt_databricks")
print(f"Using schema: {database_name}")
print(f"dataset_bookstore: {dataset_bookstore}")
print(f"checkpoint_path: {checkpoint_path}")

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json

def bronze_json_parse(df:DataFrame, schema:str) -> DataFrame:
    """
    Parses JSON data in the 'value' column and selects fields based on the provided schema

    Args:
        df (DataFrame): Input DataFrame containing a column 'value' with JSON data
        schema (String): Schema string specifying the structure fo the JSON data

    Returns:
        DataFrame with the parsed JSON fields selected
    """

    return df.select(from_json(col("value").cast("string"), schema).alias("v")).select("v.*")

# COMMAND ----------

print("Available function: 'bronze_json_parse' \nUsage: '.transform(lambda df: bronze_json_parse(df, schema))'")
