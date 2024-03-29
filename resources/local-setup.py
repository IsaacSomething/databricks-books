# Databricks notebook source
# MAGIC %md
# MAGIC ![Static Badge](https://img.shields.io/badge/Local-Setup-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)

# COMMAND ----------

name = spark.sql("SELECT current_user()").first()[0].split("@")[0]
database_name = f'{name}_ud_pr'
db_name = f'`{database_name}`'
dataset_bookstore = f'dbfs:/mnt/{database_name}/bookstore'
checkpoint_path = f'{dataset_bookstore}/checkpoints'

spark.sql("USE CATALOG hive_metastore")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{database_name}`")
spark.sql(f"USE SCHEMA `{database_name}`")

print(f"Using catalog: hive_metastore")
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

print("Available function: 'bronze_json_parse' \nUsage: '.transform(lambda df: bronze_json_parse(df, schema))'")

# COMMAND ----------

def clean_up():
    try:
        spark.sql(f"USE `{database_name}`") 
        spark.sql(f"DROP DATABASE `{database_name}` CASCADE")
        print(f"Database '{database_name}' dropped successfully.")
    except:
        print(f"Database '{database_name}' does not exist.")

    directory_path = f'dbfs:/mnt/{database_name}'
    try:
        dbutils.fs.ls(directory_path)
        dbutils.fs.rm(directory_path, True)
        print(f'Directory deleted: {directory_path}')
    except:
        print(f"There was an issue deleting the {directory_path}. Delete operation skipped")

print("Available function: 'clean-up' \nUsage: 'cleanup()'")
