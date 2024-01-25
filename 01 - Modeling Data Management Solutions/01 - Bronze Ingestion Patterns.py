# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Patterns
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook/01-white?style=for-the-badge&logo=databricks)
# MAGIC
# MAGIC  - **Singleplex:** One-to-one
# MAGIC  - **Multiplex:** Many-to-one
# MAGIC  
# MAGIC <!-- <img src="https://raw.githubusercontent.com/IsaacSomething/databricks-books/8cd88346aa040d5868b3138c76f116dca632fd1a/resources/01-model.png" alt="Data model" width="800" height="auto"> -->

# COMMAND ----------

# MAGIC %md
# MAGIC #### Singleplex
# MAGIC Where each dataset is ingested separately into a bronze table
# MAGIC
# MAGIC dataset/topic 1 ---> Delta table 1 <br />
# MAGIC dataset/topic 2 ---> Delta table 2 <br /> 
# MAGIC dataset/topic 3 ---> Delta table 3 <br /> 
# MAGIC
# MAGIC This pattern can work well for **batch processing**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Multiplex
# MAGIC
# MAGIC dataset ----topic 1,2,3 ...----> Delta table
# MAGIC
# MAGIC Combines many topics and streams them into a single bronze table. Silver layer tables are then created with filters on the topics from the bronze table

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

database_name = f'{spark.sql("SELECT current_user()").first()[0].split("@")[0]}-udemy-professional' 
spark.sql("USE CATALOG dvt_databricks")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{database_name}`")
spark.sql(f"USE SCHEMA `{database_name}`")

checkpoint_path = "dbfs/mnt/demo-datasets/bookstore/checkpoints"

# COMMAND ----------


files = dbutils.fs.ls(f"{dataset_bookstore}/kafka-raw")
display(files)                    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example using Autoloader
# MAGIC Read file from `dbfs:/mnt/demo-datasets/DE-Pro/bookstore/kafka-raw/01.json` (*Raw Kafka Data*) and detect new files as they arrive in order to ingest them into the **Multiplex** bronze table

# COMMAND ----------

raw_json_df = spark.read.json(f"{dataset_bookstore}/kafka-raw")
display(raw_json_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **key** and **value** columns are encoded in binary. The **value** column represents the actual values sent as `json`

# COMMAND ----------

from pyspark.sql.functions import col, date_format

def process_bronze():
  """Incrementally process files from the source to the bronze table"""

  schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

  query = (
    spark.readStream
    .format("cloudFiles") # Autoloader
    .option("cloudFiles.format", "json")
    .schema(schema)
    .load(f"{dataset_bookstore}/kafka-raw") # loading everything from the directory
    .withColumn("timestamp", (col("timestamp")/1e6).cast("timestamp"))
    .withColumn("year_month", date_format("timestamp", "yyyy-MM"))
    .writeStream
    .option("checkpointLocation", f"{checkpoint_path}/bronze")
    .option("mergeSchema", True) # Schema evolution for Autoloader
    .partitionBy("topic", "year_month")
    .trigger(availableNow=True)
    .table("bronze")
  )

  query.awaitTermination()

# COMMAND ----------

# MAGIC %md 
# MAGIC Process a batch of data with the `process_bronze()` function to load data into the bronze table

# COMMAND ----------

process_bronze()

# COMMAND ----------

bronze_df = spark.table("bronze")
display(bronze_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(topic) FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Load new data into the source directory `dbfs:/mnt/demo-datasets/DE-Pro/bookstore/kafka-raw` ready for ingestion

# COMMAND ----------

bookstore.load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC Initialize bronze process to ingest new source file added in the cell above.

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze
