# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Patterns
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook|1.01-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)
# MAGIC
# MAGIC <br />
# MAGIC
# MAGIC  - **Singleplex:** One-to-one
# MAGIC  - **Multiplex:** Many-to-one
# MAGIC
# MAGIC #### Singleplex
# MAGIC Where each dataset is ingested separately into a bronze table. This pattern can work well for **batch processing**
# MAGIC ![](/files/tcuthbert/singleplex_bronze.png)
# MAGIC
# MAGIC #### Multiplex
# MAGIC Combines many topics and streams them into a single bronze table. Silver layer tables are then created with filters on the topics from the bronze table
# MAGIC ![](/files/tcuthbert/multiplex_bronze.png)
# MAGIC

# COMMAND ----------

# MAGIC %run ../resources/local-setup

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/kafka-raw")
display(files)                    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example using Autoloader
# MAGIC Read file from `dbfs:/mnt/demo-datasets/DE-Pro/bookstore/kafka-raw/01.json` (*Raw Kafka Data*) and detect new files as they arrive in order to ingest them into the **Multiplex** bronze table.
# MAGIC
# MAGIC **key** and **value** columns are encoded in binary. The **value** column represents the actual values sent as `json`

# COMMAND ----------

from pyspark.sql.functions import col, date_format

def process_bronze():
  """
  Read JSON files from dbfs:/mnt/demo-datasets/DE-Pro/bookstore/kafka-raw using Autoloader.
  Write to the bronze table with schema evolution and partitioning by "topic" and "year".
  Trigger the stream as a micro-batch.
  """
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
    .trigger(availableNow=True) # Micro-batch trigger
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
# MAGIC Load new data into the source directory ready for ingestion

# COMMAND ----------

bookstore.load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC Initialize bronze process to ingest new source file added in the cell above.

# COMMAND ----------

bronze_count_df = spark.table("bronze").count()
print(bronze_count_df)

# COMMAND ----------

process_bronze()

# COMMAND ----------

bronze_count_df = spark.table("bronze").count()
print(bronze_count_df)
