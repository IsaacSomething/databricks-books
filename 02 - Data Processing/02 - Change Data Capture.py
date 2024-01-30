# Databricks notebook source
# MAGIC %md
# MAGIC # Change Data Capture
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook|2.02-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)
# MAGIC
# MAGIC Process of identifying changes made to data in the **source** and delivering those changes (*row level changes*) to the **target** (*sink*) <br />
# MAGIC Changes include the store data as well as metadata with timestamp (*or version number*) and operation (*update/delete/insert*) <br />
# MAGIC
# MAGIC **Processing CDC feed** <br />
# MAGIC Can be done with `MERGE INTO`
# MAGIC - Limitations:
# MAGIC   - Merge can not be performed if multiple source rows are matched and attempted to modify the same target row in the Delta table
# MAGIC   - CDC feed with multiple updates for the same key will generate an exception
# MAGIC - Solution:
# MAGIC   - Merge only the most recent changes
# MAGIC   - This can be achieved using the `rank().over([Window])` function. The rank will assign a rank number for each row within a window
# MAGIC   - **What is a window?**
# MAGIC     - It is a group of records which have the same **partitioning key**
# MAGIC     - Has an **ordering column** sorted in descending order, so the most recent row in the window with have **rank 1**
# MAGIC   - We can therefor filter rows based on **rank 1** and merge it into the target table with `MERGE INTO`

# COMMAND ----------

# MAGIC %run ../resources/local-setup

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

from pyspark.sql.functions import col

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

customers_df = (
    spark.table("bronze")
    .filter("topic = 'customers'")
    .transform(lambda df: bronze_json_parse(df, schema))
    .filter(col("row_status").isin(["insert","update"]))
    .orderBy("customer_id")
)

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see in the table below there are 3 records for "00003" in the bronze table as raw data. We now want to create a silver table customers with only the most recent data for each `customer_id`. We could use `dropDuplicates` to remove exact duplicates. However in this case the problem is different, since records are not identical. 

# COMMAND ----------

customers_00003_df = customers_df.filter(col("customer_id") == 'C00003')
display(customers_00003_df)

# COMMAND ----------

# MAGIC %md
# MAGIC The solution to send only the most recent change is to use `.rank().over(window)` that is; get **rank 1** on a partitioned window ordered in descending order by a timestamp. In this case the windowed partition is the common **key** of `customer_id` with the window ordered descending by the timestamp `row_time`

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Define the window partition details `customer_id` and order by `row_time`
window = Window.partitionBy("customer_id").orderBy(col("row_time").desc())

ranked_df = (
    customers_df.withColumn("rank", rank().over(window)) # Create the rank column with the windowed partition
    .filter("rank == 1") # filter for just rank 1
    .drop("rank") # drop the column as it has served its purpose
)

display(ranked_df)

# COMMAND ----------

display(customers_00003_df.orderBy(col("row_time").desc()))

# COMMAND ----------

ranked_00003_df = ranked_df.filter(col("customer_id") == 'C00003')
display(ranked_00003_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Now that the logic has been determined, lets apply this to a streaming read from the `bronze` table

# COMMAND ----------

ranked_df = (
    spark.readStream.table("bronze")
    .filter("topic = 'customers'")
    .transform(lambda df: bronze_json_parse(df, schema))
    .filter(col("row_status").isin(["insert","update"]))
    .withColumn("rank", rank().over(window))
    .filter("rank == 1")
    .drop("rank")
)

display(ranked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE:** Rank window functions are not allowed on streams! To bypass this issue we can we can use `foreachBatch` logic. Processing the data of each batch before merging it into the target table (*sink*)

# COMMAND ----------

def batch_upsert(micro_batch_df, batch_id):
    """
    First get the most recent data using ranked over window
    and store them in a temporary view "ranked_updates"
    """
    window = Window.partitionBy("customer_id").orderBy(col("row_time").desc())

    (
        micro_batch_df.filter(col("row_status").isin(["insert","update"]))
        .withColumn("rank", rank().over(window))
        .filter("rank == 1")
        .drop("rank")
        .createOrReplaceTempView("ranked_updates")
    )

    """
    Then Merge the data into the table "customers_silver" based on the customer_id. Updating values when they are matched 
    and inserting values when they do not match
    """
    query = """
        MERGE INTO customers_silver c
        USING ranked_updates r
        ON c.customer_id = r.customer_id
            WHEN MATCHED AND c.row_time < r.row_time
                THEN UPDATE SET *
            WHEN NOT MATCHED
                THEN INSERT *
    """

    micro_batch_df.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_silver
# MAGIC (customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC Becuase we are using the **`country STRING`** name and not the country code we first need to perform a lookup for the country name based on the country code

# COMMAND ----------

country_lookup_df = spark.read.json(f"{dataset_bookstore}/country_lookup")
display(country_lookup_df)

# COMMAND ----------

# MAGIC %md
# MAGIC In the `readStream` we can use `broadcast` to optimize the lookup, where the smaller dataframe gets broadcast to all the executor nodes in the cluster. This tell spark that this dataframe can fit in memory on all executors

# COMMAND ----------

from pyspark.sql.functions import broadcast

query = (
  spark.readStream.table("bronze")
  .filter("topic = 'customers'")
  .transform(lambda df: bronze_json_parse(df, schema))
  .join(broadcast(country_lookup_df), col("country_code") == col("code"), "inner")
  .writeStream
  .foreachBatch(batch_upsert)
  .option("checkpointLocation", f"{checkpoint_path}/customers_silver")
  .trigger(availableNow=True)
  .start()
)

query.awaitTermination()

# COMMAND ----------

count = spark.table("customers_silver").count()
expected_count = spark.table("customers_silver").select("customer_id").distinct().count()

assert count == expected_count
print("Passed: count is equal to expected count")
