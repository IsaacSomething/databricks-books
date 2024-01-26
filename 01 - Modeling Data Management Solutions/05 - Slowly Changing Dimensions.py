# Databricks notebook source
# MAGIC %md
# MAGIC # Slowly Changing Dimensions
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook|05-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)
# MAGIC
# MAGIC SCD is a data management concept that determines how tables handle data that changes over time i.e. whether you want to overwrite values in the table or retain their history, which is determined by the **SCD type**
# MAGIC
# MAGIC - **Type 0:** No changes allowed, static/append only 
# MAGIC   - i.e. Static lookup tables
# MAGIC - **Type 1:** New data will overwrite existing data (no history is retained [Use Delta Time Travel])
# MAGIC   - i.e Tables where the current values are more important than historic comparision 
# MAGIC - **Type 2:** New rows are added for each change in data values with identification on old values as obsolete. The new record becomes the current active record
# MAGIC   - i.e. Table where full history of transactions are important to data ingestion
# MAGIC
# MAGIC ##### Delta time travel vs. SCD Type 2
# MAGIC Delta time travel is not long-term versioning solution, running a `VACUUM` command will destroy the version history.

# COMMAND ----------

# MAGIC %run ../resources/local-setup

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC **SCD type 2** query statement that merges updates and checks the condition with `WHEN MATCHED` to either add or append data into the table

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC MERGE INTO books_silver
# MAGIC USING (
# MAGIC   SELECT updates.book_id AS merge_key, updates.*
# MAGIC   FROM updates
# MAGIC   UNION ALL
# MAGIC   SELECT NULL AS merge_key, updates.*
# MAGIC   FROM updates
# MAGIC   JOIN books_silver ON updates.book_id = books_silver.book_id
# MAGIC   WHERE books_silver.current = true AND updates.price <> books_silver.price
# MAGIC ) staged_updates
# MAGIC ON books_silver.book_id = merge_key
# MAGIC WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price THEN
# MAGIC   UPDATE SET current = false, end_date = staged_updates.updated
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (book_id, title, author, price, current, effective_date, end_date)
# MAGIC   VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, NULL)
# MAGIC ```

# COMMAND ----------

def type2_upsert(micro_batch_df, batch):
    """
    SCD type 2 query statement that merges updates and checks the condition with `WHEN MATCHED` to either add or append data into the table
    """

    micro_batch_df.createOrReplaceTempView("updates")

    query = """
        MERGE INTO books_silver
        USING (
            SELECT updates.book_id AS merge_key, updates.*
            FROM updates
            UNION ALL
            SELECT NULL AS merge_key, updates.*
            FROM updates
            JOIN books_silver ON updates.book_id = books_silver.book_id
            WHERE books_silver.current = true AND updates.price <> books_silver.price
        ) staged_updates
        ON books_silver.book_id = merge_key
        WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price THEN
            UPDATE SET current = false, end_date = staged_updates.updated
        WHEN NOT MATCHED THEN
            INSERT (book_id, title, author, price, current, effective_date, end_date)
            VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, NULL)
    """
    
    micro_batch_df.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS books_silver
# MAGIC (book_id STRING, title STRING, author STRING, price DOUBLE, current BOOLEAN, effective_date TIMESTAMP, end_date TIMESTAMP)

# COMMAND ----------

def process_books():
    schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"

    query = (
        spark.readStream.table("bronze")
        .filter("topic = 'books'")
        .transform(lambda df:bronze_json_parse(df, schema))
        .writeStream
        .foreachBatch(type2_upsert)
        .option("checkpointLocation", f"{checkpoint_path}/books_silver")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

# COMMAND ----------

process_books()

# COMMAND ----------

books_silver_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_silver_df)

# COMMAND ----------

bookstore.load_books_updates()
bookstore.process_bronze()
process_books()

# COMMAND ----------

books_silver_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC [Current books img here]
# MAGIC
# MAGIC From the books table we are going to create the silver table "current_books" as a batch process. Creating the table with batch overwrite logic witht the `CREATE OR REPLACE` syntax. Each time the query is run the contents of the table will be overwritten.      

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE current_books
# MAGIC AS SELECT book_id, title, author, price
# MAGIC FROM books_silver
# MAGIC WHERE current IS TRUE

# COMMAND ----------

from pyspark.sql.functions import col

current_books_df = spark.table("current_books").orderBy(col("book_id"))
display(current_books_df)
