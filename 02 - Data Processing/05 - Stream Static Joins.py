# Databricks notebook source
# MAGIC %md
# MAGIC # Stream Static Joins
# MAGIC
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook|2.05-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)
# MAGIC
# MAGIC   **Streaming tables** are append only data sources. These tables drive the process. **ONLY** new data arriving on the streaming source will trigger the processing.  
# MAGIC
# MAGIC **Static tables** contains data that may be changed or overwritten and **breaks** the requirmenet of ever-appending source of data for structured streaming.
# MAGIC In a stream-static join the latest version of the static table is guarantee.
# MAGIC
# MAGIC **Unmatched records** at the time of processing will be skipped. If a data row in table A does note have a relation in table B the record will not be updated. Streaming-static joins are **not** stateful.

# COMMAND ----------

# MAGIC %run ../resources/local-setup

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

from pyspark.sql.functions import explode

def process_book_sales():
    orders_df = spark.readStream.table("orders_silver").withColumn("book", explode("books"))
    books_df = spark.read.table("current_books")

    query = (
        orders_df.join(books_df,orders_df.book.book_id == books_df.book_id, "inner")
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/book_sales")
        .trigger(availableNow=True)
        .table("book_sales")
    )

    query.awaitTermination()

# COMMAND ----------

process_book_sales()

# COMMAND ----------

book_sales_df = spark.table("book_sales")
display(book_sales_df)

# COMMAND ----------

display(book_sales_df.count())

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_books_silver()
bookstore.process_current_books()

process_book_sales()

# COMMAND ----------

bookstore.process_orders_silver()
process_book_sales()

# COMMAND ----------

display(book_sales_df.count())
