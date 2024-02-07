# Databricks notebook source
# MAGIC %md
# MAGIC # Partitioning Delta Lake Tables
# MAGIC ![Static Badge](https://img.shields.io/badge/Development-notebook|3.01-123/02?style=for-the-badge&logo=databricks&color=red&labelColor=grey&logoColor=white)
# MAGIC
# MAGIC Partitioning is an optimizing strategy for optimizing queries on **large** Delta tables. It is a subset of rows that share the same value for predefined partitioning of columns.
# MAGIC
# MAGIC | id | name | month | year | *info*|
# MAGIC |----|----|----|----|----|
# MAGIC |1 | [name] | 4 | 2015 | **partition 1** : "year 2015" |
# MAGIC |2 | [name] | 3 | 2015 | **partition 1** : "year 2015" |
# MAGIC |3 | [name] | 6 | 2000 | **partition 2** : "year 2000" |
# MAGIC |4 | [name] | 8 | 2000 | **partition 2** : "year 2000" |
# MAGIC |5 | [name] | 1 | 2000 | **partition 2** : "year 2000" |
# MAGIC |6 | [name] | 1 | 2000 | **partition 2** : "year 2000" |
# MAGIC |7 | [name] | 3 | 2003 | **partition 3** : "year 2003" |
# MAGIC |8 | [name] | 12 | 2003 | **partition 3** : "year 2003" |
# MAGIC |9 | [name] | 11 | 2003 | **partition 4** : "year 2003" |
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC When creating a table you use the `PARTITIONED BY ()`. Databricks then creates a directory for each partition and dispatches rows into the appropriate directory. Using these partitions will speed up queries against the table as well as data manipulation. You can partition a table by more than one column  
# MAGIC
# MAGIC **Partition Skipping**
# MAGIC When filtering columns on a partitioned column, partitions not matching the conditional statement will be skipped entirely.
# MAGIC
# MAGIC Delta lake also has several operations like `OPTIMIZE` commands that can be applied at the partition level. 
# MAGIC
# MAGIC **Choosing parition columns** 
# MAGIC - Total values present in a column, choose low cardinality fields (*columns with a small number of distinct values* - where the same values appear in the column or few unique values)
# MAGIC - Partitions should be at least 1GB in size
# MAGIC - if records with a given vlaue will continue to arrive indefinitely datetime fields can be used for partitioning
# MAGIC
# MAGIC
# MAGIC **Avoid over-partitioning**
# MAGIC  - Files cannot be combined or compacted across partition boundaries as such **small partitioned** tables increase storage costs and total number of files to scan
# MAGIC  - Partitions **less than** 1GB of data is considered over-partitioned
# MAGIC  - Data that is over-partitioned or incorrectly partitioned will suffer greatly and lead to slowdowns for most general queries
# MAGIC
# MAGIC > When in doubt do not partition a table
# MAGIC
# MAGIC **`ignoreDeletes`** 
# MAGIC Deleting partitioned data from a streaming table breaks the append-only requirement from streaming sources which will prevent the table from being streamable. In order to circumvent this you need to add the `ignoreDeletes` option
# MAGIC
# MAGIC <br />
# MAGIC
# MAGIC ```
# MAGIC spark.readStream
# MAGIC   .optiom("ignoreDeletes", True)
# MAGIC   .table("my_table)
# MAGIC ```
# MAGIC
# MAGIC **NOTE:** the actual deletion of the files will not occur until you have run the `VACUUM` command on the table.

# COMMAND ----------

# MAGIC %run ../resources/local-setup

# COMMAND ----------

# MAGIC %run ../resources/copy-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC We have a partitioned table, `bronze` which has been partitioned by the `year` and `month` columns

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED bronze
