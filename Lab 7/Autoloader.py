# Databricks notebook source
# DBTITLE 1,Przygotowanie środowiska
# Import functions
from pyspark.sql.functions import col, current_timestamp

# Define variables used in code below
file_path = "/FileStore/stream/events/"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)



# COMMAND ----------

dbutils.fs.rm(f"dbfs:/user/hive/warehouse/{username}_etl_quickstart", True)


# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/structured-streaming/events

# COMMAND ----------

# MAGIC %md
# MAGIC Żeby sprawdzić czy stream działa kopjuj po jednym bądź kilku plikach 

# COMMAND ----------

dbutils.fs.cp("/databricks-datasets/structured-streaming/events/file-4.json","/FileStore/stream/events/file-4.json",True)

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/stream/events/"))

# COMMAND ----------

spark.read.format("json").load("dbfs:/FileStore/stream/events/").count()

# COMMAND ----------

# MAGIC %md
# MAGIC Dokończyć kod autoloadera 
# MAGIC 1. Dodaj opcje 'cloudfiles'
# MAGIC 2. Dodaj kolumnę z metadanych 'source_file'
# MAGIC 3. Dane zapisać do tabeli

# COMMAND ----------

from pyspark.sql.functions import input_file_name
schema_location = f"/tmp/{username}/schema/etl_quickstart"

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.includeExistingFiles", "true")
  .option("cloudFiles.schemaLocation", schema_location)
  .load(file_path)
  .withColumn("source_file", input_file_name())  
  .writeStream
  .trigger(availableNow=True)                   
  .option("checkpointLocation", checkpoint_path)
  .toTable(table_name))


# COMMAND ----------

df = spark.sql(f"select count(*) from {table_name}")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Sprawdzć metadane 

# COMMAND ----------

display(dbutils.fs.ls(f"/tmp/{username}/_checkpoint/etl_quickstart/sources/0/metadata/"))

# COMMAND ----------

# MAGIC %fs ls /tmp/{user}/_checkpoint/etl_quickstart/metadata

# COMMAND ----------

