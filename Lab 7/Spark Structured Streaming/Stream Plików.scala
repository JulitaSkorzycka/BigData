// Databricks notebook source
// MAGIC %md ## Dane
// MAGIC Dane są dostępne na AWS i dostęp zapewnia Databricks `/databricks-datasets/structured-streaming/events/` 

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/structured-streaming/events/

// COMMAND ----------

// MAGIC %fs head /databricks-datasets/structured-streaming/events/file-0.json

// COMMAND ----------

// MAGIC %md 
// MAGIC * Stwórz osobny folder 'streamDir' do którego będziesz kopiować część plików. możesz użyć dbutils....
// MAGIC * Pozostałe pliki będziesz kopiować jak stream będzie aktywny

// COMMAND ----------

// MAGIC %python
// MAGIC stream_dir = "dbfs:/FileStore/stream/streamDir"
// MAGIC dbutils.fs.mkdirs(stream_dir)
// MAGIC
// MAGIC sourcePath = "dbfs:/FileStore/stream/events/"
// MAGIC streamDir = "dbfs:/FileStore/stream/streamDir/"
// MAGIC
// MAGIC dbutils.fs.mkdirs(streamDir)
// MAGIC
// MAGIC files = dbutils.fs.ls(sourcePath)
// MAGIC
// MAGIC selectedFiles = files[:2]
// MAGIC
// MAGIC for f in selectedFiles:
// MAGIC     sourceFile = f.path
// MAGIC     fileName = sourceFile.split("/")[-1]
// MAGIC     targetPath = streamDir + fileName
// MAGIC     dbutils.fs.cp(sourceFile, targetPath)
// MAGIC

// COMMAND ----------

// MAGIC %md ## Analiza danych/Statyczny DF
// MAGIC * Stwórz schemat danych i wyświetl zawartość danych z oginalnego folderu

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import StructType, StructField, StringType
// MAGIC
// MAGIC input_path = "/databricks-datasets/structured-streaming/events/"
// MAGIC
// MAGIC json_schema = StructType([
// MAGIC     StructField("action", StringType(), True),
// MAGIC     StructField("time", StringType(), True),
// MAGIC     StructField("user", StringType(), True)
// MAGIC ])
// MAGIC  
// MAGIC static_input_df = spark.read.schema(json_schema).json(input_path)
// MAGIC
// MAGIC display(static_input_df)

// COMMAND ----------

// MAGIC %md 
// MAGIC Policz ilość akcji "open" i "close" w okienku (window) jedno godzinnym (kompletny folder). 

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col, window
// MAGIC
// MAGIC ilosc_akcji = static_input_df \
// MAGIC     .filter(col("action").isin("open", "close")) \
// MAGIC     .groupBy(
// MAGIC         window(col("time"), "1 hour"),
// MAGIC         col("action")
// MAGIC     ) \
// MAGIC     .count()
// MAGIC
// MAGIC ilosc_akcji.createOrReplaceTempView("static_counts")

// COMMAND ----------

// MAGIC %md 
// MAGIC Użyj sql i pokaż na wykresie ile było akcji 'open' a ile 'close'.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT action, SUM(count) AS total
// MAGIC FROM static_counts
// MAGIC GROUP BY action

// COMMAND ----------

// MAGIC %md
// MAGIC Użyj sql i pokaż ile było akcji w każdym dniu i godzinie przykład ('Jul-26 09:00')

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC   date_format(window.start, 'MMM-dd HH:00') AS hour_label,
// MAGIC   action,
// MAGIC   count
// MAGIC FROM static_counts
// MAGIC ORDER BY hour_label, action
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %md ## Stream Processing 
// MAGIC Teraz użyj streamu.
// MAGIC * Ponieważ będziesz streamować pliki trzeba zasymulować, że jest to normaly stream. Podpowiedź dodaj opcję 'maxFilesPerTrigger'
// MAGIC * Użyj 'streamDir' niekompletne pliki

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col, window
// MAGIC
// MAGIC streamingInputDF = (
// MAGIC     spark.readStream
// MAGIC     .schema(json_schema)
// MAGIC     .option("maxFilesPerTrigger", 1)
// MAGIC     .json(streamDir)
// MAGIC )
// MAGIC
// MAGIC streamingCountsDF = (
// MAGIC     streamingInputDF
// MAGIC     .filter(col("action").isin("open", "close"))
// MAGIC     .groupBy(
// MAGIC         window(col("time"), "1 hour"),
// MAGIC         col("action")
// MAGIC     )
// MAGIC     .count()
// MAGIC )
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC Sprawdź czy stream działa

// COMMAND ----------

// MAGIC %python
// MAGIC streamingCountsDF.isStreaming

// COMMAND ----------

// MAGIC %md 
// MAGIC * Zredukuj partyce shuffle do 4 
// MAGIC * Teraz ustaw Sink i uruchom stream
// MAGIC * użyj formatu 'memory'
// MAGIC * 'outputMode' 'complete'

// COMMAND ----------

// MAGIC %python
// MAGIC spark.conf.set("spark.sql.shuffle.partitions", 4)
// MAGIC
// MAGIC streamingCountsDF = streamingInputDF \
// MAGIC     .filter(col("action").isin("open", "close")) \
// MAGIC     .groupBy(
// MAGIC         window(col("time"), "1 hour"),
// MAGIC         col("action")
// MAGIC     ) \
// MAGIC     .count()
// MAGIC
// MAGIC query = streamingCountsDF.writeStream \
// MAGIC     .format("memory") \
// MAGIC     .queryName("counts_table") \
// MAGIC     .outputMode("complete") \
// MAGIC     .start()
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %md 
// MAGIC `query` działa teraz w tle i wczytuje pliki cały czas uaktualnia count. Postęp widać w Dashboard

// COMMAND ----------

Thread.sleep(3000) // lekkie opóźnienie żeby poczekać na wczytanie plików

// COMMAND ----------

// MAGIC %md
// MAGIC * Użyj sql żeby pokazać ilość akcji w danym dniu i godzinie 

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC   action, 
// MAGIC   date_format(window.end, "yyyy-MM-dd HH") AS day_hour,
// MAGIC   COUNT(*) AS total
// MAGIC FROM counts_table
// MAGIC GROUP BY action, date_format(window.end, "yyyy-MM-dd HH")
// MAGIC ORDER BY day_hour, action

// COMMAND ----------

// MAGIC %md 
// MAGIC * Sumy mogą się nie zgadzać ponieważ wcześniej użyłeś niekompletnych danych.
// MAGIC * Teraz przekopiuj resztę plików z orginalnego folderu do 'streamDir', sprawdź czy widać zmiany 
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC for file in files:
// MAGIC     source_file = file.path
// MAGIC     file_name = source_file.split("/")[-1]
// MAGIC     target_path = stream_dir + file_name
// MAGIC     dbutils.fs.cp(source_file, target_path)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC   action, 
// MAGIC   date_format(window.end, "yyyy-MM-dd HH") AS day_hour,
// MAGIC   COUNT(*) AS total
// MAGIC FROM counts_table
// MAGIC GROUP BY action, date_format(window.end, "yyyy-MM-dd HH")
// MAGIC ORDER BY day_hour, action

// COMMAND ----------

// MAGIC %md
// MAGIC * Zatrzymaj stream

// COMMAND ----------

// MAGIC %python
// MAGIC query.stop()