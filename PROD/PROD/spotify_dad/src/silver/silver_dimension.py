# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Cell 2
import os
import sys

project_pth = os.path.join(os.getcwd(), "..","..")
sys.path.append(project_pth)

from utils.transformation import reusable



# COMMAND ----------

df=spark.read.format("parquet")\
        .load("abfss://bronze@storageforazureprojects.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## autoloader

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@storageforazureprojects.dfs.core.windows.net/DimUser/checkpoint")\
            .load("abfss://bronze@storageforazureprojects.dfs.core.windows.net/DimUser")

# COMMAND ----------

# DBTITLE 1,Cell 5
display(df_user, checkpointLocation="abfss://silver@storageforazureprojects.dfs.core.windows.net/DimUser/checkpoint")

# COMMAND ----------

display(df)

# COMMAND ----------

df= df.withColumn("user_name", upper(col("user_name")))
display(df)

# COMMAND ----------

df_user_object = reusable()

# COMMAND ----------

# DBTITLE 1,Cell 12
df_user.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation","abfss://silver@storageforazureprojects.dfs.core.windows.net/DimUser/checkpoint")\
        .trigger(once=True)\
        .option("path","abfss://silver@storageforazureprojects.dfs.core.windows.net/DimUser/data")\
        .toTable("spotify.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dimartist

# COMMAND ----------

df_artist=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@storageforazureprojects.dfs.core.windows.net/DimUser/chekpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@storageforazureprojects.dfs.core.windows.net/DimArtist")

# COMMAND ----------

# DBTITLE 1,Cell 14
display(df_artist, checkpointLocation="abfss://silver@storageforazureprojects.dfs.core.windows.net/DimUser/chekpoint")

# COMMAND ----------

df_artist.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation","abfss://silver@storageforazureprojects.dfs.core.windows.net/DimArtist/checkpoint")\
        .trigger(once=True)\
        .option("path","abfss://silver@storageforazureprojects.dfs.core.windows.net/DimArtist/data")\
        .toTable("spotify.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ##dimtrack

# COMMAND ----------

df_track=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@storageforazureprojects.dfs.core.windows.net/DimTrack/chekpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@storageforazureprojects.dfs.core.windows.net/DimTrack")

# COMMAND ----------

# DBTITLE 1,Cell 20
display(df_track, checkpointLocation="abfss://silver@storageforazureprojects.dfs.core.windows.net/DimTrack/checkpoint_stream")

# COMMAND ----------

df_track = df_track.withColumn("durationFlag",when(col('duration_sec')<150,"low")\
                                              .when(col('duration_sec')<300,"medium")\
                                              .otherwise("high"))
                    

# COMMAND ----------

df_track=df_track.withColumn('track_name',regexp_replace(col('track_name'),"-",""))

df_track=reusable().dropColumns(df_track,["_rescued_data"])

# COMMAND ----------

df_track.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation","abfss://silver@storageforazureprojects.dfs.core.windows.net/DimTrack/checkpoint")\
        .trigger(once=True)\
        .option("path","abfss://silver@storageforazureprojects.dfs.core.windows.net/DimTrack/data")\
        .toTable("spotify.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dimdate

# COMMAND ----------

df_date=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@storageforazureprojects.dfs.core.windows.net/DimDate/chekpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@storageforazureprojects.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date = reusable().dropColumns(df_date,['_rescued_data'])

# COMMAND ----------

df_date.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation","abfss://silver@storageforazureprojects.dfs.core.windows.net/DimDate/checkpoint")\
        .trigger(once=True)\
        .option("path","abfss://silver@storageforazureprojects.dfs.core.windows.net/DimDate/data")\
        .toTable("spotify.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ##factstream

# COMMAND ----------

df_fact=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@storageforazureprojects.dfs.core.windows.net/FactStream/chekpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@storageforazureprojects.dfs.core.windows.net/FactStream")

# COMMAND ----------

display(df_fact)

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact,['_rescued_data'])

# COMMAND ----------

df_fact.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation","abfss://silver@storageforazureprojects.dfs.core.windows.net/FactStream/checkpoint")\
        .trigger(once=True)\
        .option("path","abfss://silver@storageforazureprojects.dfs.core.windows.net/FactStream/data")\
        .toTable("spotify.silver.FactStream")

# COMMAND ----------

