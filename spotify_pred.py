# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.rs1dl1.dfs.core.windows.net", 
  "pRyCGC4UOzRK8hkLrxdJlPhkn5Q8RkrN3xerBoOb6x00gqp3GXb+iFYQ07CJ1hmcwfb32E6kYMXK+AStcmWw5Q==")

# COMMAND ----------

import psutil
import time
from datetime import date
from datetime import datetime
import pandas as pd
import random

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, BooleanType
from pyspark.sql.functions import col,struct,when, lit
from pyspark.sql.window import Window


folder_path = 'abfss://rs1dl1fs1@rs1dl1.dfs.core.windows.net/spotify/'

# COMMAND ----------



# COMMAND ----------

df_tracks = spark.read.parquet(f"{folder_path}/spotify_datamodel/df_tracks/")

# COMMAND ----------

tracks = (
    df_tracks
    .drop('key')
    .drop('mode')
    .drop('time_signature')
    .toPandas()
    # .show()
)

# COMMAND ----------

df_tracks_corr = tracks.corr()
df_tracks_corr.index.name = 'track_component'
df_tracks_corr.reset_index(inplace=True)
df_tracks_corr_unpivot = pd.melt(df_tracks_corr, id_vars = "track_component", value_vars = ['popularity', 'duration_ms', 'explicit', 'danceability', 'energy',
    'loudness', 'speechiness', 'acousticness',
       'instrumentalness', 'liveness', 'valence', 'tempo',
       'age'])

# COMMAND ----------

df_tracks_corr = spark.createDataFrame(df_tracks_corr_unpivot) 

(
    df_tracks_corr
    .withColumn("track_component_sort", when( col('track_component') == 'popularity', lit(0)).otherwise(lit(1)))
    .withColumn("variable_sort", when( col('variable') == 'popularity', lit(0)).otherwise(lit(1)))
    .write.mode("overwrite").format("parquet").save(f"{folder_path}/spotify_datamodel/df_tracks_corr/")
)

# COMMAND ----------


