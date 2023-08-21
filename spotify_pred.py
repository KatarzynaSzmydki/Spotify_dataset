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
from pyspark.sql.functions import col,struct,when
from pyspark.sql.window import Window


folder_path = 'abfss://rs1dl1fs1@rs1dl1.dfs.core.windows.net/spotify/'

# COMMAND ----------



# COMMAND ----------

df_tracks = spark.read.parquet(f"{folder_path}/spotify_datamodel/df_tracks/")

# COMMAND ----------

df_tracks.printSchema()

# COMMAND ----------

tracks = (
    df_tracks
    .withColumn('year', F.year('release_date'))
    .withColumn('age', F.round(F.months_between(F.current_date(), col('release_date') )/F.lit(12),0) )   
    .toPandas()
    # .show()
)

# COMMAND ----------

tracks.corr()

# COMMAND ----------


