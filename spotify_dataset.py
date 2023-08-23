# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.rs1dl1.dfs.core.windows.net", 
  "pRyCGC4UOzRK8hkLrxdJlPhkn5Q8RkrN3xerBoOb6x00gqp3GXb+iFYQ07CJ1hmcwfb32E6kYMXK+AStcmWw5Q==")

# COMMAND ----------

# dbutils.fs.mount(
#     source="wasbs://rs1dl1fs1@rs1dl1.blob.core.windows.net"
#     ,mount_point = '/mnt/blobstorage'
#     ,extra_configs = {
#         'fs.azure.account.key.rs1dl1.blob.core.windows.net':'pRyCGC4UOzRK8hkLrxdJlPhkn5Q8RkrN3xerBoOb6x00gqp3GXb+iFYQ07CJ1hmcwfb32E6kYMXK+AStcmWw5Q=='
#     }
#     )

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

# MAGIC %md
# MAGIC <b>
# MAGIC Artists
# MAGIC </b>

# COMMAND ----------

df_artists_schema = StructType([
    StructField("id", StringType(), True), 
    StructField("followers", FloatType(), True),
    StructField("genres", StringType(), True),
    StructField("name", StringType(), True),
    StructField("popularity", IntegerType(), True),
])

df_artists = spark.read.format('com.databricks.spark.csv').options(header='True', delimiter=",").load(f'{folder_path}/artists.csv', schema = df_artists_schema)

# COMMAND ----------

df_artists = (
    df_artists
    .withColumn("genres", F.regexp_replace('genres', '\\[',''))
    .withColumn("genres", F.regexp_replace('genres', '\\]',''))
    .withColumn("genres", F.regexp_replace('genres', "\\'",''))
    .withColumn("genres", F.regexp_replace('genres', '\\"',''))
    .withColumn("genres", F.split('genres', ','))

    .withColumn("name", F.regexp_replace('name', '\\[',''))
    .withColumn("name", F.regexp_replace('name', '\\]',''))
    .withColumn("name", F.regexp_replace('name', "\\'",''))
    .withColumn("name", F.regexp_replace('name', '\\"',''))
    # .withColumn("name", F.split('name', ','))

    # .show(10, truncate=False)
)

# COMMAND ----------

df_artists = (
    df_artists
    .withColumn("genres", F.expr("transform(genres, x -> ltrim(x))" ))
    .withColumn("genres", F.expr("transform(genres, x -> rtrim(x))" ))
    # .withColumn("name", F.expr("transform(name, x -> ltrim(x))" ))
    # .withColumn("name", F.expr("transform(name, x -> ltrim(x))" ))
    .withColumn("name", F.ltrim( col('name')))
    .withColumn("name", F.rtrim( col('name')))
    .withColumnRenamed("id", "artistPKSK")
    .withColumn("genres", F.explode( col('genres')).alias('genres'))
    .withColumn("genres", when( col("genres") == "", None).otherwise(col("genres")) )
)



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Tracks
# MAGIC </b>

# COMMAND ----------

df_tracks_schema = StructType([
    StructField("id", StringType(), True), 
    StructField("name", StringType(), True), 
    StructField("popularity", IntegerType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("explicit", IntegerType(), True),
    StructField("artists", StringType(), True),
    StructField("id_artists", StringType(), True),
    StructField("release_date", DateType(), True),
    StructField("danceability", FloatType(), True),
    StructField("energy", FloatType(), True),
    StructField("key", IntegerType(), True),
    StructField("loudness", FloatType(), True),
    StructField("mode", IntegerType(), True),
    StructField("speechiness", FloatType(), True),
    StructField("acousticness", FloatType(), True),
    StructField("instrumentalness", FloatType(), True),
    StructField("liveness", FloatType(), True),
    StructField("valence", FloatType(), True),
    StructField("tempo", FloatType(), True),
    StructField("time_signature", IntegerType(), True)
    
])

df_tracks = spark.read.format('com.databricks.spark.csv').options(header='True', delimiter=",").load(f'{folder_path}/tracks.csv' 
                                                                                                      ,schema = df_tracks_schema
                                                                                                      )

# COMMAND ----------


df_tracks = (
    df_tracks
    .withColumnRenamed("id", "trackPKSK")
    .drop("artists")

    .withColumn("id_artists", F.regexp_replace('id_artists', '\\[',''))
    .withColumn("id_artists", F.regexp_replace('id_artists', '\\]',''))
    .withColumn("id_artists", F.regexp_replace('id_artists', "\\'",''))
    .withColumn("id_artists", F.regexp_replace('id_artists', '\\"',''))
    .withColumn("id_artists", F.split('id_artists', ','))

    .withColumn("danceability", F.round( col('danceability'),4))
    .withColumn("energy", F.round( col('energy'),4))
    .withColumn("loudness", F.round( col('loudness'),4))
    .withColumn("speechiness", F.round( col('speechiness'),4))
    .withColumn("acousticness", F.round( col('acousticness'),4))
    .withColumn("instrumentalness", F.round( col('instrumentalness'),4))
    .withColumn("liveness", F.round( col('liveness'),4))
    .withColumn("valence", F.round( col('valence'),4))
    .filter( col("release_date").rlike('^[0-9]-*') )
    .filter( F.year( col('release_date') ) > 1900)
    .withColumn('age', F.round(F.months_between(F.current_date(), col('release_date') )/F.lit(12),0) )   
    
)

# COMMAND ----------

df_dim_bridge_tracks_artists = (
    df_tracks
    .select( "trackPKSK", F.explode( col('id_artists')).alias('id_artists'))
    .withColumnRenamed("id_artists", "artistSK")
)


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Saving data to parquet tables
# MAGIC </b>

# COMMAND ----------

df_artists.count()

# COMMAND ----------

df_dim_bridge_artists_genres.count()

# COMMAND ----------

df_dim_genres.count()

# COMMAND ----------

df_tracks.count()

# COMMAND ----------

df_dim_bridge_tracks_artists.count()

# COMMAND ----------

df_artists = (
    df_artists
    # .drop("genres")
    # .drop("name")
)

# COMMAND ----------

df_artists.write.mode("overwrite").format("parquet").save(f"{folder_path}/spotify_datamodel/df_artists/")

# COMMAND ----------

df_tracks.write.mode("overwrite").format("parquet").save(f"{folder_path}/spotify_datamodel/df_tracks/")

# COMMAND ----------

df_dim_bridge_tracks_artists.write.mode("overwrite").format("parquet").save(f"{folder_path}/spotify_datamodel/df_dim_bridge_tracks_artists/")
