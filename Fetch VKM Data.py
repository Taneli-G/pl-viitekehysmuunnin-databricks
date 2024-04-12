# Databricks notebook source
# MAGIC %pip install geopandas geojson

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Specify target URL and Databricks filename and Volume
url = "https://avoinapi.vaylapilvi.fi/vaylatiedot/ogc/features/v1/collections/tiestotiedot:tieosoiteverkko/items?f=application%2Fgeo%2Bjson"
volume = "/Volumes/pl_viitekehysmuunnin/vkm_data/vkm_data_json/"
file_name = "response.json"

# Make the request
response_json = requests.get(url).json()

# Write the request response to a Databricks volume
with open(volume + file_name, 'w') as json_file:
    json.dump(response_json, json_file)

# COMMAND ----------

# Read the file from the volume in to a spark dataframe
df = spark.read.json(f"dbfs:{volume}{file_name}")

spark.sql("CREATE TABLE IF NOT EXISTS pl_viitekehysmuunnin.vkm_data.tieosoiteverkko_bronze")
(
df.write
    .option("mergeSchema", True)
    .insertInto("pl_viitekehysmuunnin.vkm_data.tieosoiteverkko_bronze", overwrite=True)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pl_viitekehysmuunnin.vkm_data.tieosoiteverkko_bronze
