# Databricks notebook source
import requests
from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.getOrCreate()

url = "https://avoinapi.vaylapilvi.fi/vaylatiedot/ogc/features/v1/collections/tiestotiedot:tieosoiteverkko/items?f=application%2Fgeo%2Bjson&&filter=tie=1"
file_name = '/Volumes/pl_viitekehysmuunnin/vkm_data/vkm_data_json/response.json'
response_json = requests.get(url).json()

with open(file_name, 'w') as json_file:
    json.dump(response_json, json_file)

df = spark.read.format("json").json(file_name)
df.show()

