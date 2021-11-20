# Databricks notebook source
from utils.load_functions import enrich_source

# COMMAND ----------

df = spark.createDataFrame([
  ('Bob', 'Dole'),
  ('Tony', 'Stark')
])
display(df)

# COMMAND ----------

from datetime import datetime

df = enrich_source(df, datetime.now(), "Try_Local")
display(df)
