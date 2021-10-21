# Databricks notebook source
# MAGIC %md # Ingest Files (add ingest metrics)

# COMMAND ----------

# MAGIC %md ## Import requirements

# COMMAND ----------

pip install -r requirements.txt

# COMMAND ----------

# MAGIC %md ## read source file  
# MAGIC   
# MAGIC - e.g. Read csv file (here using pyspark)
# MAGIC - Add input_file_name() to dataframe

# COMMAND ----------

import os

# print("Use the file: {}{}".format(os.getcwd(),"/data/winequality-red.csv"))
# "file:" prefix and absolute file path are required for PySpark
source_df = spark.read.csv(f"file:{os.getcwd()}/data/winequality-red.csv", header=True) 
display(source_df)

# COMMAND ----------

# MAGIC %md ## enrich source dataframe

# COMMAND ----------

from utils import *
enriched_df = enrich(source_df)

# COMMAND ----------

# MAGIC %md ## write to target table

# COMMAND ----------

# MAGIC %md ## query info about table (source, target, etc.)

# COMMAND ----------

display(spark.sql("select *, input_file_name() from ggw_department.department"))
