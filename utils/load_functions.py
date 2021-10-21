# Databricks notebook source
# MAGIC %md ### Utility Notebook to set common load measures & functions

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

measure_usecase_tag = "data_load" 

# COMMAND ----------

def enrich_source(df,comp):
  enr_df = (df.withColumn('load_dt',current_timestamp())
              .withColumn('load_source',input_file_name())
              .withColumn('load_component',lit(comp))
           )
  return(enr_df)

# COMMAND ----------

from pyspark.sql import functions as F

def rename_df_cols(df):
  nospace_df = df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])
  return(nospace_df)

# COMMAND ----------

def setup_db(db_name):
  spark.sql("""CREATE DATABASE IF NOT EXIST {}
            """.format(db_name)
           )
  print("Database created: {}".format(name))
