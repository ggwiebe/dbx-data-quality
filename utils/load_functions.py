# Databricks notebook source
# MAGIC %md ### Utility Notebook to set common load measures & functions

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# These should be set during load_config  
# db_name = "ggw_wine"
# track_table_name = "load_track"
# measure_usecase_tag = "data_load" 

# COMMAND ----------

# DBTITLE 1,Enrich Source dataframe with tracking columns
def enrich_source(df,start_dt,comp):
  enr_df = (df.withColumn('load_dt',lit(start_dt))
              .withColumn('load_source',input_file_name())
              .withColumn('load_component',lit(comp))
           )
  return(enr_df)

# COMMAND ----------

# DBTITLE 1,Rename Dataframe Columns to remove spaces
from pyspark.sql import functions as F

def rename_df_cols(df):
  nospace_df = df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])
  return(nospace_df)

# COMMAND ----------

# DBTITLE 1,Setup Database for this prototype
def setup_db(db_name):
  spark.sql("""CREATE DATABASE IF NOT EXIST {}
            """.format(db_name)
           )
  print("Database created: {}".format(name))

# COMMAND ----------

# DBTITLE 1,Drop Table utility function
def drop_table(db_name,table_name):
  spark.sql("DROP TABLE {}.{}".format(db_name,table_name)
           )
  print("Table dropped: {}.{}".format(db_name,table_name))

# COMMAND ----------

# DBTITLE 1,Add Source Row Identities
from pyspark.sql.functions import monotonically_increasing_id 
from pyspark.sql.window import Window

def id_source(df,part_col):
  ided_df = df.withColumn("id",monotonically_increasing_id())
#   id_numbered_df = (ided_df.withColumn(
#                      "row_id",row_number().over(Window.partitionBy(col(part_col)).orderBy(col(part_col)))
#                    ))

  return(ided_df)
#   return(id_numbered_df)

# COMMAND ----------

# DBTITLE 1,Create Load Tracking Table
def setup_load_track(db_name,load_table_name):
  # "load_start_dt","load_end_dt","load_process","load_source","load_target","load_rows","load_checksum","load_metrics_df"]
  print("Dropping existing load tracking table {}.{}".format(db_name,load_table_name))
  spark.sql("""DROP TABLE IF EXISTS {}.{}""".format(db_name,load_table_name))
  print("Creating load tracking table {}.{}".format(db_name,load_table_name))
  spark.sql("""CREATE TABLE IF NOT EXISTS {}.{} (
                 load_start_dt TIMESTAMP,
                 load_end_dt TIMESTAMP,
                 load_process STRING,
                 load_source STRING,
                 load_target STRING,
                 load_count LONG,
                 load_checksum DOUBLE,
                 load_metrics STRING
               )""".format(db_name,load_table_name)
           )
  print("Load tracking table created: {}.{}".format(db_name, load_table_name))

# COMMAND ----------

# DBTITLE 1,Track Load
# load_config must have been run first to setup the db_name and track_table_name
def track_load(load_start_dt, load_end_dt, load_process, load_source, load_target, load_rows, load_checksum, load_metrics):
  load = [(load_start_dt,load_end_dt,load_process,load_source,load_target,load_count,load_checksum,load_metrics)]
  load_cols = ["load_start_dt","load_end_dt","load_process","load_source","load_target","load_count","load_checksum","load_metrics"]
  load_df = spark.createDataFrame(data=load, schema = load_cols)
  
  enr_df = (load_df.write
             .format("delta")
             .mode("append")
             .saveAsTable("{}.{}".format(db_name,track_table_name))
           )
  return(enr_df)
