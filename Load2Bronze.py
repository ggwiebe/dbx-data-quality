# Databricks notebook source
# MAGIC %md # Ingest Files (add ingest metrics)

# COMMAND ----------

# MAGIC %md ## Import requirements & utils

# COMMAND ----------

pip install -r requirements.txt

# COMMAND ----------

# MAGIC %run ./config/setup

# COMMAND ----------

# MAGIC %run ./utils/load_functions

# COMMAND ----------

# MAGIC %md ## Read source data   
# MAGIC   
# MAGIC e.g. Read csv file (here using pyspark)

# COMMAND ----------

import os

# "file:" prefix and absolute file path are required for PySpark
source_df = (spark.read.format('csv')
                       .option('header',True)
                       .option('inferSchema',True)
                       .load("file:/Workspace/Repos/{}/dbx-data-quality/data/winequality-red.csv".format(current_user))
  )

display(source_df)

# COMMAND ----------

# import os

# # "file:" prefix and absolute file path are required for PySpark
# source_df = (spark.read.format('csv')
#                        .option('header',True)
#                        .option('inferSchema',True)
#                        .load(f"file:{os.getcwd()}/data/winequality-red.csv")
#   )

# display(source_df)

# COMMAND ----------

# MAGIC %md ## Enrich source dataframe

# COMMAND ----------

source_enh_df = enrich_source(source_df,'notebook:{}/Load2Bronze'.format(os.getcwd()))
display(source_enh_df)

# COMMAND ----------

# MAGIC %md ## Normalize dataframe  
# MAGIC   
# MAGIC e.g. fix column names, types, etc.

# COMMAND ----------

source_enh_nn_df = rename_df_cols(source_enh_df)
display(source_enh_nn_df)

# COMMAND ----------

# MAGIC %md ## write to target table

# COMMAND ----------

(source_enh_nn_df.write
             .format("delta")
             .mode("overwrite")
             .saveAsTable("{}.quality_red_bronze".format(db_name))
)

# COMMAND ----------

# MAGIC %md ## query info about table (source, target, etc.)

# COMMAND ----------

display(spark.sql("select *, input_file_name() from {}.quality_red_bronze".format(db_name)))

# COMMAND ----------

# MAGIC %md ## Compare load_source as actually loaded to source directory  
# MAGIC   
# MAGIC NOTE: The opposite of below!!! :-)

# COMMAND ----------

# input file list - in clause
file_list = 'file:/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/winequality-red.csv'

print("SELECT * FROM {}.quality_red_bronze WHERE load_source NOT IN ('{}')".format(db_name,file_list))
# display(spark.sql("SELECT * FROM {}.quality_red_bronze WHERE load_source NOT IN '{}'".format(db_name,file_list)))


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ggw_wine.quality_red_bronze WHERE load_source NOT IN ('file:/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/winequality-red.csv')
