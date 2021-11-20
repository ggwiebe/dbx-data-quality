# Databricks notebook source
# MAGIC %md # Ingest Files (add calc & store metrics)

# COMMAND ----------

# MAGIC %md ## Import requirements, define config & utilities

# COMMAND ----------

# DBTITLE 1,Install python requirements
pip install -r requirements.txt

# COMMAND ----------

# Replace running a notebook, with an import (as below)
# %run ./config/load_setup

# COMMAND ----------

from utils.load_functions import *

# COMMAND ----------

# MAGIC %run ./utils/load_functions

# COMMAND ----------

dbutils.widgets.dropdown("ReadData", "Yes", ["Yes", "No"], "Re-read Data")
# src_file = "file:/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/winequality-red.csv"
# src_file = "file:/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/winequality-red-badchecksum.csv"
src_file = "file:/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/winequality-red-missingrows.csv"
target_table = "quality_red_bronze"
# dbutils.widgets.remove("Read Data")

# COMMAND ----------

# MAGIC %sh ls -l /Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/winequality-red-missingrows.csv

# COMMAND ----------

display(dbutils.fs.ls(src_file))

# COMMAND ----------

# MAGIC %md ## Read source data   
# MAGIC   
# MAGIC e.g. Read csv file (here using pyspark)

# COMMAND ----------

import os

# "file:" prefix and absolute file path are required for PySpark
if dbutils.widgets.get("ReadData") == "Yes":
  source_df = (spark.read.format('csv')
                         .option('header',True)
                         .option('inferSchema',True)
                         .load(src_file.format(current_user))
              )

  display(source_df)
else:
  print("Re-read Drop-down set to No - skipping data read!!!")

# COMMAND ----------

# MAGIC %md ## Enrich source dataframe  
# MAGIC   
# MAGIC Add the name of the load process, the start timestamp of this load

# COMMAND ----------

source_enh_df = enrich_source(source_df,load_start_dt,'notebook:{}/Load2Bronze'.format(os.getcwd()))
display(source_enh_df)

# COMMAND ----------

# MAGIC %md ## Add row identifier to this particular dataset  
# MAGIC   
# MAGIC Must enrich the source first to get a field on which to create the incrementing row id

# COMMAND ----------

source_enh_num_df = id_source(source_enh_df,'quality')
display(source_enh_num_df)

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

# MAGIC %md ## Normalize dataframe  
# MAGIC   
# MAGIC e.g. fix column names, types, etc.

# COMMAND ----------

source_enh_num_norm_df = rename_df_cols(source_enh_num_df)
display(source_enh_num_norm_df)

# COMMAND ----------

# MAGIC %md ## Create Load Checksum info  
# MAGIC   
# MAGIC Gather metrics of the load set.  
# MAGIC - get a list of the column sums, for adding to load_track function later

# COMMAND ----------

source_enh_num_norm_agg_chksum_df = source_enh_num_norm_df.groupBy().agg(
                                                                            sum("fixed_acidity").alias("sum_fixed_acidity"),
                                                                            sum("volatile_acidity").alias("sum_volatile_acidity"),
                                                                            sum("citric_acid").alias("sum_citric_acid"),
                                                                            sum("residual_sugar").alias("sum_residual_sugar"),
                                                                            sum("chlorides").alias("sum_chlorides"),
                                                                            sum("free_sulfur_dioxide").alias("sum_free_sulfur_dioxide"),
                                                                            sum("total_sulfur_dioxide").alias("sum_total_sulfur_dioxide"),
                                                                            sum("density").alias("sum_density"),
                                                                            sum("pH").alias("sum_pH"),
                                                                            sum("sulphates").alias("sum_sulphates"),
                                                                            sum("alcohol").alias("sum_alcohol"),
                                                                            sum("quality").cast('double').alias("sum_quality"),
                                                                            sum("id").alias("sum_id"),
                                                                            count("id").alias("count")
                                                                          )
# (F.sum('numb').cast('decimal(38,3)')).show()

load_count    = source_enh_num_norm_agg_chksum_df.first()["count"]
load_checksum = source_enh_num_norm_agg_chksum_df.first()["sum_quality"]

display(source_enh_num_norm_agg_chksum_df)
source_enh_num_norm_agg_chksum_json = source_enh_num_norm_agg_chksum_df.toJSON().first()

print(source_enh_num_norm_agg_chksum_json)

# COMMAND ----------

# MAGIC %md ## Write to target table

# COMMAND ----------

# drop_table("quality_red_bronze")
write_ret = (source_enh_num_norm_df.write
              .mode("append")
              .format("delta")
              .saveAsTable("{}.{}".format(db_name,target_table))
            )

# COMMAND ----------

# MAGIC %md ## Track load to tracking table. 
# MAGIC   
# MAGIC Once written successfully, write out a single record to the tracking table with the relevant information about this load step.

# COMMAND ----------

from datetime import datetime

track_ret = ( track_load(load_start_dt,
                         datetime.now(),
                         current_notebook,
                         src_file,
                         target_table,
                         load_count,
                         load_checksum,
                         source_enh_num_norm_agg_chksum_json
                        )
            )

# COMMAND ----------

# MAGIC %md ## query info about table (source, target, etc.)

# COMMAND ----------

display(spark.sql("select *, input_file_name() from {}.{}".format(db_name,target_table)))

# COMMAND ----------

# MAGIC %md ## Compare load_source as actually loaded to source directory  
# MAGIC   
# MAGIC NOTE: The opposite of below!!! :-)

# COMMAND ----------

# input file list - in clause
# file_list = 'file:/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/winequality-red.csv'

# print("SELECT * FROM {}.quality_red_bronze WHERE load_source NOT IN ('{}')".format(db_name,file_list))
# display(spark.sql("SELECT * FROM {}.quality_red_bronze WHERE load_source NOT IN '{}'".format(db_name,file_list)))


# COMMAND ----------

display(sql("SELECT * FROM {}.{} WHERE load_source NOT IN ('{}')".format(use_db,target_table,src_file)))

# COMMAND ----------

src_file
