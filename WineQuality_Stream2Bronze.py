# Databricks notebook source
# MAGIC %md # Stream Ingest Files (add calc & store metrics)

# COMMAND ----------

# MAGIC %md ## Import requirements, define config & utilities  
# MAGIC   
# MAGIC Note, the below import uses the local python library files;
# MAGIC An alternate approach required pre-arbitrary files was to use %run like this:
# MAGIC ```
# MAGIC %run ./config/load_setup
# MAGIC ```
# MAGIC but this runs within the context of the external notebook, not within this notebook.

# COMMAND ----------

# DBTITLE 1,Install python requirements
pip install -r requirements.txt

# COMMAND ----------

# Replace running a notebook, with an import (as below)
# %run ./config/load_setup

# COMMAND ----------

# MAGIC %md ### Import ConfigHelper object

# COMMAND ----------

#%run ./utils/load_functions
from utils.load_functions import *
from utils.config_helper import *
config_helper = ConfigHelper(spark,dbutils)

# COMMAND ----------

# MAGIC %md ### Initialize the environment

# COMMAND ----------

ret = config_helper.init_env()

# COMMAND ----------

# MAGIC %md ### Configure the environment (from settings json/secrets)

# COMMAND ----------

# use the key/value pairs to configure the UI for interactively configuring the setup notebook
ret = config_helper.set_config(
                        dbutils.secrets.get(scope="ggw_scope", key="store_loc"), 
                        dbutils.secrets.get(scope="ggw_scope", key="db_name"),
                        dbutils.secrets.get(scope="ggw_scope", key="track_table_name"), 
)

# COMMAND ----------

# MAGIC %md ### Set DB Environment

# COMMAND ----------

ret = config_helper.set_data_env(config_helper.store_loc,config_helper.db_name)
# Workaround to display config secret information
displayHTML("<span/>".join("store_loc: " + config_helper.store_loc))
displayHTML("<span/>".join("db_name: " + config_helper.db_name))
displayHTML("<span/>".join("data_file_loc: " + config_helper.data_file_loc))
displayHTML("<span/>".join("db_loc: " + config_helper.db_loc))
displayHTML("<span/>".join("track_table_name: " + config_helper.track_table_name))


# COMMAND ----------

# MAGIC %md ### Personalize Run Interactively

# COMMAND ----------

dbutils.widgets.dropdown("ReadData", "Yes", ["Yes", "No"], "Re-read Data")
dbutils.widgets.dropdown("LocRelative", "ReposLoc", ["ReposLoc", "StoreLoc"], "Location Relative To")
dbutils.widgets.text("SourceFile", "data/winequality-red.csv", "Source File")
dbutils.widgets.text("SourceFolder", "data/folder/", "Source File")
dbutils.widgets.text("TargetTable", "quality_red_wine", "Target Table Name")
# source_file = "file:/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/winequality-red.csv"
# source_file = "file:/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/winequality-red-badchecksum.csv"
#dbutils.widgets.remove("SourceRelative")

# COMMAND ----------

# Get runtime values from interactive widgets

# Either Read data from Repos or from a Store Location...
if dbutils.widgets.get("LocRelative") == "ReposLoc":
  # file:/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/winequality-red-missingrows.csv"
  source_file = "/Workspace{}/{}".format(config_helper.folder,dbutils.widgets.get("SourceFile"))
  source_file_url = "file:"+source_file
  source_folder = "/Workspace{}/{}".format(config_helper.folder,dbutils.widgets.get("SourceFolder"))
  source_folder_url = "file:"+source_folder
elif dbutils.widgets.get("LocRelative") == "StoreLoc":
  source_file = "{}/{}".format(config_helper.data_file_loc,dbutils.widgets.get("SourceFile"))
  source_file_url = "dbfs:"+source_file
  source_folder = "{}/{}".format(config_helper.data_file_loc,dbutils.widgets.get("SourceFolder"))
  source_folder_url = "dbfs:"+source_folder

target_table = dbutils.widgets.get("TargetTable")
#"quality_red_bronze"
  
print("      source_file: {}\n  source_file_url: {}".format(source_file,source_file_url))
print("    source_folder: {}\nsource_folder_url: {}".format(source_folder,source_folder_url))
print("     target_table: {}".format(target_table))

# COMMAND ----------

import os
os.environ['SOURCE_FOLDER'] = source_folder
os.environ['SOURCE_FILE'] = source_file

# COMMAND ----------

# MAGIC %sh ls -l /Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/winequality-red-missingrows.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC echo "listing source folder:" $SOURCE_FOLDER
# MAGIC ls -l $SOURCE_FOLDER
# MAGIC echo "listing source file:" $SOURCE_FILE
# MAGIC ls -l $SOURCE_FILE
# MAGIC echo "listing source file: data/winequality-red.csv"
# MAGIC ls -l data/winequality-red.csv
# MAGIC pwd

# COMMAND ----------

# Workaround to display config secret information
displayHTML("<span/>".join(config_helper.data_file_loc))
displayHTML("<span/>".join(config_helper.db_loc))
displayHTML("<span/>".join(source_file))

# This de-redaction technique does not work :-(
# x_d_f_l = "x-{}".format(config_helper.data_file_loc)
# print(x_d_f_l)
# x_d_f_l_2 = "x" + config_helper.data_file_loc
# print(x_d_f_l_2)

# COMMAND ----------

# MAGIC %md ## Read source data   
# MAGIC   
# MAGIC e.g. Read csv file (here using pyspark)

# COMMAND ----------

# Start the clock
now_ts = config_helper.set_start_now()
print("Start timestamp set as {}.".format(now_ts))

# COMMAND ----------

import os

displayHTML("<span/>".join("source_file: " + source_file))

# "file:" prefix and absolute file path are required for PySpark
if dbutils.widgets.get("ReadData") == "Yes":
  source_df = (spark.read.format('csv')
                         .option('header',True)
                         .option('inferSchema',True)
#                          .load(source_file.format(current_user))
                         .load(source_file_url)
              )

  display(source_df)
else:
  print("Re-read Drop-down set to No - skipping data read!!!")

# COMMAND ----------

# MAGIC %md ## Enrich source dataframe  
# MAGIC   
# MAGIC Add the name of the load process, the start timestamp of this load

# COMMAND ----------

source_enh_df = enrich_source(source_df,config_helper.start_timestamp,'notebook:{}'.format(config_helper.current_notebook))
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
              .saveAsTable("{}.{}".format(config_helper.db_name,target_table))
            )

# COMMAND ----------

# MAGIC %md ## Track load to tracking table. 
# MAGIC   
# MAGIC Once written successfully, write out a single record to the tracking table with the relevant information about this load step.

# COMMAND ----------

from datetime import datetime

track_ret = ( track_load(config_helper.spark, config_helper.db_name, config_helper.track_table_name, 
                         config_helper.start_timestamp,
                         datetime.now(),
                         config_helper.current_notebook,
                         source_file_url,
                         target_table,
                         load_count,
                         load_checksum,
                         source_enh_num_norm_agg_chksum_json
                        )
            )

# COMMAND ----------

# MAGIC %md ## query info about table (source, target, etc.)

# COMMAND ----------

display(spark.sql("select *, input_file_name() Delta_Part_File from {}.{}".format(config_helper.db_name,target_table)))

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

display(sql("SELECT * FROM {}.{} WHERE load_source NOT IN ('{}')".format(use_db,target_table,source_file)))

# COMMAND ----------

source_file
