# Databricks notebook source
# MAGIC %md ## Setup notebook for prototype: create database, tracking table, and set common variables, etc.
# MAGIC   
# MAGIC Defaults to not doing this, so you can run by default without damaging existing.

# COMMAND ----------
import json
from datetime import datetime

# import local functions
from utils.setup_functions import *

# COMMAND ----------

# TODO resolve these "create" widgets with "reset" widget 
# Setup widgets for General Setup Process
dbutils.widgets.dropdown("SetupType", 'new',['new','clear','reset'],"Setup DB Options")
dbutils.widgets.dropdown("CreateTable", "No", ["Yes", "No"], "Create Tracking Table")

# Setup widgets for Setup Config
dbutils.widgets.text("store_loc", default_user_path, "User Storage Location")
dbutils.widgets.text("db_name", db_name, "Database Name")
dbutils.widgets.text("track_table_name", track_table_name, "Track Table Name")

# COMMAND ----------

# Setup variables separate from current widget
db_name = dbutils.widgets.get("db_name")
track_table_name = dbutils.widgets.get("track_table_name")
store_loc = dbutils.widgets.get("store_loc")
SetupType = dbutils.widgets.get("SetupType")

# Remove widgets as necessary
# dbutils.widgets.remove("db_name")


# COMMAND ----------

# First get component to initialize environment (user and notebook name)
set_env(store_loc,db_name)

# COMMAND ----------

# Create DB
if SetupType == "new":
  print("Setting up / creating new database {db_name} in storage location {db_loc}...".format(db_name,db_loc))
  create_db(db_loc,db_name)

if dbutils.widgets.get("CreateTable") == "Yes":
  print("Setting up load tracking table...")
  create_track_table(db_name,track_table_name)

print("Done!")

# drop_table(use_db,"quality_red_bronze")


notebook_path_json = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
current_notebook = notebook_path_json['extraContext']['notebook_path']

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
default_user_path = "/Users/{}/".format(current_user)
db_name = "ggw_wine"
track_table_name = "load_track"
file_path = "file:/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/"
measure_usecase_tag = "data_load" 

load_start_dt = datetime.now()

print("config setup:")
print("  current_notebook = {}\n      current_user = {},\n default_user_path = {}".format(current_notebook,current_user,default_user_path))
print("         file_path = {}".format(file_path))
print("           db_name = {}\n  track_table_name = {}\n     load_start_dt = {}".format(db_name,table_name,load_start_dt))
