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

# Use dbutils to extract environment values
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
tags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
notebook_json = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

default_user_path = "/Users/{}/".format(current_user)
current_notebook = notebook_json['extraContext']['notebook_path']

print("     Current user: {}\nDefault User Path: {}\n    Notebook name: {}".format(current_user,default_user_path,current_notebook))

# COMMAND ----------

# Setup widgets for General Setup Process
dbutils.widgets.dropdown("setup_type", 'new',['new','clear','reset'],"Setup DB Options")
dbutils.widgets.dropdown("create_table", "No", ["Yes", "No"], "Create Tracking Table")

# Setup widgets for Setup Config
dbutils.widgets.text("store_loc", default_user_path, "User Storage Location")
dbutils.widgets.text("db_name", "YourDb", "Database Name")
dbutils.widgets.text("track_table_name", "load_track", "Track Table Name")

# dbutils.widgets.remove("setup_type")
# dbutils.widgets.remove("create_table")
# dbutils.widgets.remove("store_loc")
# dbutils.widgets.remove("db_name")
# dbutils.widgets.remove("track_table_name")

# COMMAND ----------

# Setup variables separate from current widget
db_name = dbutils.widgets.get("db_name")
track_table_name = dbutils.widgets.get("track_table_name")
store_loc = dbutils.widgets.get("store_loc")
setup_type = dbutils.widgets.get("setup_type")
create_table= dbutils.widgets.get("create_table")

# COMMAND ----------

# Set data environment from current env plus user supplied values
set_data_env(store_loc,db_name)

# COMMAND ----------

# First get component to initialize environment (user and notebook name)
set_env(store_loc,db_name)

# COMMAND ----------

# Create DB
if setup_type == "new":
  print("Setting up / creating new database {db_name} in storage location {db_loc}...".format(db_name,db_loc))
  create_db(db_loc,db_name)

# Create DB
if create_table") == "Yes":
  print("Setting up load tracking table...")
  create_track_table(db_name,track_table_name)

print("Done!")

# drop_table(use_db,"quality_red_bronze")


print("config setup:")
print("  current_notebook = {}\n      current_user = {},\n default_user_path = {}".format(current_notebook,current_user,default_user_path))
print("         file_path = {}".format(file_path))
print("           db_name = {}\n  track_table_name = {}\n     load_start_dt = {}".format(db_name,table_name,load_start_dt))

# COMMAND ----------

# not done now...
load_start_dt = datetime.now()
