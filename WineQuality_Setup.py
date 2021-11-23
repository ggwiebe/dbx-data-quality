# Databricks notebook source
# MAGIC %md # Template Setup Notebook

# COMMAND ----------

# MAGIC %md ## Create core database & tracking elements
# MAGIC e.g. database, tracking table, and set common variables, etc.
# MAGIC   
# MAGIC Use widgets to assist in setting these values.  
# MAGIC One can avoid re-creating various elements, so you can run by default without damaging existing.

# COMMAND ----------

# DBTITLE 1,Import common & local libraries
import json
from datetime import datetime

# import local functions
from utils.setup_functions import *


# COMMAND ----------

# DBTITLE 1,Get current notebook context
# Use dbutils to extract environment values
current_datetime = datetime.now()
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
tags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
notebook_json = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

default_user_path = "/Users/{}/".format(current_user)
current_notebook = notebook_json['extraContext']['notebook_path']

print(" Setup started at: {}\n     Current user: {}\nDefault User Path: {}\n    Notebook name: {}".format(current_datetime,current_user,default_user_path,current_notebook))

# COMMAND ----------

# MAGIC %md ## Create user widgets & run variables

# COMMAND ----------

# DBTITLE 1,Create interaction widgets
# Setup widgets for General Setup Process
dbutils.widgets.dropdown("setup_type", 'New',['New','Clear','Reset'],"Setup DB Options")
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

# DBTITLE 1,Get runtime variables
# Setup variables separate from current widget
db_name = dbutils.widgets.get("db_name")
track_table_name = dbutils.widgets.get("track_table_name")
store_loc = dbutils.widgets.get("store_loc")
setup_type = dbutils.widgets.get("setup_type")
create_table= dbutils.widgets.get("create_table")

print(" db_name: {}\n track_table_name: {}\n store_loc: {}\n setup_type: {}\n create_table: {}".format(db_name,track_table_name,store_loc,setup_type,create_table))

# COMMAND ----------

# Set data environment from current env plus user supplied values
db_loc = set_data_env(store_loc,db_name)
print(" db_loc: {}".format(db_loc))

# COMMAND ----------

# Create DB
if setup_type == "New":
  print("Setting up / creating new database {} in storage location {}...".format(db_name,db_loc))
  dbutils.fs.mkdirs(db_loc)
  print("...storage location created, db_loc={}.".format(db_loc))
  create_db(db_loc,db_name,spark)

elif setup_type == "Clear":
  print("TODO implement Clear")

elif setup_type == "Reset":
  print("TODO implement Reset")

# Create Tracking Table
if create_table == "Yes":
  print("Setting up load tracking table...")
  create_track_table(db_name,track_table_name,spark)

elif create_table == "No":
  print("Skipped the setting up of the load tracking table.")
  
print("Done!")

# drop_table(use_db,"quality_red_bronze")


print("config setup:")
print("  current_notebook = {}\n      current_user = {},\n default_user_path = {}".format(current_notebook,current_user,default_user_path))
print("            db_loc = {}\n           db_name = {}".format(db_loc,db_name))
print("  track_table_name = {}\n  current_datetime = {}".format(track_table_name,current_datetime))

# COMMAND ----------

from utils.setup_functions import *
setup_helper = TrackerSetupHelper(dbutils)
# setup_helper.db_utilstuff(store_loc)
# setup_helper.db_utilstuff("/Users/glenn.wiebe@databricks.com")
# setup_helper.db_utilstuff()
setup_helper.my_fs_test(dbutils)

