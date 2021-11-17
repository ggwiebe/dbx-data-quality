# Databricks notebook source
# MAGIC %md ### Setup Notebook to set common variables

# COMMAND ----------

import json
from datetime import datetime

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

# COMMAND ----------

# MAGIC %sh pwd
