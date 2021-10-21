# Databricks notebook source
# MAGIC %md ## Setup environment

# COMMAND ----------

# MAGIC %run ../config/setup

# COMMAND ----------

import re

dbutils.widgets.text("db_name", db_name, "Database Name")
dbutils.widgets.text("user_loc", default_user_path, "User Location")
dbutils.widgets.dropdown("reset_all_data",'new',['new','clear','reset'],"Rest Run Selector")

# COMMAND ----------

# dbutils.widgets.remove("db_name")
# dbutils.widgets.remove("user_file_loc")

# COMMAND ----------

# dbutils.widgets.remove("file_loc")
tags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
print(tags)

# COMMAND ----------

data_file_loc = "{}{}".format(dbutils.widgets.get("user_loc"),db_name) 
db_loc = "{}{}/{}.db".format(dbutils.widgets.get("user_loc"),db_name,db_name) 

print("Using...\n            Database = {},\n       file location = {}\n db storage location = {}".format(db_name, data_file_loc, db_loc))

# COMMAND ----------

print(dbutils.widgets.get("reset_all_data"))

# COMMAND ----------

new   = dbutils.widgets.get("reset_all_data") == "new"
clear = dbutils.widgets.get("reset_all_data") == "clear"
reset = dbutils.widgets.get("reset_all_data") == "reset"

if new:
  print("Creating db_storage directory: {} and using it to create new database: {}...".format(db_loc,db_name))
  dbutils.fs.mkdirs(db_loc)
  spark.sql(f"""CREATE DATABASE {db_name}
    LOCATION '{db_loc}'
  """)

if reset:
  print("Dropping database: {} and deleting storage directory: {}...".format(db_name,db_loc))
  spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
  dbutils.fs.rm(data_file_loc, True)

if clear:
  print("Clearing storage directory: {}...".format(db_loc))
  dbutils.fs.rm(data_file_loc, False)

spark.sql(f"""CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_loc}' """)
spark.sql(f"""USE {db_name}""")

display(dbutils.fs.ls(data_file_loc))
