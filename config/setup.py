# Databricks notebook source
# MAGIC %md ## Setup environment

# COMMAND ----------

import re

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

default_user_path = "/Users/{}/quality".format(current_user)
print(default_user_path)

dbutils.widgets.text("db_name", "ggw_dataquality", "Database Name")
dbutils.widgets.text("file_loc", default_user_path, "File Location")

# COMMAND ----------

# dbutils.widgets.remove("file_loc")
tags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
print(tags)

# COMMAND ----------

db_name = dbutils.widgets.get("db_name")
file_loc = dbutils.widgets.get("file_loc")

print("Using Database: {}, and file location: {}".format(db_name, file_loc))


# COMMAND ----------

reset_all = dbutils.widgets.get("reset_all_data") == "true"

if reset_all:
  spark.sql(f"DROP DATABASE IF EXISTS {dbName} CASCADE")
  dbutils.fs.rm(cloud_storage_path, True)

spark.sql(f"""create database if not exists {dbName} LOCATION '{cloud_storage_path}/tables' """)
spark.sql(f"""USE {dbName}""")
print("using cloud_storage_path {}".format(cloud_storage_path))
