# Databricks notebook source
# MAGIC %md ## Create Database Stuff  
# MAGIC   
# MAGIC Defaults to not doing this, so you can run by default without damaging existing.

# COMMAND ----------

dbutils.widgets.dropdown("CreateDB", "No", ["Yes", "No"], "Create Database")
dbutils.widgets.dropdown("CreateTable", "No", ["Yes", "No"], "Create Tracking Table")
# dbutils.widgets.remove("Read Data")

# COMMAND ----------

# Setup DB
if dbutils.widgets.get("CreateDB") == "Yes":
  print("Setting up ggw_wine database...")
  setup_db(use_db)

if dbutils.widgets.get("CreateTable") == "Yes":
  print("Setting up load tracking table...")
  setup_load_track(use_db,use_table)

print("Done!")

# drop_table(use_db,"quality_red_bronze")
