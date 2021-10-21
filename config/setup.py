# Databricks notebook source
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
default_user_path = "/Users/{}/".format(current_user)
db_name = "ggw_wine"

print("config setup: \n      current_user = {},\n default_user_path = {}\n           db_name = {}".format(current_user,default_user_path,db_name))

# COMMAND ----------


