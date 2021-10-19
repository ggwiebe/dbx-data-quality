# Databricks notebook source
# MAGIC %md # Check File Age

# COMMAND ----------

# MAGIC %md ## read source file  
# MAGIC   
# MAGIC - e.g. Read csv file
# MAGIC - Add input_file_name() to dataframe

# COMMAND ----------

# MAGIC %md ## write to target table

# COMMAND ----------

# MAGIC %md ## query info about table (source, target, etc.)

# COMMAND ----------

display(spark.sql("select *, input_file_name() from ggw_department.department"))
