# Databricks notebook source
# MAGIC %md ## Interact with File System as a data source  
# MAGIC   
# MAGIC With this information we can then compare file source to loaded data.

# COMMAND ----------

# MAGIC %md ### Setup notebook

# COMMAND ----------

# MAGIC %run ./config/setup

# COMMAND ----------



# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.text("notebook", dbutils.notebook.getContext().notebookPath.get)

# COMMAND ----------

current_notebook = dbutils.widgets.get("notebook")
print("Using notebook: {}".format(current_notebook))

# COMMAND ----------

# MAGIC %md ### Get source directory as a data source

# COMMAND ----------

import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

dir_ls = dbutils.fs.ls("file:/Workspace/Repos/{}/dbx-data-quality/data/".format(current_user))
# dir_ls = dbutils.fs.ls("{}/ggw_wine".format(default_user_path))
# df=spark.read.csv(f"file:{os.getcwd()}/data/winequality-red.csv", header=True) # "file:" prefix and absolute file path are required for PySpark

display(dir_ls)

# # if you wanted to look at the first row
# dir_path = dir_ls[0].path
# print("dir_ls(path): {}".format(dir_path))

# COMMAND ----------

# MAGIC %md ### Create Dataframe from directory & create a temp view

# COMMAND ----------

dirls_schema = StructType([StructField("path", StringType(), True),
                            StructField("name", StringType(), True),
                            StructField("size", IntegerType(), True)],)
dir_df = sqlContext.createDataFrame(sc.parallelize(dir_ls), dirls_schema)
# display(dir_df)

dir_df.createOrReplaceTempView('source_files')
display(sql("SELECT * FROM source_files"))

# COMMAND ----------

# For interest, get the path val from the first row
path_val = dir_df.select('path').collect()[0].path
print("First file in source directory: {}".format(path_val))

# COMMAND ----------

# MAGIC %md ### Query directory source against Loaded Data Source

# COMMAND ----------

# Select all data against the source file
matched_df = sql("SELECT s.path source_file, d.* FROM source_files s LEFT OUTER JOIN {}.quality_red_bronze d ON s.path = d.load_source".format(db_name))

display(matched_df)

# COMMAND ----------

# MAGIC %md ### Query aggregate info by source file

# COMMAND ----------

# Select aggregate info by source file
matched_df = sql("""SELECT s.path source_file, 
                           COUNT(quality) quality_count, 
                           SUM(quality) quality_sum 
                      FROM source_files s 
                      LEFT OUTER JOIN {}.quality_red_bronze d 
                        ON s.path = d.load_source 
                     GROUP BY s.path""".format(db_name))

display(matched_df)

# COMMAND ----------

# MAGIC %md ### Check for missing files

# COMMAND ----------

# Select aggregate having nulls or zero count (i.e. quality_count = 0 opposite of quality_sum > 0)
not_used_df = sql("""SELECT s.path source_file, 
                           COUNT(quality) quality_count, 
                           SUM(quality) quality_sum 
                      FROM source_files s 
                      LEFT OUTER JOIN {}.quality_red_bronze d 
                        ON s.path = d.load_source 
                     GROUP BY s.path
                    HAVING quality_count = 0
                 """.format(db_name))

# what is the count of files not used:
not_used_count = not_used_df.count()
print("Count of source file with no loaded data: {}".format(not_used_count))

#
if not_used_count > 0:
  print("Found source files with no loaded data:")
  # display those source files not used
  display(not_used_df)


# COMMAND ----------

# MAGIC %md ### Throw exception if files do not match

# COMMAND ----------

#       current_user = glenn.wiebe@databricks.com,
#  default_user_path = /Users/glenn.wiebe@databricks.com/
#            db_name = ggw_wine

if not_used_count > 0:
#   raise Exception("Notebook: {} Business Failure!!!; Found {} source files with no loaded data: {}".format(current_notebook,not_used_count,not_used_df[0].source_file))
  raise Exception("Notebook: mynotebook Business Failure!!!; Found {} source files with no loaded data: {}".format(not_used_count,not_used_df[0].source_file))

# COMMAND ----------


