# Databricks notebook source
# MAGIC %md ## Interact with File System as a data source  
# MAGIC   
# MAGIC With this information we can then compare file source to loaded data.

# COMMAND ----------

# MAGIC %run ./config/setup

# COMMAND ----------

import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
default_user_path = "/Users/{}/".format(current_user)

dir_ls = dbutils.fs.ls("file:/Workspace/Repos/{}/dbx-data-quality/data/winequality-red.csv".format(current_user))
# dir_ls = dbutils.fs.ls("{}/ggw_wine".format(default_user_path))
# df=spark.read.csv(f"file:{os.getcwd()}/data/winequality-red.csv", header=True) # "file:" prefix and absolute file path are required for PySpark

display(dir_ls)
dir_path = dir_ls[0].path
print("dir_ls(path): {}".format(dir_path))

# COMMAND ----------

dirls_schema = StructType([StructField("path", StringType(), True),
                            StructField("name", StringType(), True),
                            StructField("size", IntegerType(), True)],)
dir_df = sqlContext.createDataFrame(sc.parallelize(dir_ls), dirls_schema)
display(dir_df)

dir_df.createOrReplaceTempView('source_files')
display(sql("SELECT * FROM source_files"))

# COMMAND ----------

path_val = dir_df.select('path').collect()[0].path
path_val

# COMMAND ----------

# "SELECT count(*) FROM ggw_wine.quality_red_bronze WHERE load_source = \'{}\'".format(path_val)
# matched_df = sql("SELECT d.load_source, s.path FROM {}.quality_red_bronze d INNER JOIN source_files s ON s.path = d.load_source".format(db_name))
matched_df = sql("SELECT s.path source_file, d.* FROM source_files s LEFT OUTER JOIN {}.quality_red_bronze d ON s.path = d.load_source".format(db_name))

display(matched_df)
