# Databricks notebook source
# MAGIC %md ## Compare load_source as actually loaded to source directory  
# MAGIC   
# MAGIC Use view on file list dataframe

# COMMAND ----------

#%run ./utils/load_functions
from utils.load_functions import *
from utils.config_helper import *
config_helper = ConfigHelper(spark,dbutils)

# COMMAND ----------

ret = config_helper.init_env()

# COMMAND ----------

# print(source_file)
print(source_folder_url)

# COMMAND ----------

sourceList = dbutils.fs.ls(source_folder_url)
sourceDF = spark.createDataFrame(data=sourceList)
sourceDF.createOrReplaceTempView('wine_quality_source_files')

spark.sql(
    """
    CREATE TEMP VIEW wine_quality_source_file AS
    SELECT *
    FROM wine_quality_source_files
    """
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM wine_quality_source_file

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check all source files loaded
# MAGIC SELECT DISTINCT(load_source)
# MAGIC     --   , load_end_dt
# MAGIC   FROM ggw_wine.load_track
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check all source files loaded
# MAGIC SELECT DISTINCT(load_source)
# MAGIC     --   , load_end_dt
# MAGIC   FROM ggw_wine.load_track
# MAGIC ;
