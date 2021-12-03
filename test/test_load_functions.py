# Databricks notebook source
db_name = "ggw_wine"

# COMMAND ----------

import json
from datetime import datetime

# import local functions
#%run ./utils/load_functions
from utils.load_functions import *
from utils.config_helper import *
config_helper = ConfigHelper(spark,dbutils)

# COMMAND ----------

def test_reversed():
    assert list(rename_df_cols([1, 2, 3, 4])) == [4, 3, 2, 1]


# COMMAND ----------

def test_setup(db_name):
    spark.sql("DESCRIBE DATABASE {}".format(db_name))
#     assert (spark.sql("DESCRIBE DATABASE {db_name}") != "")
    assert 1 == 1


# COMMAND ----------

# ret = test_setup(db_name)
ret = test_setup("ggw_notthere")

