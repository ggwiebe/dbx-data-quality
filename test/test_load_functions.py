# Databricks notebook source
# MAGIC %run ../utils/load_functions

# COMMAND ----------

def test_reversed():
    assert list(rename_df_cols([1, 2, 3, 4])) == [4, 3, 2, 1]

