# Databricks notebook source
# MAGIC %md # Featurize Wine Quality from source delta table  
# MAGIC   
# MAGIC All below needs to be re-worked!!!!!!

# COMMAND ----------

# MAGIC %md ## Import requirements & utils

# COMMAND ----------

pip install -r requirements.txt

# COMMAND ----------

# MAGIC %run ./config/setup

# COMMAND ----------

# MAGIC %run ./utils/load_functions

# COMMAND ----------

# MAGIC %md ## Profile source data   
# MAGIC   
# MAGIC e.g. Read Delta table and summarize

# COMMAND ----------

bronze_df = sql("SELECT * FROM {}.quality_red_bronze".format(db_name))

dbutils.data.summarize(bronze_df)

# COMMAND ----------

# MAGIC %md ## Define Feature Table(s)

# COMMAND ----------

from databricks.feature_store import feature_table

quality_cols = ["fixed_acidity",
                "volatile_acidity",
                "citric_acid",
                "residual_sugar",
                "chlorides",
                "free_sulfur_dioxide",
                "total_sulfur_dioxide",
                "density",
                "pH",
                "sulphates",
                "alcohol"]

@feature_table
def compute_quality_features(data):
  return data.select(quality_cols)

quality_df = compute_quality_features(bronze_df)
display(quality_df)

# COMMAND ----------

# MAGIC %md ## Create Feature Table(s)

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

quality_features_table = fs.create_feature_table(
  name='{}.quality_features'.format(db_name),
  keys='customerID',
  schema=quality_df.schema,
  description='Red Wine - Quality Features')

# COMMAND ----------

# MAGIC %md ## Normalize dataframe  
# MAGIC   
# MAGIC e.g. fix column names, types, etc.

# COMMAND ----------

# MAGIC %md ## write to target table

# COMMAND ----------

(source_enh_nn_df.write
             .format("delta")
             .mode("overwrite")
             .saveAsTable("{}.quality_red_bronze".format(db_name))
)

# COMMAND ----------

# MAGIC %md ## query info about table (source, target, etc.)

# COMMAND ----------

# MAGIC %md ## Compare load_source as actually loaded to source directory  
# MAGIC   
# MAGIC NOTE: The opposite of below!!! :-)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ggw_wine.quality_red_bronze WHERE load_source NOT IN ('file:/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/data/winequality-red.csv')
