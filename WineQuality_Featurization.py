# Databricks notebook source
# MAGIC %md ## Wine Quality Featurization  
# MAGIC   
# MAGIC Here we will do any adjustments to the data before creating a Feature Table

# COMMAND ----------

# Set config for database name, file paths, and table names
database_name = 'ggw_wine'
 
# Read into Spark
qualityDF = spark.table('{}.quality_red_wine'.format(database_name))
 
display(qualityDF)

# COMMAND ----------

qualityDF.schema

# COMMAND ----------

from databricks.feature_store import feature_table
# from pyspark.sql.types import StructType,StructField,DoubleType,IntegerType

# qualDataSchema = StructType([
#         StructField('fixed_acidity',DoubleType(),True),
#         StructField('volatile_acidity',DoubleType(),True),
#         StructField('citric_acid',DoubleType(),True),
#         StructField('residual_sugar',DoubleType(),True),
#         StructField('chlorides',DoubleType(),True),
#         StructField('free_sulfur_dioxide',DoubleType(),True),
#         StructField('total_sulfur_dioxide',DoubleType(),True),
#         StructField('density',DoubleType(),True),
#         StructField('pH',DoubleType(),True),
#         StructField('sulphates',DoubleType(),True),
#         StructField('alcohol',DoubleType(),True),
#         StructField('quality',IntegerType(),True),
# ])

# This does not work because rdd has 16 cols
# qualDataDF = spark.createDataFrame(data=qualityDF.rdd, schema=qualDataSchema)

@feature_table
def compute_wine_quality_features(data):
  return data.select(*qualityDF.columns[:12],qualityDF.columns[15])

wine_quality_features_df = compute_quality_features(qualityDF)
display(wine_quality_features_df)

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

# wine_quality_features_df = compute_churn_features(telcoDF)

wine_quality_feature_table = fs.create_feature_table(
  name='{}.quality_red_wine_features'.format(database_name),
  keys='id',
  schema=wine_quality_features_df.schema,
  description='These features are derived from the {}.quality_red_wine table in the lakehouse.  The id is a synthetic value on load.  No aggregations were performed.'.format(database_name)
)

fs.write_table(df=wine_quality_features_df, name='{}.quality_red_wine_features'.format(database_name), mode='overwrite')

# COMMAND ----------

### Un-documented fs client api to delete feature table:
from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()

# Set config for database name, file paths, and table names
database_name = 'ggw_wine'

# fs._catalog_client.delete_feature_table('{}.quality_red_wine_features'.format(database_name))
# fs._catalog_client.delete_feature_table('ggw_wine.quality_red_wine_features')
# fs._catalog_client
# fs._catalog_client.delete_feature_table()

### Examine the churn feature dataframe being used to create the feature store table
# qualDataDF = compute_quality_features(qualityDF)
# qualDataDF.spark.schema()
