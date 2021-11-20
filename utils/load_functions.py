# imports
from pyspark.sql.functions import *
from pyspark.sql.functions import monotonically_increasing_id 
from pyspark.sql.window import Window

# Function to setup component name variable
def get_component():
  print("get_component called: finding component info...")
  tags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
  print(tags)

  notebook_path_json = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
  current_notebook = notebook_path_json['extraContext']['notebook_path']
  current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

  print("Notebook name: {current_notebook}, current user: {current_user}...".format(current_notebook,current_user))


# Enrich Source dataframe with tracking columns
def enrich_source(df,start_dt,comp):
  enr_df = (df.withColumn('load_dt',lit(start_dt))
              .withColumn('load_source',input_file_name())
              .withColumn('load_component',lit(comp))
           )
  return enr_df

# Rename Dataframe Columns to remove spaces
from pyspark.sql import functions as F

def rename_df_cols(df):
  nospace_df = df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])
  return nospace_df

# Add Source Row Identities
def id_source(df,part_col):
  ided_df = df.withColumn("id",monotonically_increasing_id())
  return(ided_df)

  #id_numbered_df = (ided_df.withColumn(
  #                   "row_id",row_number().over(Window.partitionBy(col(part_col)).orderBy(col(part_col)))
  #                 ))
  #return id_numbered_df


# Track Load
# assumes database and tracking table have already been setup (see setup_functions.py) 
def track_load(load_start_dt, load_end_dt, load_process, load_source, load_target, load_count, load_checksum, load_metrics):
  load = [(load_start_dt,load_end_dt,load_process,load_source,load_target,load_count,load_checksum,load_metrics)]
  load_cols = ["load_start_dt","load_end_dt","load_process","load_source","load_target","load_count","load_checksum","load_metrics"]
  load_df = spark.createDataFrame(data=load, schema=load_cols)
  
  enr_df = (load_df.write
             .format("delta")
             .mode("append")
             .saveAsTable("{}.{}".format(db_name,track_table_name))
           )
  return enr_df
