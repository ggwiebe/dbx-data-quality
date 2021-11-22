### MAGIC %run ../config/setup

# import common librarys
import json
from datetime import datetime

# Function to setup component name variable
def get_current_notebook(tags,notebook_json,user):
  print("get_component called: finding component info...")
  # tags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
  # print(tags)

  # notebook_path_json = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
  current_notebook = notebook_json['extraContext']['notebook_path']
  # current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

  print("Notebook name: {current_notebook}, current user: {current_user}...".format(current_notebook,current_user))

# Function to setup component name variable
def set_data_env(store_loc,db_name):
  print("set_data_env() called: setting data environment for location: {} and database name: {}...".format(store_loc,db_name))

  data_file_loc = "{}{}".format(store_loc,db_name) 
  db_loc = "{}{}/{}.db".format(store_loc,db_name,db_name) 

  print("Using...\n    store location = {}\n          Database = {},\n       db location = {}".format(store_loc,db_name,db_loc))

# Function to setup database
def create_db(db_name,db_loc):
  print("create_db called: creating storage location, db_loc={}, and creating db, db_name={}...".format(db_loc,db_name))
  dbutils.fs.mkdirs(db_loc)
  spark.sql(f"""CREATE DATABASE {db_name}
    LOCATION '{db_loc}'
  """)
  display(dbutils.fs.ls(data_file_loc))

def clear_db(db_name,db_loc):
  print("clear_db called: droping db, db_name={db_name}, removing and storage location={db_loc}...".format(db_name,db_loc))
  spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
  dbutils.fs.rm(db_loc, True)
  display(dbutils.fs.ls(data_file_loc))

def reset_db(db_name,db_loc):
  print("reset_db called: clearing storage directory: {db_loc}, and creating database (if not exists) {db_name}...".format(db_loc,db_name))
  dbutils.fs.rm(data_file_loc, False)
  spark.sql(f"""CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_loc}' """)
  spark.sql(f"""USE {db_name}""")

  display(dbutils.fs.ls(data_file_loc))

# Function to setup database
def create_track_table(db_name,track_table_name):
  print("setup_track_table called: dropping existing load tracking table {}.{}".format(db_name,track_table_name))
  spark.sql("""DROP TABLE IF EXISTS {db_name}.{track_table_name}""".format(db_name,track_table_name))
  print("                          re-creating load tracking table {}.{}".format(db_name,track_table_name))
  spark.sql("""CREATE TABLE IF NOT EXISTS {db_name}.{track_table_name} (
                 load_start_dt TIMESTAMP,
                 load_end_dt TIMESTAMP,
                 load_process STRING,
                 load_source STRING,
                 load_target STRING,
                 load_count LONG,
                 load_checksum DOUBLE,
                 load_metrics STRING
               )""".format(db_name,track_table_name)
           )
  print("                          load tracking table created: {db_name}.{track_table_name}".format(db_name, track_table_name))

# Drop Table utility function
def drop_table(db_name,table_name):
  print("drop_table called: dropping table: {}.{}...".format(db_name,table_name))
  spark.sql("DROP TABLE {}.{}".format(db_name,table_name)
           )
  print("Table dropped: {}.{}".format(db_name,table_name))

