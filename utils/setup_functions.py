### MAGIC %run ../config/setup

# import common librarys
import json
from datetime import datetime

class TrackerSetupHelper:

  def __init__(self, dbutils):
    self.dbutils = dbutils

  # Test dbutils passing
  def db_utilstuff(self,loc):
    print("db_utilstuff called: testing a dbutils.fs.ls({}) function call...".format(loc))
    self.dbutils.fs.ls(loc)
    print("db_utilstuff done!")

  def my_fs_test(self,dbutils):
    print("my_fs_test called: testing a dbutils.fs.ls({}) function call...".format("/Users/glenn.wiebe@databricks.com"))
    ret = dbutils.fs.ls("/Users/glenn.wiebe@databricks.com")
    return ret
  
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

  print("Using...\n    store location = {}\n          b_name = {},\n       db location = {}".format(store_loc,db_name,db_loc))
  return db_loc

# Function to setup database
def create_db(db_loc,db_name,spark):
  print("create_db called: creating storage location, db_loc={}, and creating db, db_name={}...".format(db_loc,db_name))
  # dbutils.fs.mkdirs(db_loc)
  spark.sql(f"""CREATE DATABASE {db_name}
    LOCATION '{db_loc}'
  """)
  #display(dbutils.fs.ls(data_file_loc))

def clear_db(db_name,db_loc):
  print("clear_db called: droping db, db_name={}, removing and storage location={}...".format(db_name,db_loc))
  spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
  dbutils.fs.rm(db_loc, True)
  display(dbutils.fs.ls(data_file_loc))

def reset_db(db_name,db_loc):
  print("reset_db called: clearing storage directory: {}, and creating database (if not exists) {}...".format(db_loc,db_name))
  dbutils.fs.rm(data_file_loc, False)
  spark.sql(f"""CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_loc}' """)
  spark.sql(f"""USE {db_name}""")

  display(dbutils.fs.ls(data_file_loc))

# Function to setup database
def create_track_table(db_name,track_table_name,spark):
  print("setup_track_table called: dropping existing load tracking table {}.{}".format(db_name,track_table_name))
  spark.sql("""DROP TABLE IF EXISTS {}.{}""".format(db_name,track_table_name))
  print("                          re-creating load tracking table {}.{}".format(db_name,track_table_name))
  spark.sql("""CREATE TABLE IF NOT EXISTS {}.{} (
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
  print("                          load tracking table created: {}.{}".format(db_name, track_table_name))

# Drop Table utility function
def drop_table(db_name,table_name):
  print("drop_table called: dropping table: {}.{}...".format(db_name,table_name))
  spark.sql("DROP TABLE {}.{}".format(db_name,table_name)
           )
  print("Table dropped: {}.{}".format(db_name,table_name))

