# import common librarys
import json
from datetime import datetime

class ConfigHelper:

  def __init__(self, spark, dbutils):
    self.spark = spark
    self.dbutils = dbutils
    self.init_time = datetime.now()

  # Test dbutils passing
  def db_fs_list(self,loc):
    print("db_fs_list called: testing a dbutils.fs.ls({}) function call...".format(loc))
    ret = self.dbutils.fs.ls(loc)
    return ret

  # Initialize the environment
  def init_env(self):
    from pathlib import Path
    # Use dbutils to extract environment values
    self.current_user = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
    tags = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
    notebook_json = json.loads(self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

    self.default_user_path = "/Users/{}/".format(self.current_user)
    self.current_notebook = notebook_json['extraContext']['notebook_path']
    npath = Path(self.current_notebook)
    self.folder = npath.parent
    print(" init_env called:\n     Current user: {}\nDefault User Path: {}\n    Notebook name: {}\n  Notebook folder: {}".format(self.current_user,self.default_user_path,self.current_notebook,self.folder))
    
    return vars(self)

  # List object attributes
  def list_attrs(self):
    print("list_attrs called:")
    # get all attributes of this object
    attrs = vars(self)
    print(" ", ",\n  ".join("%s: %s" % item for item in attrs.items()))

    return attrs

  # function to process json-based config
  def parse_config(self, JSonPath):
    jsonFile = self.spark.read.text(JSonPath, wholetext=True)
    # print("store_loc : " + json.loads(jsonFile.first()[0])["store_loc"])
    self.store_loc = json.loads(jsonFile.first()[0])["store_loc"]
    # print("db_name : " + json.loads(jsonFile.first()[0])["db_name"])
    self.db_name = json.loads(jsonFile.first()[0])["db_name"]
    # print("track_table_name : " + json.loads(jsonFile.first()[0])["track_table_name"])
    self.track_table_name = json.loads(jsonFile.first()[0])["track_table_name"]
    
    # get all attributes of this object
    attrs = vars(self)
    # now dump this in some way or another
    print("parse_config called:")
    print(" ", ",\n  ".join("%s: %s" % item for item in attrs.items()))
    
    return attrs

  # function to process json-based config
  def set_config(self, store_loc, db_name, track_table_name):
    print("set_config called: (useful when getting config from Secrets)...")
    self.store_loc = store_loc
    self.db_name = db_name
    self.track_table_name = track_table_name
    
    # get all attributes of this object
    attrs = vars(self)
    # now dump this in some way or another
    print(" ", ",\n  ".join("%s: %s" % item for item in attrs.items()))
    
    return attrs

  # Function to setup component name variable
  def set_data_env(self,store_loc,db_name):
    print("set_data_env() called...")

    self.store_loc = store_loc
    self.db_name = db_name
    self.data_file_loc = "{}{}".format(store_loc,db_name) 
    self.db_loc = "{}{}/{}.db".format(store_loc,db_name,db_name) 

    print("     store_loc = {}\n       db_name = {},\n data_file_loc = {},\n        db loc = {}".format(store_loc,db_name,self.data_file_loc,self.db_loc))
    return self.list_attrs()

  # Initialize the environment
  def set_start_now(self):
    self.start_timestamp = datetime.now()
    return self.start_timestamp

  # Function to setup database
  def setup_db(self):
    print("setup_db called (no parms)...")
    self.dbutils.fs.mkdirs(self.data_file_loc)
    self.dbutils.fs.mkdirs(self.db_loc)
    print("  data file location {} created; db location created {}.".format(self.data_file_loc,self.db_loc))
    self.spark.sql(f"""CREATE DATABASE {self.db_name}
      LOCATION '{self.db_loc}'
    """)
    print("  database {} created using LOCATION '{}'".format(self.db_loc))

  # Function to setup database
  def create_db(self):
    print("create_db called (no parms)...")
    # dbutils.fs.mkdirs(db_loc)
    self.spark.sql(f"""CREATE DATABASE {self.db_name}
      LOCATION '{self.db_loc}'
    """)
    print("  database {} created using LOCATION '{}'".format(self.db_loc))

  # Function to setup database
  def create_db(db_loc,db_name):
    print("create_db called (storage db_loc={}, db_name={}...".format(db_loc,db_name))
    # dbutils.fs.mkdirs(db_loc)
    self.spark.sql(f"""CREATE DATABASE {db_name}
      LOCATION '{db_loc}'
    """)
    print("  database {} created using LOCATION '{}'".format(db_name,db_loc))

  def clear_db(self):
    print("clear_db called (no parms)...")
    self.spark.sql(f"DROP DATABASE IF EXISTS {self.db_name} CASCADE")
    self.dbutils.fs.rm(self.db_loc, True)
    display(self.dbutils.fs.ls(self.data_file_loc))
    print("  dropping db, db_name={}, removing and storage location={}...".format(self.db_name,self.db_loc))

  def clear_db(self,db_name,db_loc):
    print("clear_db called (db_name={}, db_loc={})...".format(db_name,db_loc))
    self.spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    self.dbutils.fs.rm(db_loc, True)
    print("  dropping db, db_name={}, removing storage location, db_loc={}...".format(db_name,db_loc))

  # Function to setup database
  def reset_db(self):
    print("reset_db called (no parms)...")
    self.dbutils.fs.rm(self.db_loc, True)
    print("  removing data file location, db_file_loc: {}...".format(self.data_file_loc))
    self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.db_name} LOCATION '{self.db_loc}' """)
    self.spark.sql(f"""USE {self.db_name}""")
    print("  creating db {} IF NOT EXISTS, at LOCATION, db_loc: {}...".format(self.db_name,self.db_loc))

#  def reset_db(self,data_file_loc,db_name,db_loc):
#    print("reset_db called: clearing storage directory: {}, and creating database (if not exists) {}...".format(db_loc,db_name))
#    self.dbutils.fs.rm(data_file_loc, False)
#    self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_loc}' """)
#    self.spark.sql(f"""USE {db_name}""")
#    print("  creating db {} IF NOT EXISTS, at LOCATION, db_loc: {}...".format(db_name,db_loc))


  # Function to setup database
  def setup_track_table(self):
    print("setup_track_table called (no parms): dropping existing load tracking table {}.{}".format(self.db_name,self.track_table_name))
    self.spark.sql("""DROP TABLE IF EXISTS {}.{}""".format(self.db_name,self.track_table_name))
    print("                          re-creating load tracking table {}.{}".format(self.db_name,self.track_table_name))
    self.spark.sql("""CREATE TABLE IF NOT EXISTS {}.{} (
                   load_start_dt TIMESTAMP,
                   load_end_dt TIMESTAMP,
                   load_process STRING,
                   load_source STRING,
                   load_target STRING,
                   load_count LONG,
                   load_checksum DOUBLE,
                   load_metrics STRING
                 )""".format(self.db_name,self.track_table_name)
             )
    print("                          load tracking table created: {}.{}".format(self.db_name,self.track_table_name))

#  def create_track_table(self,db_name,track_table_name):
#    print("setup_track_table called: dropping existing load tracking table {}.{}".format(db_name,track_table_name))
#    self.spark.sql("""DROP TABLE IF EXISTS {}.{}""".format(db_name,track_table_name))
#    print("                          re-creating load tracking table {}.{}".format(db_name,track_table_name))
#    self.spark.sql("""CREATE TABLE IF NOT EXISTS {}.{} (
#                   load_start_dt TIMESTAMP,
#                   load_end_dt TIMESTAMP,
#                   load_process STRING,
#                   load_source STRING,
#                   load_target STRING,
#                   load_count LONG,
#                   load_checksum DOUBLE,
#                   load_metrics STRING
#                 )""".format(db_name,track_table_name)
#             )
#    print("                          load tracking table created: {}.{}".format(db_name, track_table_name))

  # Drop Table utility function
  def drop_table(self,db_name,table_name):
    print("drop_table called: dropping table: {}.{}...".format(db_name,table_name))
    self.spark.sql("DROP TABLE {}.{}".format(db_name,table_name)
             )
    print("Table dropped: {}.{}".format(db_name,table_name))

