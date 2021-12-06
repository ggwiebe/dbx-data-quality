# Databricks notebook source
# MAGIC %md # Databricks Prototype Setup Notebook  
# MAGIC   
# MAGIC Prototype solution for tracking the quality and lineage of data/files ingested into Databricks.  
# MAGIC   
# MAGIC As per documentation in README.md the project is organized with the following characteristics:
# MAGIC - Includes Python library components
# MAGIC - Leverages common config via JSON or Secrets
# MAGIC - Accounts for interactive setup and automated execution
# MAGIC   
# MAGIC These elements in the broader Databricks DevOps landscape is shown in the following diagram:  
# MAGIC   
# MAGIC <!-- Currently the Markdown engine runs from the UI and not from the data plane  
# MAGIC      where these files exist or can be read. For this reason, use the GitHub location
# MAGIC      for the time being - a feature to read MD in UI from Repos is under way!
# MAGIC -->
# MAGIC <!-- ![Databricks Developer & Operations Landsape](/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/images/DevOps.png "Databrisk DevOps")   -->
# MAGIC <!-- ![Databricks Developer & Operations Landsape](images/DevOps.png)   -->
# MAGIC <!-- ![Databricks Developer & Operations Landsape](./images/DevOps.png "Databrisk DevOps")   -->
# MAGIC <!-- <img src="/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/images/DevOps.png" alt="Prototype Setup Notebook" > -->
# MAGIC <!--      style="width: 600px; height: 163px"> -->
# MAGIC ![Databricks Developer & Operations Landsape](https://raw.githubusercontent.com/ggwiebe/dbx-data-quality/main/images/DevOps.png?token=AEXQUADAEJ7G6QDZIUPXKPDBVZ3KE "Databrisk DevOps")

# COMMAND ----------

# MAGIC %md ## Create core database & tracking elements
# MAGIC e.g. database, tracking table, and set common variables, etc.
# MAGIC   
# MAGIC Use widgets to assist in setting these values.  
# MAGIC One can avoid re-creating various elements, so you can run by default without damaging existing.

# COMMAND ----------

# DBTITLE 1,Import common & local libraries
import json
from datetime import datetime

# import local functions
from utils.config_helper import *
config_helper = ConfigHelper(spark,dbutils)

# COMMAND ----------

# MAGIC %md ## Initialize Environment

# COMMAND ----------

ret = config_helper.init_env()

# You can also get a list of the config_helper object attributes via:
# ret = config_helper.list_attrs()

# COMMAND ----------

# MAGIC %md ## Config - via json  
# MAGIC   
# MAGIC Values are read from config/setup.json file and stored in setup_helper object,  
# MAGIC and then pushed into widgets for interactive use. 

# COMMAND ----------

# If using Secrets key/value method comment this section of code out
# ret = config_helper.parse_config("file:/Workspace{}/config/setup.json".format(config_helper.folder))

# COMMAND ----------

# MAGIC %md ## Config - via "secrets" Key/Value store  
# MAGIC   
# MAGIC This approach requires a Secret Scope and three "Secret" values: key="store_loc", key="db_name", key="track_table_name"  
# MAGIC   
# MAGIC Those values are set externally via the Databricks CLI using the following commands:  
# MAGIC ```
# MAGIC databricks secrets create-scope --scope ggw_scope
# MAGIC ```
# MAGIC   
# MAGIC Then to your chosen scope add the application-specific necessary config values, via these three comnands:  
# MAGIC ```
# MAGIC databricks secrets put --scope ggw_scope --key store_loc        --string-value /users/glenn.wiebe@databricks.com/
# MAGIC databricks secrets put --scope ggw_scope --key db_name          --string-value ggw_wine
# MAGIC databricks secrets put --scope ggw_scope --key track_table_name --string-value load_track
# MAGIC ```  

# COMMAND ----------

# use the key/value pairs to configure the UI for interactively configuring the setup notebook
ret = config_helper.set_config(
                        dbutils.secrets.get(scope="ggw_scope", key="store_loc"), 
                        dbutils.secrets.get(scope="ggw_scope", key="db_name"),
                        dbutils.secrets.get(scope="ggw_scope", key="track_table_name"), 
)

# COMMAND ----------

# MAGIC %md ## Create user widgets & run variables

# COMMAND ----------

# DBTITLE 1,Create interaction widgets
# Setup widgets for General Setup Process
dbutils.widgets.dropdown("setup_type", 'New',['New','Clear','Reset'],"Setup DB Options")
dbutils.widgets.dropdown("create_table", "No", ["Yes", "No"], "Create Tracking Table")

# Setup widgets for Setup Config
dbutils.widgets.text("store_loc", config_helper.default_user_path, "User Storage Location")
dbutils.widgets.text("db_name", config_helper.db_name, "Database Name")
dbutils.widgets.text("track_table_name", config_helper.track_table_name, "Track Table Name")

# dbutils.widgets.remove("setup_type")
# dbutils.widgets.remove("create_table")
# dbutils.widgets.remove("store_loc")
# dbutils.widgets.remove("db_name")
# dbutils.widgets.remove("track_table_name")

# COMMAND ----------

# DBTITLE 1,Get runtime variables
# Setup variables separate from current widget
db_name = dbutils.widgets.get("db_name")
track_table_name = dbutils.widgets.get("track_table_name")
store_loc = dbutils.widgets.get("store_loc")
setup_type = dbutils.widgets.get("setup_type")
create_table= dbutils.widgets.get("create_table")

print(" db_name: {}\n track_table_name: {}\n store_loc: {}\n setup_type: {}\n create_table: {}".format(db_name,track_table_name,store_loc,setup_type,create_table))

# COMMAND ----------

# Set data environment from current env plus user supplied values
db_loc = config_helper.set_data_env(store_loc,db_name)
# print("Returned db_loc: {}".format(db_loc))

# COMMAND ----------

# Note current environment set into ConfigHelper object
config_helper.list_attrs()

# Create DB
if setup_type == "New":
  print("Setting up new database {} in storage location {}...".format(config_helper.db_name,config_helper.store_loc))
  #   dbutils.fs.mkdirs(db_loc)
  config_helper.setup_db()
  print("After new DB, set up load tracking table...")
  config_helper.create_track_table(db_name,track_table_name,spark)

elif setup_type == "Reset":
  print("Re-setting up database {} in storage location {}...".format(config_helper.db_name,config_helper.store_loc))
  config_helper.reset_db()
  print("After reset DB, set up load tracking table...")
  config_helper.setup_track_table()

elif setup_type == "Clear":
  print("TODO implement Clear")

  # Create Tracking Table
  if create_table == "Yes":
    print("Setting up load tracking table...")
    config_helper.create_track_table(db_name,track_table_name,spark)

  elif create_table == "No":
    print("Skipped the setting up of the load tracking table.")
  
print("Done!")

# drop_table(use_db,"quality_red_bronze")


print("config setup:")
print("  current_notebook = {}\n      current_user = {},\n default_user_path = {}".format(config_helper.current_notebook,config_helper.current_user,config_helper.default_user_path))
print("            db_loc = {}\n           db_name = {}".format(config_helper.db_loc,config_helper.db_name))
print("  track_table_name = {}\n  current_datetime = {}".format(config_helper.track_table_name,datetime.now()))

# COMMAND ----------

# Exit here successfully!
dbutils.notebook.exit("Completed WineQuality_Setup Notebook!")

# COMMAND ----------

# MAGIC %md ## Appendix Section - test code in appendix after notebook "exit()"

# COMMAND ----------

# Re-defined here for ease of testing
from utils.setup_functions import *
config_helper = ConfigHelper(spark,dbutils)

# COMMAND ----------

my_list = config_helper.db_fs_list("/Users/glenn.wiebe@databricks.com")
display(my_list)

# COMMAND ----------

config_helper.parse_config(f"file:/Workspace/Repos/glenn.wiebe@databricks.com/dbx-data-quality/config/setup.json")

# COMMAND ----------

setup_helper
attrs = vars(setup_helper)
# now dump this in some way or another
print(',\n '.join("%s: %s" % item for item in attrs.items()))
