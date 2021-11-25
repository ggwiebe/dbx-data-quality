# Databricks notebook source
# MAGIC %md # Secrets-based Key Value config  
# MAGIC   
# MAGIC This approach to configuring a project uses the Secrets key/value utility to set and get values.

# COMMAND ----------

# MAGIC %md ## Set Config  
# MAGIC   
# MAGIC Secrets are written from outside Databricks using the Databricks CLI.  
# MAGIC The following code should be run from an externall client (e.g. a build machine setting the runtime configuration).  
# MAGIC   
# MAGIC First Create a Scope for organizing your config key/value pairs via this commands:  
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

# MAGIC %md ## Get Config at Runtime  
# MAGIC   
# MAGIC Secrets are read for runtime from within a notebook using the dbutils.secrets.get() method.  
# MAGIC Note, the cell below will not actually show you the value as this is a "secrets"-based infrastructure, so the values will be "[REDACTED]" when written out.

# COMMAND ----------

print(dbutils.secrets.get(scope="ggw_scope", key="store_loc"))
print(dbutils.secrets.get(scope="ggw_scope", key="db_name"))
print(dbutils.secrets.get(scope="ggw_scope", key="track_table_name"))

# COMMAND ----------

# MAGIC %md ### Getting key/values for building widgets is allowed, as below

# COMMAND ----------

# use the key/value pairs to configure the UI for interactively configuring the setup notebook
dbutils.widgets.text("store_loc", dbutils.secrets.get(scope="ggw_scope", key="store_loc"), "User Storage Location")
dbutils.widgets.text("db_name", dbutils.secrets.get(scope="ggw_scope", key="db_name"), "Database Name")
dbutils.widgets.text("track_table_name", dbutils.secrets.get(scope="ggw_scope", key="track_table_name"), "Track Table Name")
