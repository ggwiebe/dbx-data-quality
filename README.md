# dbx-data-quality
Databricks Data Quality

## Components  
  
This prototype solution is separated into:
- config - a directory for holding a separate & external config (a build manager could replace and inject a config per environment)
- utils - a directory for a set of python libraries that perform useful information  
  
## Initialization & Configuration  
  
This prototype dynamically determines current information like user_name and notebook name to be used for subsequent tracking.  
  
Subsequent to environmental initialization, one can use a persistent config, separate and external to this notebook. Two options are provided:
- JSON config located in config/setup.json
- Secrets key/value pairs (defaulting to using ggw_quality as the secret scope)
  
### Secrets setup  
  
The following commands need to be run to setup a secret scope and then to add the necessary config values to be used to configure this prototype:  
```
databricks secrets create-scope --scope ggw_scope
```
  
Then to your chosen scope add the necessary config values:
```
databricks secrets put --scope ggw_scope --key store_loc        --string-value /users/glenn.wiebe@databricks.com/
databricks secrets put --scope ggw_scope --key db_name          --string-value ggw_wine
databricks secrets put --scope ggw_scope --key track_table_name --string-value load_track
```  
  
To confirm your setup, run this DB CLI secrets command:
```
databricks secrets list --scope ggw_scope
databricks secrets get --scope ggw_scope -key store_loc
databricks secrets get --scope ggw_scope -key db_name
databricks secrets get --scope ggw_scope -key track_table_name
```
  
