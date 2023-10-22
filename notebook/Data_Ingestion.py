# Databricks notebook source
# Getting all details from Azure
storage_account_name = 'enter the storage name'
storage_account_access_key = 'Enter the keys'
blob_container = 'enter the container name'

# COMMAND ----------

spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)

# COMMAND ----------

filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/"

# COMMAND ----------

display(dbutils.fs.ls(filePath))
