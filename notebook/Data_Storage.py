# Databricks notebook source
# MAGIC %run "/Repos/akashpandey15071996@outlook.com/end-to-end-supply-chain-project-with-azure-databricks-and-pipeline/notebook/Data_Preparation"

# COMMAND ----------

display(dbutils.fs.ls("wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/"))

# COMMAND ----------

# Specify the write mode as "overwrite"
df2.write.mode("overwrite").csv("wasbs://supply-chain-datasets@project99110.blob.core.windows.net/cleaneddatasets/Tokendatasets/")

# COMMAND ----------

# Specify the write mode as "overwrite"
df1.write.mode("overwrite").csv("wasbs://supply-chain-datasets@project99110.blob.core.windows.net/cleaneddatasets/SupplyChainDataset/")
