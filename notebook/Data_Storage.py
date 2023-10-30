# Databricks notebook source
# MAGIC %run "/Repos/akashpandey15071996@outlook.com/end-to-end-supply-chain-project-with-azure-databricks-and-pipeline/notebook/Data_Preparation"

# COMMAND ----------

#create the scope
sqlServer = dbutils.secrets.get(scope="bigdata Scope", key="sqlServer")
sqlDatabase = dbutils.secrets.get(scope="bigdata Scope", key="sqlDatabase")
SQLpassward = dbutils.secrets.get(scope="bigdata Scope", key="SQLpassward")

# COMMAND ----------

display(df1)

# COMMAND ----------

server = sqlServer
database = sqlDatabase
username = 'mailboxakash'
password = SQLpassward
#driver= '{ODBC Driver 17 for SQL Server}'
#connection = pyodbc.connect(f'SERVER={server};DATABASE={database};UID={username};PWD={password};Driver={driver}')


# COMMAND ----------

# Assuming 'df1' is your DataFrame
df1.write \
  .format("jdbc") \
  .option("url", f"jdbc:sqlserver://{server};databaseName={database}") \
  .option("dbtable", "SupplyandChain") \
  .option("user", username) \
  .option("password", password) \
  .mode("overwrite") \
  .save()


# COMMAND ----------

df2.display()

# COMMAND ----------

# Assuming 'df2' is your DataFrame
df2.write \
  .format("jdbc") \
  .option("url", f"jdbc:sqlserver://{server};databaseName={database}") \
  .option("dbtable", "SupplyandChain2") \
  .option("user", username) \
  .option("password", password) \
  .mode("overwrite") \
  .save()
