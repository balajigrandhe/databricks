# Databricks notebook source
dbutils.help()
dbutils.fs.help()
dbutils.secrets.help()

# COMMAND ----------

application_id = dbutils.secrets.listScopes()
display(application_id)

# COMMAND ----------

application = dbutils.secrets.get(scope='databricks-secret-9101', key='application-id')
tenant = dbutils.secrets.get(scope='databricks-secret-9101', key='tenant-id')
secret = dbutils.secrets.get(scope='databricks-secret-9101', key='secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://employee@datalake9101.dfs.core.windows.net/",
  mount_point = "/mnt/employee",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

path_data = '/mnt/employee/bronze/'
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
employees_schema = StructType([
    StructField("EMPLOYEE_ID", IntegerType(), False),
    StructField("FIRST_NAME", StringType(), False),
    StructField("LAST_NAME", StringType(), False),
    StructField("EMAIL", StringType(), False),
    StructField("PHONE_NUMBER", StringType(), False),
    StructField("HIRE_DATE", StringType(), False),
    StructField("JOB_ID", StringType(), False),
    StructField("SALARY", IntegerType(), False),
    StructField("MANAGER_ID", IntegerType(), True),
    StructField("DEPARTMENT_ID", IntegerType(), False),
])

# COMMAND ----------

employees = spark.read.csv(path=path_data+'employees.csv', header=True, schema=employees_schema)

# COMMAND ----------

display(employees)

# COMMAND ----------

employees = employees.drop("EMAIL", "PHONE_NUMBER")

# COMMAND ----------

from pyspark.sql.functions import to_date
employees = employees.select(
    "EMPLOYEE_ID",
    "FIRST_NAME",
    "LAST_NAME",
    to_date(employees["HIRE_DATE"], "MM/dd/yyyy").alias("HIRE_DATE"),
    "JOB_ID",
    "SALARY",
    "MANAGER_ID",
    "DEPARTMENT_ID"
)

# COMMAND ----------

display(employees)

# COMMAND ----------

employees.write.parquet('/mnt/employee/silver/employees', mode='overwrite')

# COMMAND ----------

departments_schema = StructType([
    StructField("DEPARTMENT_ID", IntegerType(), False),
    StructField("DEPARTMENT_NAME", StringType(), False),
    StructField("MANAGER_ID", IntegerType(), True),
    StructField("LOCATION_ID", IntegerType(), False),
])
countries_schema = StructType([
    StructField("COUNTRY_ID", IntegerType(), False),
    StructField("COUNTY_NAME", StringType(), False),
])

# COMMAND ----------

departments = spark.read.csv(path=path_data+'departments.csv', header=True, schema=departments_schema)

# COMMAND ----------

departments = departments.drop("MANAGER_ID", "LOCATION_ID")

# COMMAND ----------

departments.write.parquet('/mnt/employee/silver/departments', mode='overwrite')

# COMMAND ----------

countries = spark.read.csv(path=path_data+'countries.csv', header=True, schema=countries_schema)

# COMMAND ----------

countries.write.parquet('/mnt/employee/silver/countires', mode='overwrite')

# COMMAND ----------

display(employees)

# COMMAND ----------

from pyspark.sql.functions import concat_ws
employees = employees.withColumn("FULL_NAME", concat_ws(' ', employees["FIRST_NAME"], employees["LAST_NAME"]))

# COMMAND ----------

display(employees)

# COMMAND ----------

employees = employees.drop("FIRST_NAME", "LAST_NAME", "MANAGER_ID").select("EMPLOYEE_ID", "FULL_NAME", "HIRE_DATE", "JOB_ID","SALARY","DEPARTMENT_ID")

# COMMAND ----------

departments = spark.read.parquet('/mnt/employee/silver/departments')

# COMMAND ----------

employees = employees.join(departments, employees.DEPARTMENT_ID==departments.DEPARTMENT_ID, 'left').select(employees["EMPLOYEE_ID"],employees["FULL_NAME"],employees["HIRE_DATE"],employees["JOB_ID"],employees["SALARY"],departments["DEPARTMENT_NAME"])

# COMMAND ----------

employees.display()

# COMMAND ----------

employees.write.parquet('/mnt/employee/gold/employees', mode='overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists employees

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table employees.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists employees.employees
# MAGIC (
# MAGIC EMPLOYEE_ID int,
# MAGIC FULL_NAME string,
# MAGIC HIRE_DATE date,
# MAGIC JOB_ID string,
# MAGIC SALARY int,
# MAGIC DEPARTMENT_NAME string
# MAGIC )
# MAGIC using parquet
# MAGIC location '/mnt/employee/gold/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended employees.employees

# COMMAND ----------

