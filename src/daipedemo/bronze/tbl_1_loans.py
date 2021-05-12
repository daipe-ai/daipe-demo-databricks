# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #1: Create a new table from CSV

# COMMAND ----------

# MAGIC %md ## Welcome to your first Daipe-powered notebook!
# MAGIC In this notebook you will learn how to:
# MAGIC  - Load the Daipe framework
# MAGIC  - How to structure your data and notebooks
# MAGIC  - And how load CSVs into Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading Daipe framework and all project dependencies

# COMMAND ----------

# MAGIC %run ../app/install_master_package

# COMMAND ----------

from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *
from logging import Logger

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Reading a CSV file
# MAGIC
# MAGIC Since this is a **bronze** notebook, we are going to be loading the raw data in CSV format into a Delta table.
# MAGIC
# MAGIC Use the `read_csv()` function inside the `@transformation` decorator to load the CSV file into Spark dataframe. Use `display=True` to display the DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Writing transformed data into a table
# MAGIC
# MAGIC In Daipe it is recommended to **write data into Hive tables rather than datalake paths**. The following code writes the returned Spark dataframe into the `bronze.tbl_1_loans` table.
# MAGIC
# MAGIC #### Schema or no schema?
# MAGIC
# MAGIC It is very much recommended to use a fixed schema in production environment, though in developement it is possible to use just the table name in our case `bronze.tbl_1_loans`. The input DataFrame schema will be used for table creation. This behavior raises a **warning** in the logs and suggests a schema to be copied into code.
# MAGIC
# MAGIC We will look at how to define a schema in the following notebook.

# COMMAND ----------


@transformation(read_csv("/LoanData.csv", options=dict(header=True, inferSchema=True)), display=True)
@table_overwrite("bronze.tbl_loans")
def save(df: DataFrame, logger: Logger):
    logger.info(f"Saving {df.count()} records")
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Reading data from a table
# MAGIC
# MAGIC To check that the data is in the table, let's use the `read_table()` function inside the `@transformation` decorator to load the data from our table.

# COMMAND ----------


@transformation(read_table("bronze.tbl_loans"), display=True)
def read_table_tbl_loans(df: DataFrame):
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's move on to the second <a href="$./tbl_2_repayments/tbl_2_repayments">notebook</a>
