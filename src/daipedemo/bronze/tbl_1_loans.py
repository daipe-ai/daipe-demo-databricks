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

import os
import urllib.request
from zipfile import ZipFile
from pyspark.sql import functions as f, SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *
from logging import Logger

# COMMAND ----------

# MAGIC %md
# MAGIC ### Downloading and unpacking data
# MAGIC We are using the **Public Reports** dataset from Bondora, an Estonian peer-to-peer loan company.
# MAGIC For dataset format and explanation visit [here](https://www.bondora.com/en/public-reports#dataset-file-format)
# MAGIC
# MAGIC

# COMMAND ----------


@notebook_function()
def download_data():
    opener = urllib.request.URLopener()
    # Bondora server checks User-Agent and forbids the default User-Agent of urllib
    opener.addheader(
        "User-Agent",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    )

    opener.retrieve("https://www.bondora.com/marketing/media/LoanData.zip", "/loanData.zip")
    opener.retrieve("https://www.bondora.com/marketing/media/RepaymentsData.zip", "/repaymentsData.zip")


# COMMAND ----------


@notebook_function()
def unpack_data():
    with ZipFile("/loanData.zip", "r") as zip_obj:
        zip_obj.extractall("/")
    with ZipFile("/repaymentsData.zip", "r") as zip_obj:
        zip_obj.extractall("/")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Moving data to dbfs

# COMMAND ----------


@notebook_function()
def move_to_dbfs():
    dbutils.fs.cp("file:/LoanData.csv", "dbfs:/")  # noqa: F821
    dbutils.fs.cp("file:/RepaymentsData.csv", "dbfs:/")  # noqa: F821


# COMMAND ----------

# MAGIC %md
# MAGIC ## Standard Datalake layers
# MAGIC
# MAGIC ![Bronze, silver, gold](https://docs.daipe.ai/images/bronze_silver_gold.png)
# MAGIC
# MAGIC For further information read [here](https://docs.daipe.ai/data-pipelines-workflow/managing-datalake/#2-recommended-notebooks-structure)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating empty databases

# COMMAND ----------


@notebook_function()
def init(spark: SparkSession):
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_bronze;")  # noqa: F821
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_silver;")  # noqa: F821
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_gold;")  # noqa: F821


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Reading a CSV file
# MAGIC
# MAGIC Since this is a **bronze** notebook, we are going to be loading the raw data in CSV format into a Delta table.
# MAGIC
# MAGIC Use the `read_csv()` function inside the `@transformation` decorator to load the CSV file into Spark dataframe. Use `display=True` to display the DataFrame.

# COMMAND ----------


@transformation(
    read_csv("/LoanData.csv", options=dict(header=True, inferSchema=True)),
    display=True,
)
def read_csv_and_select_columns(df: DataFrame, logger: Logger):
    logger.info(f"Number of records: {df.count()}")
    return df.select(
        "LoanID",
        "UserName",
        "Amount",
        "Interest",
        "Education",
        "Gender",
        "Rating",
        "DefaultDate",
        "Country",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Writing transformed data into a table
# MAGIC
# MAGIC In Daipe it is recommended to **write data into Hive tables rather than datalake paths**. The following code writes the returned Spark dataframe into the `bronze.tbl_1_loans` table.
# MAGIC
# MAGIC #### Schema or no schema?
# MAGIC
# MAGIC It is very much recommended to use a fixed schema in production environment, though in developement it is possible to use just the table name in our case `bronze.tbl_1_loans`. The input DataFrame schema will be used for table creation. This behavior raises a **warning** in the logs.
# MAGIC
# MAGIC We will look at how to define a schema in the following notebook.

# COMMAND ----------


@transformation(read_csv_and_select_columns, display=True)
@table_overwrite("bronze.tbl_1_loans")
def save(df: DataFrame, logger: Logger):
    logger.info(f"Saving {df.count()} records")
    return df.withColumn("DefaultDate", f.to_date(f.col("DefaultDate"), "yyyy-MM-dd"))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Reading data from a table
# MAGIC
# MAGIC To check that the data is in the table, let's use the `read_table()` function inside the `@transformation` decorator to load the data from our table.

# COMMAND ----------


@transformation(read_table("bronze.tbl_1_loans"), display=True)
def read_table_tbl_loans(df: DataFrame):
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's move on to the second <a href="$./tbl_2_repayments/tbl_2_repayments">notebook</a>
