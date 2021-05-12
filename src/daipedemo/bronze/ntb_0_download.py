# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook 0: Download the data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading Daipe framework and all project dependencies

# COMMAND ----------

# MAGIC %run ../app/install_master_package

# COMMAND ----------

import os
import urllib.request
from zipfile import ZipFile
from pyspark.sql import SparkSession
from datalakebundle.imports import *

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


@notebook_function()
def init(spark: SparkSession):
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_bronze;")  # noqa: F821
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_silver;")  # noqa: F821
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_gold;")  # noqa: F821


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's move on to the first actual <a href="$./tbl_1_loans">notebook</a>
