# Databricks notebook source
# MAGIC %md
# MAGIC # Initialize the project

# COMMAND ----------

# MAGIC %md
# MAGIC Return to <a href="$../_index">index page</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # Welcome to your first Daipe powered notebook!
# MAGIC #### Loading Daipe framework and all project dependencies
# MAGIC The master package installs all Daipe dependencies (__datalake-bundle__,...) and user defined dependencies (__numpy__, __pandas__, __matplotlib__,...)

# COMMAND ----------

# MAGIC %run ../app/install_master_package

# COMMAND ----------

import os
import urllib.request
import datalakebundle.imports as dl
from zipfile import ZipFile
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function based notebooks
# MAGIC The philosophy of the Daipe framework is built upon __decorated function based notebooks__. Each cell in a notebook contains __exactly one__ decorated function. The decorator runs the enclosed function without the need to call it. This allows a __prototype__ code to be used in __production__ with minimal changes necessary.
# MAGIC
# MAGIC ### Advantages of Daipe function-based notebooks
# MAGIC  0. Create and publish __auto-generated documentation__ and __lineage__ of notebooks and pipelines - Big seeling point for a customer
# MAGIC  0. Daipe integrates __best practices__, which greatly simplify project structuring on all levels allowing you to write __simple and clean notebooks__ with ease
# MAGIC  0. Daipe automatically __injects pre-configured objects__ into functions without the need for any boilerplate code
# MAGIC  0. Using functions allows you to write __Unit tests__ with ease, by contrast simply writing code in a cell makes it almost impossible to test
# MAGIC  0. In production environment, use __YAML to configure__ your notebooks for even simpler and less error-prone notebooks, when it really matters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Downloading data
# MAGIC We are using the **Public Reports** dataset from Bondora, an Estonian peer-to-peer loan company.
# MAGIC For dataset format and explanation visit [here](https://www.bondora.com/en/public-reports#dataset-file-format)
# MAGIC
# MAGIC In this use-case we introduce the `@notebook_function()` decorator. This decorator is used when writing functions which do __anything other__ than transforming DataFrames e.g. downloading and unziping data or creating empty databases.
# MAGIC
# MAGIC More about `@notebook_function()` decorator can be found in [technical documentation](https://docs.daipe.ai/data-pipelines-workflow/technical-docs/#notebook_function).

# COMMAND ----------


@dl.notebook_function()
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

# MAGIC %md
# MAGIC ### Unpacking data

# COMMAND ----------


@dl.notebook_function()
def unpack_data():
    with ZipFile("/loanData.zip", "r") as zip_obj:
        zip_obj.extractall("/")
    with ZipFile("/repaymentsData.zip", "r") as zip_obj:
        zip_obj.extractall("/")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Moving data to dbfs
# MAGIC It is necessary to move input data to __dbfs__ as that is where Daipe assumes it will be.

# COMMAND ----------


@dl.notebook_function()
def move_to_dbfs():
    dbutils.fs.cp("file:/LoanData.csv", "dbfs:/")  # noqa: F821
    dbutils.fs.cp("file:/RepaymentsData.csv", "dbfs:/")  # noqa: F821


# COMMAND ----------

# MAGIC %md
# MAGIC ### Environments
# MAGIC Each database is prefixed by an `APP_ENV` variable. This prevents mixing data between `dev`, `test` and `prod` environments. The Daipe framework automatically inserts the prefix based on your selected environment therefore __the code stays the same__ across all environments.

# COMMAND ----------


@dl.notebook_function()
def show_env():
    print(f"We are currently in the {os.environ['APP_ENV']} environment")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating empty databases
# MAGIC Three databases are created to contain the three layers (bronze, silver, gold) used in this project. More about bronze, silver, gold in the following notebook.

# COMMAND ----------


@dl.notebook_function()
def init(spark: SparkSession):
    print(f"create database if not exists {os.environ['APP_ENV']}_bronze;")
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_bronze;")  # noqa: F821
    print(f"create database if not exists {os.environ['APP_ENV']}_silver;")
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_silver;")  # noqa: F821
    print(f"create database if not exists {os.environ['APP_ENV']}_gold;")
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_gold;")  # noqa: F821


# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$./tbl_loans">sample notebook #1</a>
