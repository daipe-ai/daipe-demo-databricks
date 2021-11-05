# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # #2 Configuration
# MAGIC
# MAGIC ## Bronze layer
# MAGIC Return to <a href="$../../_index">index page</a>
# MAGIC
# MAGIC In this notebook, we will take a look at how to **use and change configuration parameters**. Configuration helps to keep code clean in production environments as well as it keeps unchanging constants ready to be used everywhere. This is __not necessary__ in prototyping workflows.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decorators
# MAGIC Here are the most important decorators
# MAGIC  - `@transformation()` - for manipulating with DataFrames
# MAGIC    - read_csv()
# MAGIC    - read_table()
# MAGIC  - `@notebook_function()`  - for anything other than manipulationg with DataFrames
# MAGIC  - `@table_overwrite`
# MAGIC  - `@table_upsert`
# MAGIC  - `@table_append`
# MAGIC
# MAGIC  For further information read [here](https://docs.daipe.ai/data-pipelines-workflow/managing-datalake/#4-writing-function-based-notebooks)

# COMMAND ----------

# MAGIC %run ../../app/bootstrap

# COMMAND ----------

from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *
from daipedemo.bronze.tbl_repayments.csv_schema import get_schema as get_csv_schema

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Passing configuration to notebook functions (advanced)
# MAGIC
# MAGIC The following function uses the `"%loans.confirmed_csv_path%"` config parameter. To change it:
# MAGIC
# MAGIC 1. [Setup your local development environment](https://docs.daipe.ai/data-pipelines-workflow/daipe-demo-project/)
# MAGIC 1. Edit the `src/daipedemo/_config/config.yaml` file on your local machine
# MAGIC 1. Deploy changes back to Databricks by using the `console dbx:deploy-master-package` command.
# MAGIC
# MAGIC #### Hidden files
# MAGIC
# MAGIC It is also possible to use hidden files for longer reusable parts of code such as schemas. The `daipedemo.bronze.tbl_repayments.csv_schema.py` is used here as an example. Creating these files requires the local developement environment as they are __not visible__ from Databricks.

# COMMAND ----------

@transformation(
    read_csv("%loans.repayments_csv_path%", schema=get_csv_schema(), options=dict(header=True)),
)
@table_overwrite("bronze.tbl_repayments")
def load_csv_and_save(df: DataFrame):
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$../../silver/tbl_loans">sample notebook #3</a>
