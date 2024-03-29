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
# MAGIC  - `@dp.transformation()` - for manipulating with DataFrames
# MAGIC    - dp.read_csv()
# MAGIC    - dp.read_table()
# MAGIC  - `@dp.notebook_function()`  - for anything other than manipulationg with DataFrames
# MAGIC  - `@dp.table_overwrite`
# MAGIC  - `@dp.table_upsert`
# MAGIC  - `@dp.table_append`
# MAGIC
# MAGIC  For further information read [here](https://docs.daipe.ai/data-pipelines-workflow/managing-datalake/#4-writing-function-based-notebooks)

# COMMAND ----------

# MAGIC %run ../../app/bootstrap

# COMMAND ----------

from pyspark.sql.dataframe import DataFrame
import daipe as dp
from daipedemo.bronze.tbl_repayments.csv_schema import get_schema as get_csv_schema

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Passing configuration to notebook functions
# MAGIC
# MAGIC The following function uses the `"%loans.repayments_csv_path%"` config parameter. To change it:
# MAGIC
# MAGIC 1. Edit the `[PROJECT_ROOT]/src/daipedemo/_config/config.yaml`
# MAGIC 1. (Re)run the following notebook cell

# COMMAND ----------

@dp.transformation(
    dp.read_csv("%loans.repayments_csv_path%", schema=get_csv_schema(), options=dict(header=True)),
)
@dp.table_overwrite("bronze.tbl_repayments")
def load_csv_and_save(df: DataFrame):
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$../../silver/tbl_loans">sample notebook #3</a>
