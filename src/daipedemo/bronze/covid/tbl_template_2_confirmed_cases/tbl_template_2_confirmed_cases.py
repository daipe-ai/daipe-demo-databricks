# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Sample notebook #2: Configuration, appending new data
# MAGIC
# MAGIC In this notebook, you will learn how to **use and change configuration parameters**.

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from logging import Logger
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import transformation, notebook_function, read_csv, table_append
from daipedemo.bronze.covid.tbl_template_2_confirmed_cases.csv_schema import get_schema as get_csv_schema

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Passing configuration to notebook function
# MAGIC
# MAGIC The following function uses the `"%covid.confirmed_csv_path%"` config parameter. To change it:
# MAGIC
# MAGIC 1. [Setup your local development environment](https://datasentics.github.io/ai-platform-docs/data-pipelines-workflow/local-project-setup/)
# MAGIC 1. Edit the `src/daipedemo/_config/config.yaml` file on your local machine
# MAGIC 1. Deploy changes back to Databricks by using the `console dbx:deploy` command.

# COMMAND ----------


@transformation(
    read_csv("%covid.confirmed_csv_path%", schema=get_csv_schema(), options=dict(header=True)),
)
@table_append("bronze_covid.tbl_template_2_confirmed_cases")
def rename_columns(df: DataFrame):
    return df.limit(10).withColumnRenamed("County Name", "County_Name")
