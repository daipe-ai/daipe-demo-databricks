# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #1: Create new table from CSV

# COMMAND ----------

# MAGIC %md ## Running the first Daipe-powered notebook

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading Daipe framework and all project dependencies

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

import os
from pyspark.sql import functions as f, SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import notebook_function, transformation, read_csv, table_overwrite

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating empty databases

# COMMAND ----------


@notebook_function()
def init(spark: SparkSession):
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_bronze_covid;")  # noqa: F821
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_silver_covid;")  # noqa: F821
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_gold_reporting;")  # noqa: F821


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Reading a CSV file
# MAGIC
# MAGIC Use the `read_csv()` function inside the `@transformation` decorator to load the CSV file into Spark dataframe.
# MAGIC
# MAGIC To display the loaded dataframe, set the `display=True` keyword argument of the `@transformation` decorator.

# COMMAND ----------


@transformation(
    read_csv("dbfs:/databricks-datasets/COVID/covid-19-data/mask-use/mask-use-by-county.csv", options=dict(header=True, inferSchema=True)),
    display=False,
)
def add_something(df: DataFrame):
    return df.limit(10).withColumn("INSERT_TS", f.current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Writing transformed data into a table
# MAGIC
# MAGIC In Daipe it is recommended to **write data into Hive tables rather than datalake paths**. The following code writes the returned Spark dataframe into the _bronze_covid.tbl_template_1_mask_usage_ table with [explicit schema defined](https://github.com/daipe-ai/daipe-demo-databricks/blob/master/src/daipedemo/bronze/covid/tbl_template_1_mask_usage/schema.py). Keep in mind that any new rows inserted into the table are validated against the schema. For more details visit the ["Managing datalake" documentation section](https://docs.daipe.ai/data-pipelines-workflow/managing-datalake/).

# COMMAND ----------


@transformation(add_something)
@table_overwrite("bronze_covid.tbl_template_1_mask_usage")
def add_current_timestamp_and_save(df: DataFrame):
    return df
