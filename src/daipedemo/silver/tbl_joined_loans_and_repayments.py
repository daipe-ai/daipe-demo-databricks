# Databricks notebook source
# MAGIC %md
# MAGIC # #5 Joining tables
# MAGIC ## Silver level
# MAGIC Return to <a href="$../_index">index page</a>
# MAGIC
# MAGIC This notebook shows how simple it is to join tables and define a schema for the joined table
# MAGIC using the Daipe framework
# MAGIC

# COMMAND ----------

# MAGIC %run ../app/install_master_package

# COMMAND ----------

import pyspark.sql.types as t
from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Joined schema

# COMMAND ----------

from daipedemo.silver.tbl_loans import get_schema as get_loans_schema
from daipedemo.silver.tbl_repayments.schema import get_schema as get_repayments_schema


def get_joined_schema():
    schema = TableSchema(
        get_loans_schema().fields + get_repayments_schema().fields,  # Schema is a composed of columns from both tables
        primary_key=["LoanID", "Date"],
    )

    # "LoanID" column is duplicated therefore it has to be removed once
    schema.fields.remove(t.StructField("LoanID", t.StringType(), True))

    return schema


# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining tables
# MAGIC There are multiple different ways how to join two tables using Daipe. We are going to demonstrate __two__ of them.
# MAGIC #### Option 1) Chaning workflow
# MAGIC The __power__ of Daipe comes from being able to __chain__ decorated functions - creating a pipeline.

# COMMAND ----------


@transformation(read_table("silver.tbl_loans"))
def read_tbl_loans(df: DataFrame):
    return df


# COMMAND ----------


@transformation(read_table("silver.tbl_repayments"))
def read_tbl_repayments(df: DataFrame):
    return df


# COMMAND ----------


@transformation(read_tbl_loans, read_tbl_repayments)
def join_loans_and_repayments(df1: DataFrame, df2: DataFrame):
    return df1.join(df2, "LoanID")


# COMMAND ----------


@transformation(join_loans_and_repayments)
# @table_overwrite("silver.tbl_joined_loans_and_repayments", get_joined_schema())
def save_joined_loans_and_repayments(df: DataFrame):
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining tables
# MAGIC #### Option 2) Combining workflow
# MAGIC
# MAGIC The entire sequence of joining two tables can be written using only **four** lines of code.
# MAGIC
# MAGIC We incorporate the `read_table` functions as inputs, join the DataFrames inside the decorated function and apply `@table_overwrite()` to save the result.

# COMMAND ----------


@transformation(read_table("silver.tbl_loans"), read_table("silver.tbl_repayments"), display=True)
@table_overwrite("silver.tbl_joined_loans_and_repayments", get_joined_schema())
def join_loans_and_repayments_combined(df1: DataFrame, df2: DataFrame):
    return df1.join(df2, "LoanID")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$./tbl_defaults">sample notebook #6</a>
