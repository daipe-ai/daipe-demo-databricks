# Databricks notebook source
# MAGIC %md
# MAGIC # #5 Joining tables
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
# MAGIC ### Advantages of function-based notebooks
# MAGIC  1. Create and publish auto-generated documentation and lineage of notebooks and pipelines (Daipe Enterprise)
# MAGIC  2. Write much cleaner notebooks with properly named code blocks
# MAGIC  3. Test specific notebook functions with ease
# MAGIC  4. Use YAML to configure your notebooks for given environment (dev/test/prod/...)
# MAGIC  5. Utilize pre-configured objects to automate repetitive tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decorators
# MAGIC  - `@notebook_function()`
# MAGIC  - `@transformation()`
# MAGIC    - read_csv()
# MAGIC    - read_table()
# MAGIC  - `@table_{overwrite/append/upsert}`
# MAGIC
# MAGIC  For further information read [here](https://docs.daipe.ai/data-pipelines-workflow/managing-datalake/#4-writing-function-based-notebooks)

# COMMAND ----------

# MAGIC %md #### Joining tables

# COMMAND ----------

# MAGIC %md
# MAGIC Joining two tables is so simple that it takes only **four** lines of code.
# MAGIC
# MAGIC It takes the function names two `read_table` functions as arguments. The resulting DataFrames are the arguments of the `join_loans_and_repayments` function which simply returns the joined DataFrame. This DataFrame is then saved to a table using the `@table_overwrite` decorator according to the following **schema**.

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


@transformation(read_table("silver.tbl_loans"), read_table("silver.tbl_repayments"), display=True)
@table_overwrite("silver.tbl_joined_loans_and_repayments", get_joined_schema())
def join_loans_and_repayments(df1: DataFrame, df2: DataFrame):
    return df1.join(df2, "LoanID")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$./tbl_defaults">sample notebook #6</a>
