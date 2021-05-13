# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #5: Joining tables
# MAGIC
# MAGIC In this example notebook you will how and **why** to write function-based notebooks.

# COMMAND ----------

# MAGIC %run ../app/install_master_package

# COMMAND ----------

import pyspark.sql.types as t
from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *
from daipedemo.silver.tbl_3_loans import table_schema as tbl_loans_schema
from daipedemo.silver.tbl_4_repayments.tbl_4_repayments import table_schema as tbl_repayments_schema

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


table_schema = TableSchema(
    "silver.tbl_joined_loans_and_repayments",
    tbl_loans_schema.fields + tbl_repayments_schema.fields, # Schema is a union of columns of both tables
    "RepaymentID"
)

# "LoanID" column is duplicated therefore it has to be removed once
table_schema.fields.remove(t.StructField("LoanID", t.StringType(), True))

# COMMAND ----------


@transformation(read_table("silver.tbl_loans"), read_table("silver.tbl_repayments"), display=True)
@table_overwrite(table_schema)
def join_loans_and_repayments(df1: DataFrame, df2: DataFrame):
    return df1.join(df2, "LoanID")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's continue to the following <a href="$./tbl_6_defaults">notebook</a>
