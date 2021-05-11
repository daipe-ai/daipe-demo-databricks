# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #3: Function based notebooks
# MAGIC
# MAGIC In this example notebook you will how and **why** to write function-based notebooks.

# COMMAND ----------

# MAGIC %run ../app/install_master_package

# COMMAND ----------

from pyspark.sql import functions as f, types as t
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


class tbl_3_joined_loans_and_repayments:  # noqa: N801
    db = "silver"
    fields = [
        t.StructField("LoanID", t.StringType(), True),
        t.StructField("UserName", t.StringType(), True),
        t.StructField("Amount", t.DoubleType(), True),
        t.StructField("Interest", t.DoubleType(), True),
        t.StructField("Education", t.IntegerType(), True),
        t.StructField("Gender", t.IntegerType(), True),
        t.StructField("Rating", t.StringType(), True),
        t.StructField("DefaultDate", t.DateType(), True),
        t.StructField("Country", t.StringType(), True),
        t.StructField("ReportAsOfEOD", t.DateType(), True),
        t.StructField("Date", t.DateType(), True),
        t.StructField("PrincipalRepayment", t.DoubleType(), True),
        t.StructField("InterestRepayment", t.DoubleType(), True),
        t.StructField("LateFeesRepayment", t.DoubleType(), True),
        t.StructField("ID", t.LongType(), False),
    ]
    primary_key = "ID"


# COMMAND ----------


@transformation(read_table("bronze.tbl_1_loans"), read_table("bronze.tbl_2_repayments"), display=True)
@table_overwrite(tbl_3_joined_loans_and_repayments, recreate_table=True)
def join_loans_and_repayments(df1: DataFrame, df2: DataFrame):
    return df1.join(df2, "LOANID").withColumn("ID", f.monotonically_increasing_id())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's continue to the following <a href="$./tbl_4_defaults">notebook</a>
