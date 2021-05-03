# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Sample notebook #2: Configuration and fixed schema
# MAGIC
# MAGIC In this notebook, we will take a look at how to **use and change configuration parameters**.

# COMMAND ----------

# MAGIC %run ../../app/install_master_package

# COMMAND ----------

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as f, types as t
from datalakebundle.imports import *
from daipedemo.bronze.tbl_2_repayments.csv_schema import get_schema as get_csv_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recommended project structure
# MAGIC As you must have noticed from the notebook filenames. We recommend using the `[db_name]/[domain]/[table_name]` directory structure. For table names it translates to `[db_name]_[domain].[table_name]`.
# MAGIC
# MAGIC Each notebook corresponds to one table e.g. this notebook in path `bronze/loans/tbl_2_repayments/tbl_2_repayments.py` manipulates with the table `bronze_loans.tbl_2_repayments`
# MAGIC
# MAGIC For further information read [here](https://docs.daipe.ai/data-pipelines-workflow/managing-datalake/#2-recommended-notebooks-structure)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema
# MAGIC
# MAGIC A schema is defined in a class of the same name which expects parameters:
# MAGIC  - `db` => string
# MAGIC  - `fields` => a list of StructField
# MAGIC  - `primary_key` => string/list of string
# MAGIC  - `partition_by` => string/list of string (optional)
# MAGIC
# MAGIC The class name is then passed as an argument to for example the `table_[overwrite]` function.

# COMMAND ----------


class tbl_2_repayments:  # noqa: N801
    db = "bronze"
    fields = [
        t.StructField("ReportAsOfEOD", t.DateType(), True),
        t.StructField("LoanID", t.StringType(), True),
        t.StructField("Date", t.DateType(), True),
        t.StructField("PrincipalRepayment", t.DoubleType(), True),
        t.StructField("InterestRepayment", t.DoubleType(), True),
        t.StructField("LateFeesRepayment", t.DoubleType(), True),
    ]
    primary_key = "LoanID"
    # partition_by = "Date" #---takes a very long time


# COMMAND ----------

# MAGIC %md
# MAGIC #### Using the schema class as a console argument
# MAGIC `console datalake:table:create daipedemo.bronze.tbl_2_repayments.tbl_2_repayments`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Passing configuration to notebook function (optional)
# MAGIC
# MAGIC The following function uses the `"%loans.confirmed_csv_path%"` config parameter. To change it:
# MAGIC
# MAGIC 1. [Setup your local development environment](https://docs.daipe.ai/data-pipelines-workflow/daipe-demo-project/)
# MAGIC 1. Edit the `src/daipedemo/_config/config.yaml` file on your local machine
# MAGIC 1. Deploy changes back to Databricks by using the `console dbx:deploy` command.
# MAGIC
# MAGIC Reading paths from configuration makes the code **cleaner** than hard-coded paths.

# COMMAND ----------


@transformation(
    read_csv("%loans.repayments_csv_path%", schema=get_csv_schema(), options=dict(header=True)),
)
@table_overwrite(tbl_2_repayments)
def load_csv_modify_columns_and_save(df: DataFrame):
    return df.select(
        f.to_date(f.col("ReportAsOfEOD"), "yyyy-MM-dd").alias("ReportAsOfEOD"),
        "LOANID",
        f.to_date(f.col("Date"), "yyyy-MM-dd").alias("Date"),
        "PrincipalRepayment",
        "InterestRepayment",
        "LateFeesRepayment",
    ).withColumnRenamed("LOANID", "LoanID")


# COMMAND ----------

# MAGIC %md
# MAGIC #### The real table name

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see the real table name is `dev_bronze.tbl_2_repayments` where the prefix `dev` corresponds to the chosen environment `env=dev`. In `prod` environment the `prod_bronze.tbl_2_repayments` will be used.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now that we have our data loaded, let's move to the following <a href="$../../silver/tbl_3_joined_loans_and_repayments"> notebook</a>
