# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #7: Simple aggregation
# MAGIC
# MAGIC In this notebook you will see how to create a simple table of aggregations for reporting using the **Daipe** framework.

# COMMAND ----------

# MAGIC %run ../app/install_master_package

# COMMAND ----------

from pyspark.sql import functions as f, types as t
from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *
from logging import Logger

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load table and perform aggregations

# COMMAND ----------


@transformation(read_table("silver.tbl_joined_loans_and_repayments"), display=True)
def most_valuable_users(df: DataFrame):
    return (
        df.groupBy("UserName")
        .agg(
            f.count("LoanID").alias("Loans"),
            f.sum("InterestRepayment").alias("TotalInterestRepayment"),
            f.sum("LateFeesRepayment").alias("TotalLateFeesRepayment"),
        )
        .orderBy("TotalInterestRepayment", ascending=False)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC #### Save table for future use

# COMMAND ----------


table_schema = TableSchema(
    "gold.tbl_most_valuable_users",
    [
        t.StructField("UserName", t.StringType(), True),
        t.StructField("Loans", t.LongType(), False),
        t.StructField("TotalInterestRepayment", t.DoubleType(), True),
        t.StructField("TotalLateFeesRepayment", t.DoubleType(), True),
    ],
    "UserName"
)


# COMMAND ----------


@transformation(most_valuable_users, display=True)
@table_overwrite(table_schema)
def save(df: DataFrame, logger: Logger):
    logger.info(f"Saving {df.count()} records")
    number_of_mvu = 10
    return df.limit(number_of_mvu)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's continue to the following <a href="$./vw_8_defaults_reporting">notebook</a>