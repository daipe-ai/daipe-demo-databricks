# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #6: Widgets

# COMMAND ----------

# MAGIC %md
# MAGIC #### Widgets
# MAGIC Many people love using [Databricks widgets](https://docs.databricks.com/notebooks/widgets.html) to parametrize notebooks. To use widgets in Daipe, you should put them into a `@notebook_function`.
# MAGIC
# MAGIC Don't forget to check  or run command `dbutils.widgets.help()` to see options you have while working with widget.

# COMMAND ----------

# MAGIC %run ../app/install_master_package

# COMMAND ----------

from pyspark.sql import functions as f, types as t
from logging import Logger
from pyspark.dbutils import DBUtils  # enables to use Databricks dbutils within functions
from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *

# COMMAND ----------

# MAGIC %md #### Creating a widget

# COMMAND ----------


@notebook_function()
def create_input_widgets(dbutils: DBUtils):
    dbutils.widgets.dropdown("base_year", "2015", list(map(str, range(2009, 2022))), "Base year")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Filtering records since `Base year`

# COMMAND ----------


@transformation(read_table("silver.tbl_loans"), display=True)
def read_table_bronze_loans_tbl_loans(df: DataFrame, logger: Logger, dbutils: DBUtils):
    base_year = dbutils.widgets.get("base_year")

    logger.info(f"Using base year: {base_year}")

    return df.filter(f.col("DefaultDate") >= base_year)


# COMMAND ----------


@transformation(read_table_bronze_loans_tbl_loans, display=True)
def add_defaulted_column(df: DataFrame):
    return df.withColumn("Defaulted", f.col("DefaultDate").isNotNull()).where(f.col("Defaulted"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema and save table

# COMMAND ----------


table_schema = TableSchema(
    "silver.tbl_defaults",
    [
        t.StructField("LoanID", t.StringType(), True),
        t.StructField("Rating", t.StringType(), True),
        t.StructField("Country", t.StringType(), True),
        t.StructField("Defaulted", t.BooleanType(), False),
        t.StructField("Year", t.IntegerType(), True),
        t.StructField("Month", t.IntegerType(), True),
    ],
    "LoanID",
    partition_by="Year"
)

# COMMAND ----------


@transformation(add_defaulted_column, display=True)
@table_overwrite(table_schema)
def select_columns_and_save(df: DataFrame):
    return df.select("LoanID", "Rating", "Country", "Defaulted", f.year("DefaultDate").alias("Year"), f.month("DefaultDate").alias("Month"))


# COMMAND ----------

# MAGIC %md ### Removing all widgets

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's move on to <a href="$../gold/tbl_7_most_valuable_users">simple reporting</a>
