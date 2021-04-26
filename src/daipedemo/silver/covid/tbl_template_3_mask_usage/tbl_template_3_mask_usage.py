# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #3: Widgets

# COMMAND ----------

# MAGIC %md
# MAGIC #### Widgets
# MAGIC Many people love using [Databricks widgets](https://docs.databricks.com/notebooks/widgets.html) to parametrize notebooks. To use widgets in Daipe, you should put them into a `@notebook_function`.
# MAGIC
# MAGIC <img src="https://github.com/bricksflow/bricksflow/raw/master/docs/widgets.png?raw=true" width=1000/>
# MAGIC
# MAGIC Don't forget to check  or run command `dbutils.widgets.help()` to see options you have while working with widget.

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from pyspark.sql import functions as f
from datetime import datetime
from logging import Logger
from pyspark.dbutils import DBUtils  # enables to use Datbricks dbutils within functions
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import notebook_function, transformation, read_table, table_overwrite

# COMMAND ----------

# MAGIC %md #### Creating a widget

# COMMAND ----------


@notebook_function()
def create_input_widgets(dbutils: DBUtils):
    dbutils.widgets.dropdown("base_year", "2020", ["2018", "2019", "2020", "2021"], "Base year")


# COMMAND ----------


@transformation(read_table("bronze_covid.tbl_template_1_mask_usage"), display=True)
def read_table_bronze_covid_tbl_template_1_mask_usage(df: DataFrame, logger: Logger, dbutils: DBUtils):
    base_year = dbutils.widgets.get("base_year")

    logger.info(f"Using base year: {base_year}")

    return df.filter(f.col("INSERT_TS") >= base_year)


# COMMAND ----------


@transformation(read_table_bronze_covid_tbl_template_1_mask_usage, display=False)
@table_overwrite("silver_covid.tbl_template_3_mask_usage")
def add_execution_datetime(df: DataFrame):
    return df.withColumn("EXECUTE_DATETIME", f.lit(datetime.now()))


# COMMAND ----------

# MAGIC %md ### Removing all widgets

# COMMAND ----------

# dbutils.widgets.removeAll()
