# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #5: Pandas

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import transformation, notebook_function, read_table

# COMMAND ----------


@transformation(read_table("bronze_covid.tbl_template_2_confirmed_cases"), display=False)
def read_bronze_covid_tbl_template_2_confirmed_case(df: DataFrame):
    return df.select("countyFIPS", "County_Name", "State", "stateFIPS")


# COMMAND ----------

# MAGIC %md #### Working with Pandas

# COMMAND ----------

from pandas.core.frame import DataFrame as pdDataFrame  # noqa: E402

# COMMAND ----------


@notebook_function(read_bronze_covid_tbl_template_2_confirmed_case)
def spark_df_to_pandas(df: DataFrame) -> pdDataFrame:
    return df.toPandas()


# COMMAND ----------

type(spark_df_to_pandas.result)

# COMMAND ----------


@notebook_function(spark_df_to_pandas)
def pandas_tranformation(pd: pdDataFrame):
    pd2 = pd["County_Name"]
    print(pd2)
    return pd2
