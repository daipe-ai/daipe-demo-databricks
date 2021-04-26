# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #4: Joining multiple dataframes
# MAGIC

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from datalakebundle.notebook.decorators import transformation, read_table, table_overwrite

# COMMAND ----------


@transformation(read_table("bronze_covid.tbl_template_2_confirmed_cases"), display=True)
def read_bronze_covid_tbl_template_2_confirmed_case(df: DataFrame):
    return df.select("countyFIPS", "County_Name", "State", "stateFIPS").dropDuplicates()


# COMMAND ----------


@transformation(read_table("silver_covid.tbl_template_3_mask_usage"), display=True)
def read_table_silver_covid_tbl_template_3_mask_usage(df: DataFrame):
    return df.limit(10).withColumn("EXECUTE_DATE", f.to_date(f.col("EXECUTE_DATETIME")))  # only for test


# COMMAND ----------

# MAGIC %md #### Joining multiple dataframes in `@transformation`

# COMMAND ----------


@transformation(read_bronze_covid_tbl_template_2_confirmed_case, read_table_silver_covid_tbl_template_3_mask_usage, display=True)
def join_covid_datasets(df1: DataFrame, df2: DataFrame):
    return df1.join(df2, df1.countyFIPS == df2.COUNTYFP, how="right")


# COMMAND ----------


@transformation(join_covid_datasets, display=False)
def agg_avg_mask_usage_per_county(df: DataFrame):
    return df.groupBy("EXECUTE_DATE", "County_Name").agg(
        f.avg("NEVER").alias("AVG_NEVER"),
        f.avg("RARELY").alias("AVG_RARELY"),
        f.avg("SOMETIMES").alias("AVG_SOMETIMES"),
        f.avg("FREQUENTLY").alias("AVG_FREQUENTLY"),
        f.avg("ALWAYS").alias("AVG_ALWAYS"),
    )


# COMMAND ----------


@transformation(agg_avg_mask_usage_per_county, display=True)
@table_overwrite("gold_reporting.tbl_template_4_mask_usage_per_county")
def standardize_dataset(df: DataFrame):
    return df.withColumnRenamed("County_Name", "COUNTY_NAME")
