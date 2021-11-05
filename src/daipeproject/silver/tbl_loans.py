# Databricks notebook source
# MAGIC %md
# MAGIC # #3 Applying schema to loans table
# MAGIC ## Silver layer
# MAGIC Return to <a href="$../_index">index page</a>

# COMMAND ----------

# MAGIC %md
# MAGIC In this notebook a schema is applied to the __bronze__ level data - creating a __silver__ level table. We are following the bronze, silver, gold workflow. Now we need to parse the raw data according to our defined schema.
# MAGIC
# MAGIC ### Why use schema?
# MAGIC
# MAGIC Having a well defined schema helps check that the data has the correct format in production. For prototyping it is not necessary although __highly recommended__.

# COMMAND ----------

# MAGIC %run ../app/bootstrap

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema
# MAGIC
# MAGIC An explicit schema should be defined as:
# MAGIC
# MAGIC ```python
# MAGIC table_schema = TableSchema(full_table_identifier: str, fields: List[t.StructField], primary_key: Union[str, list], partition_by: Union[str, list] = None, tbl_properties: dict = None)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying schema and saving table
# MAGIC The `get_schema()` functin should be passed as a second argument into the `@table_overwrite`, `@table_upsert` or `@table_append` decorators.

# COMMAND ----------

from daipeproject.silver.tbl_loans_schema import get_schema


@transformation(read_table("bronze.tbl_loans"), display=True)
@table_overwrite("silver.tbl_loans", get_schema())
def convert_columns_and_save(df: DataFrame):
    date_cols = [c for c in df.columns if "Date" in c and "Till" not in c]
    date_cols.append("ReportAsOfEOD")

    return (
        df.select(*(f.col(c).cast("date").alias(c) if c in date_cols else f.col(c) for c in df.columns))
        .withColumn("ListedOnUTC", f.to_timestamp("ListedOnUTC"))
        .withColumn("BiddingStartedOn", f.to_timestamp("BiddingStartedOn"))
        .withColumn("StageActiveSince", f.to_timestamp("StageActiveSince"))
        .withColumnRenamed("ReportAsOfEOD", "LoanReportAsOfEOD")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$../silver/tbl_repayments/tbl_repayments">sample notebook #4</a>
