# Databricks notebook source
# MAGIC %md
# MAGIC # #4 Schema in external file
# MAGIC Return to <a href="$../../_index">index page</a>

# COMMAND ----------

# MAGIC %run ../../app/install_master_package

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *

# COMMAND ----------

# MAGIC %md
# MAGIC Schema definition **can be also loaded from an external file**. In this case we will load it from the `daipedemo.silver.tbl_4_repayments.schema` python module which contains the following code:
# MAGIC
# MAGIC
# MAGIC ```python
# MAGIC from datalakebundle.table.schema.TableSchema import TableSchema
# MAGIC from pyspark.sql import types as t
# MAGIC
# MAGIC
# MAGIC table_schema = TableSchema(
# MAGIC     "silver.tbl_repayments",
# MAGIC     [
# MAGIC         t.StructField("ReportAsOfEOD", t.DateType(), True),
# MAGIC         t.StructField("LoanID", t.StringType(), True),
# MAGIC         t.StructField("Date", t.DateType(), True),
# MAGIC         t.StructField("PrincipalRepayment", t.DoubleType(), True),
# MAGIC         t.StructField("InterestRepayment", t.DoubleType(), True),
# MAGIC         t.StructField("LateFeesRepayment", t.DoubleType(), True),
# MAGIC     ],
# MAGIC     ["LoanID", "Date"],
# MAGIC     # partition_by = "Date" #---takes a very long time
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table upsert
# MAGIC
# MAGIC It is not necessary to overwrite the table while incorporating new data. `@table_upsert` uses the primary key to either __update__ existing records or __insert__ new ones.

# COMMAND ----------

from daipedemo.silver.tbl_repayments.schema import get_schema


@transformation(read_table("bronze.tbl_repayments"), display=True)
@table_upsert("silver.tbl_repayments", get_schema())
def apply_schema_and_save(df: DataFrame):
    return df.withColumn("ReportAsOfEOD", f.to_date("ReportAsOfEOD")).withColumn("Date", f.to_date("Date"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$../tbl_joined_loans_and_repayments">sample notebook #5</a>
