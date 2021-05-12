# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #4: Schema in external file

# COMMAND ----------

# MAGIC %run ../../app/install_master_package

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *
from daipedemo.silver.tbl_4_repayments.schema import tbl_repayments

# COMMAND ----------

# MAGIC %md Schema can be loaded from an external file in this case `daipedemo.silver.tbl_4_repayments.schema`.
# MAGIC The schema looks like as follows

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC class tbl_repayments:  # noqa: N801
# MAGIC     db = "silver"
# MAGIC     fields = [
# MAGIC         t.StructField("RepaymentID", t.LongType(), True),
# MAGIC         t.StructField("ReportAsOfEOD", t.DateType(), True),
# MAGIC         t.StructField("LoanID", t.StringType(), True),
# MAGIC         t.StructField("Date", t.DateType(), True),
# MAGIC         t.StructField("PrincipalRepayment", t.DoubleType(), True),
# MAGIC         t.StructField("InterestRepayment", t.DoubleType(), True),
# MAGIC         t.StructField("LateFeesRepayment", t.DoubleType(), True),
# MAGIC     ]
# MAGIC     primary_key = "RepaymentID"
# MAGIC     # partition_by = "Date" #---takes a very long time
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC It can be copied out into the code if changes are necessary without access to the local project.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply schema and save table

# COMMAND ----------


@transformation(read_table("bronze.tbl_repayments"), display=True)
@table_upsert(tbl_repayments)
def apply_schema_and_save(df: DataFrame):
    return (
        df.withColumn("ReportAsOfEOD", f.to_date("ReportAsOfEOD"))
        .withColumn("Date", f.to_date("Date"))
        .withColumn("RepaymentID", f.monotonically_increasing_id())
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's move on to the following <a href="$../tbl_5_joined_loans_and_repayments">notebook</a>
