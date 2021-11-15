# Databricks notebook source
# MAGIC %run ../../../app/bootstrap

# COMMAND ----------

# MAGIC %run ../loan_feature_decorator_init

# COMMAND ----------

from collections import namedtuple
import numpy as np
import datetime as dt
from pyspark.sql import DataFrame, functions as f

import datalakebundle.imports as dl
from daipecore.imports import Widgets
from featurestorebundle.windows.windowed_features import windowed, with_time_windows
Args = namedtuple('Args', 'run_date time_windows')

# COMMAND ----------

@dl.notebook_function()
def create_input_widgets(widgets: Widgets):
    widgets.add_text("run_date", dt.date.today().strftime("%Y-%m-%d"))
    widgets.add_multiselect('time_windows', ["30d", "60d", "90d"], "90d")

# COMMAND ----------

@dl.notebook_function()
def args(widgets: Widgets) -> Args:
    """Get widgets args"""
    
    return (
        Args(
          dt.datetime.strptime(widgets.get_value("run_date"), "%Y-%m-%d"),
          widgets.get_value("time_windows"),
        )
    )

# COMMAND ----------

@dl.transformation(read_table("silver.tbl_joined_loans_and_repayments"), args, display=True)
def joined_loans_and_repayments_with_time_windows(df: DataFrame, args: Args):
  
    df = df.withColumn("Timestamp", f.to_timestamp("Date"))
    
    return with_time_windows(df, "Timestamp", f.lit(args.run_date), args.time_windows)

# COMMAND ----------

@dl.transformation(joined_loans_and_repayments_with_time_windows, args, display=True)
@loan_feature(
    ("interest_repayment_{agg_fun}_{time_window}", "{agg_fun} of interest repayment in a {time_window} period"),
    category="personal",
)
def new_features(df: DataFrame, args: Args):
  """Get all time windowed columns"""
  agg_cols = []
  for time_window in args.time_windows:
      agg_cols.extend([
        f.sum(
          windowed(f.col("InterestRepayment"), time_window)
        ).alias(f'interest_repayment_sum_{time_window}')
      ])
  
  """Aggregate all columns"""
  grouped_df = (
    df.groupby("LoanId")
           .agg(
             *agg_cols,
           )
  )
  
  """Return df with run_date"""
  return (
    grouped_df.withColumn('run_date', f.lit(args.run_date))
  )

# COMMAND ----------


