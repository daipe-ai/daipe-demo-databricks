# Databricks notebook source
# MAGIC %md
# MAGIC # Creating features with `time_windows`
# MAGIC 
# MAGIC Return to <a href="$../../../_index">index page</a>
# MAGIC 
# MAGIC In this notebook we explore the optimal __time windows__ calculation from Daipe Feature store.

# COMMAND ----------

# MAGIC %run ../../../app/bootstrap

# COMMAND ----------

# MAGIC %run ../loan_feature_decorator_init

# COMMAND ----------

from collections import namedtuple
import numpy as np
import datetime as dt
from pyspark.sql import DataFrame, functions as f

import daipe as dp
from featurestorebundle.windows.windowed_features import windowed, with_time_windows
Args = namedtuple('Args', 'run_date time_windows')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets for parameters

# COMMAND ----------

@dp.notebook_function()
def create_input_widgets(widgets: dp.Widgets):
    widgets.add_text("run_date", dt.date.today().strftime("%Y-%m-%d"))
    widgets.add_text('time_windows', "30d,60d,90d")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading parameters from widgets

# COMMAND ----------

@dp.notebook_function()
def args(widgets: dp.Widgets) -> Args:
    """Get widgets args"""
    
    return (
        Args(
          dt.datetime.strptime(widgets.get_value("run_date"), "%Y-%m-%d"),
          widgets.get_value("time_windows").split(','),
        )
    )

# COMMAND ----------

@dp.transformation(dp.read_table("silver.tbl_joined_loans_and_repayments"), args, display=True)
def joined_loans_and_repayments_with_time_windows(df: DataFrame, args: Args):    
    return (
      with_time_windows(df, "Date", f.lit(args.run_date), args.time_windows)
      .select("LoanId", "Date", "InterestRepayment", *[f"is_time_window_{time_window}" for time_window in args.time_windows])
      .orderBy("Date", ascending=False)
    )

# COMMAND ----------

@dp.transformation(joined_loans_and_repayments_with_time_windows, args, display=True)
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

# MAGIC %md
# MAGIC ### Continue to <a href="$./dynamic_run_date">sample notebook #11</a>

# COMMAND ----------


