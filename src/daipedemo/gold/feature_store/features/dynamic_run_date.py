# Databricks notebook source
# MAGIC %md
# MAGIC # Creating features with dynamic `run_date`
# MAGIC 
# MAGIC Return to <a href="$../../../_index">index page</a>
# MAGIC 
# MAGIC In this notebook we explore how to write features which work with both `static` a `dynamic` `run_date` columns.

# COMMAND ----------

# MAGIC %run ../loan_feature_decorator_init

# COMMAND ----------

from collections import namedtuple
from logging import Logger
import numpy as np
import datetime as dt
from pyspark.sql import DataFrame, Column, functions as f

import daipe as dp
from featurestorebundle.windows.windowed_features import windowed, with_time_windows
Args = namedtuple('Args', 'run_date time_windows target_date_column')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare widgets

# COMMAND ----------

@dp.notebook_function()
def create_input_widgets(widgets: dp.Widgets):
    widgets.add_text("run_date", dt.date.today().strftime("%Y-%m-%d"))
    widgets.add_text('time_windows', "30d,60d,90d")
    widgets.add_select('target_date_column', ["run_date", "MaturityDate_Original_Minus_5y"], "run_date")

# COMMAND ----------

def date(datestr: str):
  return dt.datetime.strptime(datestr, '%Y-%m-%d')

# COMMAND ----------

@dp.notebook_function()
def args(widgets: dp.Widgets) -> Args:
    """Get widgets args"""
    
    return (
        Args(
          date(widgets.get_value("run_date")),
          widgets.get_value("time_windows").split(','),
          widgets.get_value("target_date_column"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data
# MAGIC 
# MAGIC Prepare column `MaturityDate_Original_Minus_5y` for use as the dynamic `run_date`

# COMMAND ----------

@dp.transformation(dp.read_table("silver.tbl_joined_loans_and_repayments"), args, display=True)
def joined_loans_and_repayments(df: DataFrame, args: Args):
    df = df.withColumn("MaturityDate_Original_Minus_5y", f.date_sub(f.col("MaturityDate_Original"), 365*5))
    diff = f.datediff(f.current_date(), "MaturityDate_Original_Minus_5y")
    
    return (
      df
      .select("LoanId", "Date", "MaturityDate_Original_Minus_5y")
      .filter((-10 < diff) & (diff < 100))
      .orderBy("MaturityDate_Original_Minus_5y", ascending=False)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Value of widget `target_date_column` is either `run_date` for static or a column name from DataFrame for dynamic

# COMMAND ----------

@db.notebook_function(joined_loans_and_repayments, args)
def get_target_date_column(df: DataFrame, args: Args, logger: Logger):
    if args.target_date_column == "run_date":
      logger.info(f"Target date column set to a static run_date = {args.run_date}")
      return f.to_timestamp(f.lit(args.run_date))
    
    if args.target_date_column in df.columns:
      logger.info(f"Target date set to dataframe column '{args.target_date_column}'")
      return f.to_timestamp(f.col(args.target_date_column))
    
    raise Exception(f"run_date = {args.run_date} is neither a date nor a column name.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare time windows and `run_date` column

# COMMAND ----------

@dp.transformation(joined_loans_and_repayments, get_target_date_column, args, display=True)
def joined_loans_and_repayments_with_time_windows(df: DataFrame, target_date_column: Column, args: Args):
    df_with_time_windows = with_time_windows(df, "Date", target_date_column, args.time_windows)
    
    return (
      df_with_time_windows.withColumn("run_date", target_date_column)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate features

# COMMAND ----------

@dp.transformation(joined_loans_and_repayments_with_time_windows, args, display=True)
@loan_feature(
  ('number_of_repayments_in_last_{time_window}', 'Number of repayments made in the last {time_window}.'),
    category = 'test',
)
def repayments_before_date(df: DataFrame, args: Args):
  columns_for_agg = []
  for time_window in args.time_windows:
      columns_for_agg.extend([
        f.sum(
          windowed(f.lit(1), time_window)
        ).alias(f"number_of_repayments_in_last_" + f"{time_window}",),
      ])
  
  grouped_df = (
    df.groupby(["LoanId"])
           .agg(
             *columns_for_agg,
             f.first(args.target_date_column).alias("run_date"),
           )
  )
  
  return (
    grouped_df
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to <a href="$../../models/model_training_with_daipe_ml">sample notebook #12</a>

# COMMAND ----------


