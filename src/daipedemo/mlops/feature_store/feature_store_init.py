# Databricks notebook source
# MAGIC %run ../../app/bootstrap

# COMMAND ----------

import datetime as dt
from logging import Logger

import daipe as dp
from featurestorebundle.databricks.DatabricksFeatureStoreWriter import DatabricksFeatureStoreWriter

# COMMAND ----------

@dp.notebook_function()
def set_widgets(widgets: dp.Widgets):
    """Set a widget for picking run_date"""

    widgets.add_text("run_date", dt.date.today().strftime("%Y-%m-%d"))
    widgets.add_text('time_windows', "30d,60d,90d")

# COMMAND ----------


