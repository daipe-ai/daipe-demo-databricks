# Databricks notebook source
# MAGIC %run ../../app/bootstrap

# COMMAND ----------

import datetime as dt
from logging import Logger

import datalakebundle.imports as dl
from daipecore.widgets.Widgets import Widgets
from featurestorebundle.databricks.FeatureStoreWriter import FeatureStoreWriter

# COMMAND ----------

@dl.notebook_function()
def set_widgets(widgets: Widgets):
    """Set a widget for picking run_date"""

    widgets.add_text("run_date", dt.date.today().strftime("%Y-%m-%d"))
    widgets.add_text('time_windows', "30d,60d,90d")

# COMMAND ----------


