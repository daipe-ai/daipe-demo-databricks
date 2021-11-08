# Databricks notebook source
# MAGIC %md
# MAGIC # Calculate features and write them to Feature Store

# COMMAND ----------

# MAGIC %run ../../../app/bootstrap

# COMMAND ----------

import datalakebundle.imports as dl
import datetime as dt
from daipecore.widgets.Widgets import Widgets

# COMMAND ----------


@dl.notebook_function()
def set_widgets(widgets: Widgets):
    """Set a widget for picking run_date"""

    widgets.add_text("run_date", dt.date.today().strftime("%Y-%m-%d"))

# COMMAND ----------

# MAGIC %run ./loan_feature_decorator_init

# COMMAND ----------

# MAGIC %run ./loan_features

# COMMAND ----------

from featurestorebundle.databricks.FeatureStoreWriter import FeatureStoreWriter  # noqa E402


@dl.notebook_function()
def write_features(features_writer: FeatureStoreWriter):
    """Write all the features to Feature Store at once"""

    features_writer.write_latest(features_storage)  # noqa: F821
    features_writer.write_historized(features_storage)  # noqa: F821


# COMMAND ----------


