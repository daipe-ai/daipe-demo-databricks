# Databricks notebook source
# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

import datetime as dt
import datalakebundle.imports as dl
from daipecore.widgets.Widgets import Widgets
from featurestorebundle.databricks.FeatureStoreWriter import FeatureStoreWriter
from featurestorebundle.feature.FeatureStore import FeatureStore

# COMMAND ----------


@dl.notebook_function()
def set_widgets(widgets: Widgets):
    widgets.add_select("storage_type", ["latest", "historized"], "latest")
    widgets.add_text("run_date", dt.date.today().strftime("%Y-%m-%d"))


# COMMAND ----------

# MAGIC %run ./feature_writer_init

# COMMAND ----------

# MAGIC %run ./loan_features

# COMMAND ----------


@dl.notebook_function()
def write_features(features_writer: FeatureStoreWriter, widgets: Widgets):
    storage_type = widgets.get_value("storage_type")

    if storage_type == "latest":
        features_writer.write_latest(features_storage)

    if storage_type == "historized":
        features_writer.write_historized(features_storage)
