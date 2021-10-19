# Databricks notebook source
# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

import datalakebundle.imports as dl
from pyspark.sql import types as t
from daipecore.widgets.Widgets import Widgets
from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.notebook.decorator.feature import feature

# COMMAND ----------


@dl.notebook_function()
def set_widgets(widgets: Widgets):
    widgets.add_select("storage_type", ["latest", "historized"], "latest")


# COMMAND ----------

entity = Entity(
    name="loans",
    id_column="LoanId",
    id_column_type=t.StringType(),
    time_column="run_date",
    time_column_type=t.DateType(),
)

# COMMAND ----------


@dl.notebook_function()
def storage_type(widgets: Widgets):
    return widgets.get_value("storage_type")


# COMMAND ----------

# first time initialization
if "loan_feature" not in globals():
    features_storage = FeaturesStorage(entity, storage_type=storage_type.result)

    @DecoratedDecorator
    class loan_feature(feature):  # noqa N081
        def __init__(self, *args, category=None):
            super().__init__(*args, entity=entity, category=category, features_storage=features_storage)
