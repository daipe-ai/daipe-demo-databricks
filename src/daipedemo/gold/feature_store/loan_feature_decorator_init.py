# Databricks notebook source
# MAGIC %run ../../app/bootstrap

# COMMAND ----------

from pyspark.sql import types as t
from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.notebook.decorator.feature import feature

# COMMAND ----------

entity = Entity(
    name="loans",
    id_column="LoanId",
    id_column_type=t.StringType(),
    time_column="run_date",
    time_column_type=t.DateType(),
)

# COMMAND ----------

# first time initialization
if "loan_feature" not in globals():
    features_storage = FeaturesStorage(entity)

    @DecoratedDecorator
    class loan_feature(feature):  # noqa N081
        def __init__(self, *args, category=None):
            super().__init__(*args, entity=entity, category=category, features_storage=features_storage)

# COMMAND ----------


