# Databricks notebook source
# MAGIC %run ../../app/bootstrap_daipeML

# COMMAND ----------

import os
from logging import Logger

import datasciencefunctions as ds
from datasciencefunctions.data_exploration import plot_feature_hist_with_binary_target
from datasciencefunctions.feature_selection import feature_selection_merits
from datasciencefunctions.supervised import supervised_wrapper

from datalakebundle.imports import transformation, notebook_function
from featurestorebundle.feature.FeatureStore import FeatureStore

from daipedemo.mlops.daipe_ml import stage_model, train_model

# COMMAND ----------

@transformation(display=True)
def load_features(feature_store: FeatureStore):
    return feature_store.get_latest("loans")

# COMMAND ----------


