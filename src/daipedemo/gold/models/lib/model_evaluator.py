# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluate a model

# COMMAND ----------

try:
  eval_metric
except NameError:
  print("Function eval_metric(labels, predictions) -> float] is not defined.")
  dbutils.notebook.exit("fail")

# COMMAND ----------

# MAGIC %run ../../../app/bootstrap

# COMMAND ----------

import os
import requests
from collections import namedtuple
import numpy as np
from logging import Logger

from pyspark.sql import DataFrame
from pyspark.dbutils import DBUtils
from databricks import feature_store

import datalakebundle.imports as dl
from daipecore.widgets.Widgets import Widgets
from featurestorebundle.feature.FeatureStore import FeatureStore

Args = namedtuple('Args', 'model_name entity_name id_column')

# COMMAND ----------

@dl.notebook_function()
def set_widgets(widgets: Widgets):
    """Set widgets for args"""

    widgets.add_text("model_name", "")
    widgets.add_text("entity_name", "")
    widgets.add_text("id_column", ""),

# COMMAND ----------

@dl.notebook_function()
def args(widgets: Widgets) -> Args:
    """Get widgets args"""
    
    return (
        Args(
            widgets.get_value("model_name"),
            widgets.get_value("entity_name"),
            widgets.get_value("id_column"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get accuracy

# COMMAND ----------

@dl.transformation(args, display=False)
def features_now(args: Args, feature_store: FeatureStore):
    return feature_store.get_latest(args.entity_name).select(args.id_column, "label")

# COMMAND ----------

def get_model_metric(args: Args, model_stage: str, df: DataFrame):
    """Get accuracy for specified model for given dataset."""

    model_uri = f"models:/{args.model_name}/{model_stage}"

    fs = feature_store.FeatureStoreClient()
    predictions = fs.score_batch(model_uri, df).select(args.id_column, "prediction")
    preds = predictions.select("prediction").toPandas()

    labels = df.select("label").toPandas()

    return eval_metric(labels, preds)

# COMMAND ----------

@dl.notebook_function(args, features_now)
def staging_acc(args: Args, features_now: DataFrame):
    """Get accuracy of the staging model"""

    return get_model_metric(args, "Staging", features_now)

# COMMAND ----------

@dl.notebook_function(args, features_now)
def prod_acc(args: Args, features_now: DataFrame):
    """Get accuracy of the production model"""
    
    return get_model_metric(args, "Production", features_now)

# COMMAND ----------

# MAGIC %md
# MAGIC ## If the new model is better, transition it to Production
# MAGIC ## Move the old one to Archive

# COMMAND ----------

@dl.notebook_function(staging_acc, prod_acc)
def trigger_promotion(staging_acc, prod_acc, logger: Logger, dbutils: DBUtils):
    """Makes a github API call which runs a model promotion pipeline"""

    if staging_acc >= prod_acc:
        logger.info(f"Running production promotion pipeline for new model")

        headers = {f"Authorization": f'token {dbutils.secrets.get(scope='git', key='token')}'}
        
        git_repo_handle = "daipe-ai/daipe-demo-databricks"
        url = f"https://api.github.com/repos/{git_repo_handle}/actions/workflows/promote_model.yml/dispatches"

        requests.post(url, headers=headers, json={"ref": "master"})
    else:
        logger.info("All good")

# COMMAND ----------


