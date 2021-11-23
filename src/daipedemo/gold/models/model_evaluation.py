# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluate new model
# MAGIC 
# MAGIC Here we have a simple notebook where adding just a simple definition of an evaluation function gets you __a ready-to-go model redeployment workflow.__ 
# MAGIC 
# MAGIC Return to <a href="$../../_index">index page</a>

# COMMAND ----------

# MAGIC %run ../../app/bootstrap

# COMMAND ----------

import numpy as np
from logging import Logger
from collections import namedtuple
from daipedemo.mlops.model_evaluator import evaluate_models, promote_new_model
import datalakebundle.imports as dl
from daipecore.imports import Widgets

Args = namedtuple('Args', 'model_name entity_name id_column repo_handle')

# COMMAND ----------

@dl.notebook_function()
def create_widgets(widgets: Widgets):
    widgets.add_text("model_name", "")
    widgets.add_text("entity_name", "")
    widgets.add_text("id_column", "")
    widgets.add_text("repo_handle", "")

# COMMAND ----------

@dl.notebook_function()
def args(widgets: Widgets) -> Args:
    """Get widgets args"""
    
    return (
        Args(
            widgets.get_value("model_name"),
            widgets.get_value("entity_name"),
            widgets.get_value("id_column"),
            widgets.get_value("repo_handle"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define custom evaluation metric
# MAGIC 
# MAGIC In our case `accuracy` 

# COMMAND ----------

def eval_metric(labels, predictions) -> float:
    return np.mean(labels.iloc[:, 0] == predictions.iloc[:, 0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run model evaluation

# COMMAND ----------

@dl.notebook_function(
    evaluate_models(eval_metric,
                    model_name=args.result.model_name,
                    entity_name=args.result.entity_name,
                    id_column=args.result.id_column),
    args
)
def compare_and_promote_models(new_is_better: bool, args: Args, logger: Logger):
    if new_is_better:
        logger.info("New model promoted")
        promote_new_model(args.model_name, args.repo_handle)
    else:
        logger.info("Old model stays")

# COMMAND ----------


