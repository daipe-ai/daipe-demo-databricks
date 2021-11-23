# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluate new model
# MAGIC 
# MAGIC Here we have a simple notebook where adding just a simple definition of an evaluation function gets you __a ready-to-go model redeployment workflow.__ 
# MAGIC 
# MAGIC Return to <a href="$../_index">index page</a>

# COMMAND ----------

# MAGIC %run ../../app/bootstrap

# COMMAND ----------

import numpy as np
from logging import Logger
from daipedemo.mlops.model_evaluator import evaluate_models, promote_new_model
import datalakebundle.imports as dl

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
                    model_name="rfc_loan_default_prediction",
                    entity_name="loans",
                    id_column="LoanId")
)
def compare_and_promote_models(new_is_better: bool, logger: Logger):
    if new_is_better:
        logger.info("New model promoted")
        promote_new_model()
    else:
        logger.info("Old model stays")
