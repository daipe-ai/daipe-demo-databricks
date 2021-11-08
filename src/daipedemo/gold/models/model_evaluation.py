# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluate the new model

# COMMAND ----------

# MAGIC %run ../../app/bootstrap

# COMMAND ----------

import os
from logging import Logger

import datalakebundle.imports as dl
import numpy as np
import requests
from databricks import feature_store
from featurestorebundle.feature.FeatureStore import FeatureStore

# COMMAND ----------

model_name = "rfc_loan_default_prediction"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get accuracy

# COMMAND ----------


@dl.transformation(display=False)
def features_now(feature_store: FeatureStore):
    return feature_store.get_latest("loans").select("LoanId", "label")


# COMMAND ----------


def get_accuracy(model_name, model_stage, df):
    """Get accuracy for specified model for given dataset."""

    model_uri = f"models:/{model_name}/{model_stage}"

    fs = feature_store.FeatureStoreClient()
    predictions = fs.score_batch(model_uri, df).select("LoanId", "prediction")
    preds = predictions.select("prediction").toPandas()

    labels = df.select("label").toPandas()
    accuracy = np.mean(labels.iloc[:, 0] == preds.iloc[:, 0])

    return accuracy


# COMMAND ----------


@dl.notebook_function(features_now)
def staging_acc(features_now):
    """Get accuracy of the staging model"""

    return get_accuracy(model_name, "Staging", features_now)


# COMMAND ----------


@dl.notebook_function(features_now)
def prod_acc(features_now):
    """Get accuracy of the production model"""

    return get_accuracy(model_name, "Production", features_now)


# COMMAND ----------

# MAGIC %md
# MAGIC ## If the new model is better, transition it to Production
# MAGIC ## Move the old one to Archive

# COMMAND ----------


@dl.notebook_function(staging_acc, prod_acc)
def trigger_promotion(staging_acc, prod_acc, logger: Logger):
    """Makes a github API call which runs a model promotion pipeline"""

    if staging_acc >= prod_acc:
        logger.info(f"Running production promotion pipeline for new model")

        headers = {f"Authorization": f'token {os.environ["GITHUB_TOKEN"]}'}
        url = "https://api.github.com/repos/ds-lukaslangr/kbc-demo/actions/workflows/promote_model.yml/dispatches"

        requests.post(url, headers=headers, json={"ref": "master"})
    else:
        logger.info("All good")

