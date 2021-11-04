# Databricks notebook source
# MAGIC %md
# MAGIC # Create model predictions

# COMMAND ----------

# MAGIC %run ../../app/install_master_package

# COMMAND ----------

import datalakebundle.imports as dl
from daipecore.widgets.get_widget_value import get_widget_value
from databricks import feature_store
from featurestorebundle.feature.FeatureStore import FeatureStore
from pyspark.sql import DataFrame
from pyspark.sql import functions as f, types as t


# COMMAND ----------


@dl.transformation(get_widget_value("run_date"), display=False)
def load_feature_store(date_str, feature_store: FeatureStore):
    """Get today's features"""

    return feature_store.get_latest("loans", "run_date").select("LoanId", "run_date")


# COMMAND ----------

model_uri = "models:/rfc_loan_default_prediction/Production"

# COMMAND ----------


@dl.transformation(load_feature_store, display=False)
def score_batch(features):
    dbx_feature_store = feature_store.FeatureStoreClient()
    return dbx_feature_store.score_batch(model_uri, features)


# COMMAND ----------


def get_schema():
    return dl.TableSchema(
        [
            t.StructField("LoanId", t.StringType(), True),
            t.StructField("run_date", t.TimestampType(), False),
            t.StructField("default_prediction", t.BooleanType(), True),
        ],
        primary_key="LoanId",
    )


# COMMAND ----------


@dl.transformation(score_batch, display=True)
@dl.table_overwrite("gold.tbl_default_prediction", get_schema())
def save_predictions(df: DataFrame):
    """Save predictions to table"""

    return (
        df.select("LoanId", "run_date", "prediction")
        .withColumn("prediction", f.col("prediction").cast("boolean"))
        .withColumnRenamed("prediction", "default_prediction")
    )


# COMMAND ----------
