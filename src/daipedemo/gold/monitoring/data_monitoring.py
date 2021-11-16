# Databricks notebook source
# MAGIC %md
# MAGIC # Data monitoring

# COMMAND ----------

# MAGIC %run ../../app/bootstrap_alibi

# COMMAND ----------

from logging import Logger
from collections import namedtuple

import pandas as pd
import datetime as dt
from alibi_detect.cd.tabular import TabularDrift

from pyspark.sql import functions as f, types as t, SparkSession, DataFrame
from databricks.feature_store import FeatureStoreClient

import datalakebundle.imports as dl
from daipecore.widgets.Widgets import Widgets
from featurestorebundle.feature.FeatureStore import FeatureStore
from daipedemo.gold.monitoring.lib import get_drift_table_schema, plot_drift

Args = namedtuple('Args', 'model_uri run_date entity_name id_column time_column')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create widgets for arguments

# COMMAND ----------

@dl.notebook_function()
def set_widgets(widgets: Widgets):
    """Set widgets for args"""

    widgets.add_text("run_date", dt.date.today().strftime("%Y-%m-%d"))
    widgets.add_text("model_name", "")
    widgets.add_text("entity_name", "")
    widgets.add_text("id_column", "")
    widgets.add_text("time_column", "")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read widget arguments

# COMMAND ----------

@dl.notebook_function()
def args(widgets: Widgets) -> Args:
    """Get widgets args"""
    
    return (
        Args(f"models:/{widgets.get_value('model_name')}/Production",
            dt.datetime.strptime(widgets.get_value("run_date"), "%Y-%m-%d"),
            widgets.get_value("entity_name"),
            widgets.get_value("id_column"),
            widgets.get_value("time_column"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define function for loading

# COMMAND ----------

def get_features(feature_store: FeatureStore, date: dt.datetime, args: Args):
    """Get widgets args"""

    dbx_fs_client = FeatureStoreClient()
    
    features = (
      feature_store
      .get_historized(args.entity_name)
      .where(f.col(args.time_column) == date)
      .select(args.id_column, args.time_column)
    )
    
    return (
      dbx_fs_client
      .score_batch(args.model_uri, features)
      .drop(args.id_column, args.time_column, "prediction")
      .toPandas()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get features from a day before run_date

# COMMAND ----------

@dl.transformation(args, display=True)
def features_day_before(args: Args, feature_store: FeatureStore):
    """Get a features a day before"""

    day_before = args.run_date - dt.timedelta(days=1)
    return get_features(feature_store, day_before, args)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get features from run_date

# COMMAND ----------

@dl.transformation(args, display=True)
def features_today(args: Args, feature_store: FeatureStore):
    """Get a features a today"""
    
    return get_features(feature_store, args.run_date, args)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize drift

# COMMAND ----------

@dl.notebook_function(features_today, features_day_before)
def show_plot(now_df, day_before_df):
    plot_drift(now_df, day_before_df, "Amount")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Drift using Alibi-detect

# COMMAND ----------

@dl.notebook_function(features_today, features_day_before)
def get_drift(features_now_pandas, features_day_before_pandas):
    """Calculate drift"""

    td = TabularDrift(features_day_before_pandas.values, p_val=0.05)
    return td.predict(features_now_pandas.values)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log drift to table
# MAGIC 
# MAGIC Table contains all drift calculations for each __[Date, Entity_name]__ 

# COMMAND ----------

@dl.transformation(args, get_drift, display=True)
@dl.table_upsert("gold.tbl_data_monitoring", get_drift_table_schema())
def save_result(args: Args, result, spark: SparkSession):
    """Save schema into a logging table"""
    
    data = [[args.run_date, args.entity_name, float(result["data"]["threshold"]), bool(result["data"]["is_drift"])]]
    df = spark.createDataFrame(data, schema=t.StructType(get_drift_table_schema()))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Throw an exception if drift is NOT detected
# MAGIC to cancel the pipeline, if drift is detected continue to retrain the model

# COMMAND ----------

@dl.notebook_function(get_drift)
def check_drift(result, logger: Logger):
    """Throw an exception if drift is detected"""

    logger.info("Checking drift...")

    if not result["data"]["is_drift"]:
        logger.info("Data drift has NOT been detected. Cancelling retraining pipeline.")
        raise Exception("Data drift has NOT been detected. Cancelling retraining pipeline.")
    
    logger.info("Data drift detected. Commencing retraining pipeline...")

# COMMAND ----------


