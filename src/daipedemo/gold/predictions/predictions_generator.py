# Databricks notebook source
# MAGIC %md
# MAGIC # Create model predictions

# COMMAND ----------

# MAGIC %run ../../app/bootstrap

# COMMAND ----------

from collections import namedtuple
from databricks import feature_store
from featurestorebundle.feature.FeatureStore import FeatureStore
from pyspark.sql import DataFrame
from pyspark.sql import functions as f, types as t
import daipe as dp

Args = namedtuple('Args', 'model_uri entity_name id_column table_name')

# COMMAND ----------

@dp.notebook_function()
def set_widgets(widgets: dp.Widgets):
    """Set widgets for args"""

    widgets.add_text("model_name", "")
    widgets.add_text("entity_name", "")
    widgets.add_text("id_column", "")
    widgets.add_text("table_name", "")

# COMMAND ----------

@dp.notebook_function()
def args(widgets: dp.Widgets) -> Args:
    """Get widgets args"""
    
    return (
        Args(
            f"models:/{widgets.get_value('model_name')}/Production",
            widgets.get_value("entity_name"),
            widgets.get_value("id_column"),
            widgets.get_value("table_name"),
        )
    )

# COMMAND ----------

@dp.transformation(args, display=False)
def load_feature_store(args: Args, feature_store: FeatureStore):
    """Get today's features"""

    return feature_store.get_latest(args.entity_name).select(args.id_column)

# COMMAND ----------

@dp.transformation(args, load_feature_store, display=False)
def score_batch(args: Args, ids: DataFrame):
    dbx_feature_store = feature_store.FeatureStoreClient()
    return dbx_feature_store.score_batch(args.model_uri, ids)

# COMMAND ----------

@dp.notebook_function(args)
def get_schema(args: Args):
    return dp.TableSchema(
        [
            t.StructField(args.id_column, t.StringType()),
            t.StructField("prediction", t.BooleanType()),
        ],
        primary_key=args.id_column,
    )

# COMMAND ----------

@dp.transformation(args, score_batch, display=True)
@dp.table_overwrite(f"gold.{args.result.table_name}", get_schema.result)
def save_predictions(args: Args, df: DataFrame):
    """Save predictions to table"""
    
    return (
        df
        .select(args.id_column, "prediction")
        .withColumn("prediction", f.col("prediction").cast("boolean"))
    )

# COMMAND ----------


