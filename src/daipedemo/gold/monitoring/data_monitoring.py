# Databricks notebook source
# MAGIC %md
# MAGIC # Data monitoring

# COMMAND ----------

# MAGIC %run ../../app/install_master_package

# COMMAND ----------

from alibi_detect.cd.tabular import TabularDrift

import datalakebundle.imports as dl
from featurestorebundle.feature.FeatureStore import FeatureStore
from databricks.feature_store import FeatureStoreClient
from pyspark.sql import functions as f, types as t, SparkSession
import pandas as pd
import seaborn as sns
import datetime as dt
from logging import Logger
from daipecore.widgets.Widgets import Widgets
from daipecore.widgets.get_widget_value import get_widget_value

# COMMAND ----------

model_uri = "models:/rfc_loan_default_prediction/Production"

# COMMAND ----------


@dl.notebook_function()
def set_widgets(widgets: Widgets):
    """Set a widget for picking run_date"""

    widgets.add_text("run_date", dt.date.today().strftime("%Y-%m-%d"))


# COMMAND ----------


@dl.transformation(get_widget_value("run_date"), display=False)
def features_day_before(date_str: str, feature_store: FeatureStore):
    """Get a features a day before"""

    run_date = dt.datetime.strptime(date_str, "%Y-%m-%d")

    day_before = run_date - dt.timedelta(days=1)
    return feature_store.get("loans").where(f.col("run_date") == day_before).select("LoanId", "run_date")


# COMMAND ----------


@dl.transformation(get_widget_value("run_date"), display=False)
def features_now(date_str: str, feature_store: FeatureStore):
    """Get a features a day today"""

    run_date = dt.datetime.strptime(date_str, "%Y-%m-%d")

    return feature_store.get_latest("loans", "run_date").where(f.col("run_date") == run_date).select("LoanId", "run_date")


# COMMAND ----------


@dl.transformation(features_now)
def now(df):
    """Have Databricks Feature store collect all necessary features for today"""

    fs = FeatureStoreClient()
    return fs.score_batch(model_uri, df).drop("LoanId", "run_date", "prediction").toPandas()


# COMMAND ----------


@dl.transformation(features_day_before)
def day_before(df):
    """Have Databricks Feature store collect all necessary features for today"""

    fs = FeatureStoreClient()
    return fs.score_batch(model_uri, df).drop("LoanId", "run_date", "prediction").toPandas()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize drift

# COMMAND ----------


@dl.notebook_function(now, day_before)
def show_plot(now_df, day_before_df):
    plot_data = pd.concat([now_df.assign(period="training"), day_before_df.assign(period="serving")], axis=0, ignore_index=True)

    sns.set(
        rc={
            "figure.figsize": (11.7 * 1.5, 8.27 * 1.5),
            "font.size": 8,
            "axes.titlesize": 8,
            "axes.labelsize": 20,
            "legend.fontsize": 20,
            "legend.title_fontsize": 20,
        },
    )
    sns.set_style("whitegrid")
    sns.kdeplot(data=plot_data, hue="period", x="Amount", fill=True, common_norm=False)


# COMMAND ----------


@dl.notebook_function(now, day_before)
def get_drift(features_now_pandas, features_day_before_pandas):
    """Calculate drift"""

    td = TabularDrift(features_day_before_pandas.values, p_val=0.05)
    return td.predict(features_now_pandas.values)


# COMMAND ----------


def get_schema():
    """Schema for logging table"""

    return dl.TableSchema(
        [
            t.StructField("Date", t.DateType()),
            t.StructField("Threshold", t.DoubleType()),
            t.StructField("Is_Drift", t.BooleanType()),
        ],
        primary_key="Date",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Log drift to table

# COMMAND ----------


@dl.transformation(get_widget_value("run_date"), get_drift, display=True)
@dl.table_upsert("gold.tbl_data_monitoring", get_schema())
def save_result(date_str, result, spark: SparkSession):
    """Save schema into a logging table"""

    run_date = dt.datetime.strptime(date_str, "%Y-%m-%d")

    data = [[run_date, float(result["data"]["threshold"]), bool(result["data"]["is_drift"])]]
    df = spark.createDataFrame(data, schema=t.StructType(get_schema()))
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Crash if drift detected

# COMMAND ----------


@dl.notebook_function(get_drift)
def check_drift(result, logger: Logger):
    """Throw an exception if drift is detected"""

    logger.info("Checking drift...")

    if result["data"]["is_drift"]:
        raise Exception("Data drift detected. Model retraining initiated.")

    logger.info("No drift detected. All good.")


# COMMAND ----------
