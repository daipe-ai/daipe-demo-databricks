# Databricks notebook source
# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

# MAGIC %run ./client_feature_init

# COMMAND ----------

import datetime as dt
import datalakebundle.imports as dl
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from daipecore.widgets.Widgets import Widgets

# COMMAND ----------


@dl.notebook_function()
def set_widgets(widgets: Widgets):
    widgets.add_select("storage_type", ["latest", "historized"], "latest")
    widgets.add_text("run_date", dt.date.today().strftime("%Y-%m-%d"))


# COMMAND ----------


@dl.transformation(dl.read_table("silver.tbl_loans"), display=True)
@client_feature(  # noqa F821
    ("Age", "Client's age"),
    ("Gender", "Client's gender"),
    ("WorkExperience", "Client's work experience"),
    category="personal",
)
def client_personal_features(df: DataFrame, widgets: Widgets):
    run_date = dt.datetime.strptime(widgets.get_value("run_date"), "%Y-%m-%d")

    return (
        df.select("UserName", "Age", "Gender", "WorkExperience")
        .withColumn("Gender", f.when(f.col("Gender") == 0, "MALE").otherwise("FEMALE"))
        .groupBy("UserName")
        .agg(
            f.max("Age").alias("Age"),
            f.first("Gender").alias("Gender"),
            f.first("WorkExperience").alias("WorkExperience"),
        )
        .withColumn("run_date", f.lit(run_date))
    )
