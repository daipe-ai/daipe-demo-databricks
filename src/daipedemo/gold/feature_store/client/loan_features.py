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
from featurestorebundle.windows.windowed_features import windowed, with_time_windows

# COMMAND ----------


@dl.notebook_function()
def set_widgets(widgets: Widgets):
    widgets.add_select("storage_type", ["latest", "historized"], "latest")
    widgets.add_text("run_date", dt.date.today().strftime("%Y-%m-%d"))


# COMMAND ----------


@dl.transformation(dl.read_table("silver.tbl_loans"), display=True)
@client_feature(  # noqa
    ("NumberOfLoansTakenLast{time_window}", "Number of loans taken in the last {time_window}."),
    ("AmountTakenLast{time_window}", "Amount taken in the last {time_window}."),
    category="loan",
)
def client_loan_features(df: DataFrame, widgets: Widgets):
    columns_for_agg = []
    time_windows = ["30d", "180d", "365d", "1095d", "1825d"]
    df = df.select(
        f.col("UserName"),
        f.col("LoanId"),
        f.col("Amount"),
        f.col("LoanDate").cast(t.TimestampType()),
    )

    run_date = dt.datetime.strptime(widgets.get_value("run_date"), "%Y-%m-%d")

    df = with_time_windows(df, "LoanDate", f.lit(run_date), time_windows)

    for time_window in time_windows:
        columns_for_agg.extend(
            [
                f.sum(windowed(f.when(f.col("LoanId").isNotNull(), f.lit(1)).otherwise(f.lit(0)), time_window)).alias(
                    f"NumberOfLoansTakenLast{time_window}"
                ),
                f.sum(windowed(f.col("Amount"), time_window)).alias(f"AmountTakenLast{time_window}"),
            ]
        )

    return df.groupBy("UserName").agg(*columns_for_agg).withColumn("run_date", f.lit(run_date))
