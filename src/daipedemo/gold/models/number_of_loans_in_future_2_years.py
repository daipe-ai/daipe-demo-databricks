# Databricks notebook source
# MAGIC %run ../../app/install_master_package

# COMMAND ----------

import os
import datetime as dt
import datalakebundle.imports as dl
from daipecore.widgets.Widgets import Widgets
from pyspark.sql import functions as f
from pyspark.sql import DataFrame

import mlflow
import mlflow.spark
from mlflow.tracking.client import MlflowClient
from databricks.feature_store import FeatureLookup
from databricks import feature_store

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

import datasciencefunctions as ds
from datasciencefunctions.data_exploration import plot_feature_hist_with_binary_target
from datasciencefunctions.feature_selection import feature_selection_merits
from datasciencefunctions.supervised import supervised_wrapper, lift_curve

# COMMAND ----------


@dl.notebook_function()
def set_widgets(widgets: Widgets):
    widgets.add_text("target_date", dt.date.today().strftime("%Y-%m-%d"))


# COMMAND ----------

dbx_feature_store = feature_store.FeatureStoreClient()

# COMMAND ----------

db = f"{os.environ['APP_ENV']}_feature_store"
table = "features_client_historized"
label_col = "NumberOfLoansInFuture2Years"
model_name = "NumberOfLoansPrediction"

# COMMAND ----------


@dl.transformation(dl.read_table("silver.tbl_loans"), display=True)
def target(df: DataFrame, widgets: Widgets):
    target_date = dt.datetime.strptime(widgets.get_value("target_date"), "%Y-%m-%d")
    two_years_after_target_date = target_date + dt.timedelta(days=365 * 2)

    return (
        df.filter((f.col("LoanDate") >= target_date) & (f.col("LoanDate") <= two_years_after_target_date))
        .groupBy("UserName")
        .agg(f.count("LoanId").alias(label_col))
        .withColumn("run_date", f.lit(target_date))
    )


# COMMAND ----------

feature_lookups = [
    FeatureLookup(
        table_name=f"{db}.{table}",
        feature_name="Age",
        lookup_key=["UserName", "run_date"],
    ),
    FeatureLookup(
        table_name=f"{db}.{table}",
        feature_name="Gender",
        lookup_key=["UserName", "run_date"],
    ),
    FeatureLookup(
        table_name=f"{db}.{table}",
        feature_name="NumberOfLoansTakenLast365d",
        lookup_key=["UserName", "run_date"],
    ),
    FeatureLookup(
        table_name=f"{db}.{table}",
        feature_name="NumberOfLoansTakenLast1095d",
        lookup_key=["UserName", "run_date"],
    ),
    FeatureLookup(
        table_name=f"{db}.{table}",
        feature_name="NumberOfLoansTakenLast1825d",
        lookup_key=["UserName", "run_date"],
    ),
]

training_set = dbx_feature_store.create_training_set(
    df=target.result, feature_lookups=feature_lookups, label=label_col, exclude_columns=["UserName", "run_date"]
)

ml_df = training_set.load_df()

# COMMAND ----------

train_df, test_df, model_summary = supervised_wrapper(
    df=ml_df,
    model_type=ds.MlModel.spark_linear_regression,
    use_mlflow=False,
    label_col=label_col,
    params_fit_model={"max_evals": 2},
)

# COMMAND ----------

with mlflow.start_run(run_name=f"Linear Regression - {label_col}") as run:
    dbx_feature_store.log_model(
        model_summary["models"]["pipeline"],
        model_name,
        flavor=mlflow.spark,
        training_set=training_set,
    )

    ds.supervised.log_model_summary(model_summary)

    run_id = run.info.run_id
    model_uri = f"runs:/{run_id}/{model_name}"
    model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------

categorical_cols = ["Gender"]
numerical_cols = ["NumberOfLoansTakenLast365d", "NumberOfLoansTakenLast1095d", "NumberOfLoansTakenLast1825d", "Age"]
index_output_cols = [col + "Index" for col in categorical_cols]
ohe_output_cols = [col + "OHE" for col in categorical_cols]
assembler_inputs = ohe_output_cols + numerical_cols

# COMMAND ----------

with mlflow.start_run(run_name=f"Linear Regression - {label_col}") as run:

    # Create pipeline
    string_indexer = StringIndexer(inputCols=categorical_cols, outputCols=index_output_cols, handleInvalid="skip")
    ohe_encoder = OneHotEncoder(inputCols=index_output_cols, outputCols=ohe_output_cols)
    vec_assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol=label_col, regParam=1)
    stages = [string_indexer, ohe_encoder, vec_assembler, lr]
    pipeline = Pipeline(stages=stages)

    pipeline_model = pipeline.fit(train_df)

    # Log pipeline
    fs.log_model(
        pipeline_model,
        model_name,
        flavor=mlflow.spark,
        training_set=training_set,
    )

    # Log hyperparameters
    for param, value in pipeline_model.stages[-1].extractParamMap().items():
        mlflow.log_param(param.name, value)

    # Create predictions and metrics
    pred_df = pipeline_model.transform(test_df)
    evaluator = RegressionEvaluator(labelCol=label_col)
    rmse = evaluator.setMetricName("rmse").evaluate(pred_df)
    r2 = evaluator.setMetricName("r2").evaluate(pred_df)

    # Log metrics
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)

# COMMAND ----------

run_id = run.info.run_id
model_uri = f"runs:/{run_id}/{model_name}"

model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------

predictions = fs.score_batch(f"models:/{model_name}/None", target.result.limit(10))

display(predictions)
