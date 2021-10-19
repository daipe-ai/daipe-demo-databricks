# Databricks notebook source
# MAGIC %md
# MAGIC # Model training with Daipe ML
# MAGIC
# MAGIC ###It greatly helps in the following part of a datascientist's work:
# MAGIC * Data exploration
# MAGIC * Feature selection
# MAGIC * Model training
# MAGIC * Model evaluation
# MAGIC * Model productionization
# MAGIC
# MAGIC Return to <a href="$../_index">index page</a>

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /dbfs/FileStore/datasciencefunctions-0.2.0b1-py3-none-any.whl /dbfs/datasciencefunctions-0.2.0b1-py3-none-any.whl

# COMMAND ----------

# MAGIC %pip install /dbfs/datasciencefunctions-0.2.0b1-py3-none-any.whl --use-feature=2020-resolver

# COMMAND ----------

# MAGIC %run ../../app/install_master_package

# COMMAND ----------

import datasciencefunctions as ds
import mlflow.spark
from databricks import feature_store
from databricks.feature_store import FeatureLookup
from datalakebundle.imports import transformation
from datasciencefunctions.data_exploration import plot_feature_hist_with_binary_target
from datasciencefunctions.feature_selection import feature_selection_merits
from datasciencefunctions.supervised import supervised_wrapper
from featurestorebundle.feature.FeatureStore import FeatureStore
from mlflow.tracking.client import MlflowClient

# COMMAND ----------

dbx_feature_store = feature_store.FeatureStoreClient()

# COMMAND ----------

# MAGIC %md #0. Loading data

# COMMAND ----------


@transformation(display=False)
def load_feature_store(feature_store: FeatureStore):
    return feature_store.get_latest("loans", "run_date")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Daipe ML can work with both pandas and pySpark dataframes and automatically recognizes which tools to use for each framework

# COMMAND ----------

df = load_feature_store.result
df_pandas = df.toPandas()

# COMMAND ----------

numeric_features = [
    "Amount",
    "Interest",
    "LoanDuration",
    "MonthlyPayment",
    "IncomeTotal",
    "LiabilitiesTotal",
    "RefinanceLiabilities",
    "DebtToIncome",
    "FreeCash",
    "NoOfPreviousLoansBeforeLoan",
    "PreviousRepaymentsBeforeLoan",
    "AmountOfPreviousLoansBeforeLoan",
    "MaturityDateDelay",
    "AmountNotGranted",
]

categorical_features = [
    "NewCreditCustomer",
    "VerificationType",
    "LanguageCode",
    "Gender",
    "Country",
    "UseOfLoan",
    "Education",
    "MaritalStatus",
    "NrOfDependants",
    "EmploymentStatus",
    "EmploymentDurationCurrentEmployer",
    "WorkExperience",
    "OccupationArea",
    "HomeOwnershipType",
    "ActiveScheduleFirstPaymentReached",
    "AgeGroup",
]

# COMMAND ----------

# MAGIC %md #1. Data exploration
# MAGIC
# MAGIC ###We can use Daipe ML to obtain a plot which gives us a good overview of each feature and its relationship with the target.

# COMMAND ----------

plot_feature_hist_with_binary_target(
    df=df,
    target_col="label",
    cat_cols=[
        "EmploymentDurationCurrentEmployer",
        "WorkExperience",
        "AgeGroup",
    ],
)

# COMMAND ----------

# MAGIC %md #2. Feature selection

# COMMAND ----------

df_corr = df_pandas[numeric_features + ["label"]].corr()
df_corr

# COMMAND ----------

# DBTITLE 1,target/label correlation with features
corr_target = df_corr.loc[["label"], df_corr.columns != "label"].abs()
corr_target

# COMMAND ----------

# DBTITLE 1,correlation between features excluding target/label
corr_features = df_corr.loc[df_corr.columns != "label", df_corr.index != "label"].abs()
corr_features

# COMMAND ----------

selected_features, feature_selection_history = feature_selection_merits(
    features_correlations=corr_features,
    target_correlations=corr_target,
    algorithm="forward",
    max_iter=30,
    best_n=3,
)

selected_features

# COMMAND ----------

# DBTITLE 1,finally, we select the resulting best feature set for modelling
key = ["LoanId", "run_date"]

feature_lookups = [
    FeatureLookup(
        table_name="dev_feature_store.features_loans",
        feature_name=feature,
        lookup_key=key,
    )
    for feature in selected_features
]

training_set = dbx_feature_store.create_training_set(
    df=df.select("LoanId", "run_date", "label"), feature_lookups=feature_lookups, label="label", exclude_columns=key
)

df_ml_spark = training_set.load_df()
df_ml_pandas = df_ml_spark.toPandas()

# COMMAND ----------

# MAGIC %md # 3. Model training, evaluation and productionization
# MAGIC
# MAGIC ## `supervised_wrapper`

# COMMAND ----------

# train test split, hyperparameter space, metrics to log and evaluate model, mlflow is done automatically
train_df, test_df, model_summary = supervised_wrapper(
    df=df_ml_spark,
    model_type=ds.MlModel.spark_random_forest_classifier,
    use_mlflow=False,
    label_col="label",
    params_fit_model={"max_evals": 1},
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log model to MLflow through Feature store
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/06/MLflow-logo-pos-TM-1.png" alt="mlflow" width="200"/>

# COMMAND ----------

model_name = "rfc_loan_default_prediction"

with mlflow.start_run(run_name="Random Forest Classifier - Loan Default Prediction") as run:
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

    client = MlflowClient()
    client.transition_model_version_stage(name=model_name, version=model_details.version, stage="Staging")
    client.update_model_version(name=model_name, version=model_details.version, description="This model predicts loan defaults.")
