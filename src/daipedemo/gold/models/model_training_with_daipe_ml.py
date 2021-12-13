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
# MAGIC Return to <a href="$../../_index">index page</a>

# COMMAND ----------

# MAGIC %run ../../app/bootstrap_daipeML

# COMMAND ----------

import os
from logging import Logger

import datasciencefunctions as ds
from datasciencefunctions.data_exploration import plot_feature_hist_with_binary_target
from datasciencefunctions.feature_selection import feature_selection_merits
from datasciencefunctions.supervised import supervised_wrapper

from datalakebundle.imports import transformation, notebook_function
from featurestorebundle.feature.FeatureStore import FeatureStore

from daipedemo.mlops.daipe_ml import stage_model, train_model

# COMMAND ----------

# MAGIC %md #0. Loading data

# COMMAND ----------

@transformation(display=True)
def load_features(feature_store: FeatureStore):
    return feature_store.get_latest("loans")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Daipe ML can work with both pandas and pySpark dataframes and automatically recognizes which tools to use for each framework

# COMMAND ----------

df = load_features.result
df_pandas = df.toPandas()

# COMMAND ----------

# MAGIC %md # Data exploration

# COMMAND ----------

plot_feature_hist_with_binary_target(
    df=df,
    target_col="label",
    cat_cols=[
        "AgeGroup",
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature selection

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

# COMMAND ----------

df_corr = df_pandas[numeric_features + ["label"]].corr()
corr_target = df_corr.loc[["label"], df_corr.columns != "label"].abs()
corr_features = df_corr.loc[df_corr.columns != "label", df_corr.index != "label"].abs()

selected_features, _ = feature_selection_merits(
    features_correlations=corr_features,
    target_correlations=corr_target,
    algorithm="forward",
    max_iter=30,
    best_n=3,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Model creation

# COMMAND ----------

model_summary, training_set = train_model(
    df,
    entity_name="loans",
    id_column="LoanId",
    selected_features=selected_features,
    model_type=ds.MlModel.spark_random_forest_classifier,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log model to MLflow through Feature store

# COMMAND ----------

stage_model("rfc_loan_default_prediction", model_summary, training_set)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to <a href="$./model_evaluation">sample notebook #13</a>

# COMMAND ----------


