# Databricks notebook source
# MAGIC %md
# MAGIC # Define evaluation metric

# COMMAND ----------

def eval_metric(labels, predictions) -> float:
    return np.mean(labels.iloc[:, 0] == predictions.iloc[:, 0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run model evaluation

# COMMAND ----------

# MAGIC %run ./lib/model_evaluator $model_name="rfc_loan_default_prediction" $entity_name="loans" $id_column="LoanId"

# COMMAND ----------


