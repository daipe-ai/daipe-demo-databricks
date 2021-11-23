# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrating writing features to Feature Store

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inititialize feature store

# COMMAND ----------

# MAGIC %run ../../mlops/feature_store/feature_store_init

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize writer for Loan features

# COMMAND ----------

# MAGIC %run ./loan_feature_decorator_init

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run all feature notebooks

# COMMAND ----------

# MAGIC %run ./features/loan_features

# COMMAND ----------

# MAGIC %run ./features/interest_features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write all features at once

# COMMAND ----------

# MAGIC %run ../../mlops/feature_store/feature_store_write

# COMMAND ----------


