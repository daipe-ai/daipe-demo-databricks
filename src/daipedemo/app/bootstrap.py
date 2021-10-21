# Databricks notebook source
# MAGIC %sh
# MAGIC pip install benvy==1.2.0b2

# COMMAND ----------

from benvy.databricks.repos import bootstrap  # noqa
from benvy.databricks.detector import is_databricks_repo  # noqa

if is_databricks_repo():
    bootstrap.install()

# COMMAND ----------

from benvy.databricks.repos import bootstrap  # noqa
from benvy.databricks.detector import is_databricks_repo  # noqa

if is_databricks_repo():
    bootstrap.setup_env()

# COMMAND ----------

# %install_master_package_whl
