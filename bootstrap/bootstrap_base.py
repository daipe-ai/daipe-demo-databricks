# Databricks notebook source
# MAGIC %run ./install_benvy

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

import os

if "APP_ENV" not in os.environ:
    os.environ["APP_ENV"] = "dev"
    os.environ["DBX_TOKEN"] = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())

# COMMAND ----------


