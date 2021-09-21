# Databricks notebook source
# this notebook should be included (% run) by all other Databricks notebooks to load the Master Package properly

# COMMAND ----------

# MAGIC %install_master_package_whl

# COMMAND ----------

# %flake8_setup
import IPython  # noqa E402

IPython.get_ipython().run_line_magic("pip", "install flake8==3.7.9 pycodestyle_magic==0.5")

# COMMAND ----------

import IPython  # noqa E402

ipy = IPython.get_ipython()
ipy.run_line_magic("load_ext", "pycodestyle_magic")
ipy.run_line_magic("flake8_on", "--ignore E501,F403,F405,W503")

# COMMAND ----------

import os  # noqa E402

if "APP_ENV" not in os.environ:
    os.environ["APP_ENV"] = "dev"
