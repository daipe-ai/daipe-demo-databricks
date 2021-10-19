# Databricks notebook source
# this notebook should be included (% run) by all other Databricks notebooks to load the Master Package properly

# COMMAND ----------

import IPython
import os

if "MASTER_PACKAGE_INSTALLED" not in os.environ:
    IPython.get_ipython().run_line_magic("pip", "install -r /Workspace/Repos/kbc-demo/kbc-demo/requirements.txt --ignore-installed")

# COMMAND ----------

import sys  # noqa E402
import os  # noqa E402

if "MASTER_PACKAGE_INSTALLED" not in os.environ:
    # Setting added "src" into PYTHONPATH to be able to load project main module properly
    sys.path.append(os.path.abspath("/Workspace/Repos/kbc-demo/kbc-demo/src"))

# COMMAND ----------

from pathlib import Path  # noqa E402
from databricksbundle.bootstrap import package_config_reader  # noqa E402
from pyfonycore.bootstrap.config import config_factory  # noqa E402
from pyfonycore.bootstrap.config.raw import raw_config_reader  # noqa E402
import os  # noqa E402

# Fixes the fact that CWD is always current notebook directory
# See similar fix for Jupyter https://github.com/daipe-ai/daipe-core/blob/master/src/daipecore/bootstrap/config_reader.py#L11
if "MASTER_PACKAGE_INSTALLED" not in os.environ:

    def read_patched():
        pyproject_path = Path("/Workspace/Repos/kbc-demo/kbc-demo/pyproject.toml")
        raw_config = raw_config_reader.read(pyproject_path)
        return config_factory.create(raw_config, "[pyfony.bootstrap]")

    package_config_reader.read = read_patched

# COMMAND ----------

import os  # noqa E402

if "APP_ENV" not in os.environ:
    os.environ["APP_ENV"] = "dev"

if "MASTER_PACKAGE_INSTALLED" not in os.environ:
    os.environ["MASTER_PACKAGE_INSTALLED"] = "True"

os.environ["GITHUB_TOKEN"] = "INSERT_TOKEN_HERE"

# COMMAND ----------
