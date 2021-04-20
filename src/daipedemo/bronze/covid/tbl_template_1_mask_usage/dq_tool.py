# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Tool Demo
# MAGIC This notebook shows how to use DQ Tool with Daipe.
# MAGIC
# MAGIC To find out more about DQ Tool and Settle DQ, check the [documentation site](https://docs.daipe.ai/settle-dq/).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC ### Personal Access Token for dataengineerics artifact feed
# MAGIC You need to have access to the [dataengineerics artifact feed](https://dev.azure.com/dataengineerics/adap-cz-dq/_packaging?_a=feed&feed=dataengineerics) where all the private wheels are stored.
# MAGIC
# MAGIC First, you need to [generate a Personal Access Token](https://docs.microsoft.com/en-us/azure/devops/organizations/accounts/use-personal-access-tokens-to-authenticate?view=azure-devops&tabs=preview-page#create-a-pat) in the `dataengineerics` organization with scope `Packaging: Read`.
# MAGIC
# MAGIC Set this token as an [environment variable](https://docs.databricks.com/clusters/configure.html#environment-variables) in your cluster. You need to set the following environment variables:
# MAGIC * `DATABRICKS_HTTP_BASIC_DATASENTICS_USERNAME`: use your azure login or simply `__token__`
# MAGIC * `DATABRICKS_HTTP_BASIC_DATASENTICS_PASSWORD`: use the Personal Access Token you just generated
# MAGIC
# MAGIC ### Bronze layer tables
# MAGIC Make sure the `bronze_covid.tbl_template_1_mask_usage` table exists and contains data. If not, run the corresponding notebook `%run ../bronze/covid/tbl_template_1_mask_usage/tbl_template_1_mask_usage`

# COMMAND ----------

# MAGIC %md
# MAGIC If you don't have the environment variables set up, this notebook will just exit.

# COMMAND ----------

import os

if not os.getenv("DATABRICKS_HTTP_BASIC_DATASENTICS_USERNAME") or not os.getenv("DATABRICKS_HTTP_BASIC_DATASENTICS_PASSWORD"):
    dbutils.notebook.exit(
        "DATABRICKS_HTTP_BASIC_DATASENTICS_USERNAME or DATABRICKS_HTTP_BASIC_DATASENTICS_PASSWORD is not set, doing nothing."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Load the Daipe Framework dependencies as in any other notebook.

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

# MAGIC %md
# MAGIC Now we install the DQ Tool bundle to have access to all the DQ Tool goodness.
# MAGIC
# MAGIC Note this only needs to be done for the demo project. For production use, you would add the bundle to the project dependencies and install it with the master package. See the docs XXXXX

# COMMAND ----------

import IPython, os

IPython.get_ipython().run_line_magic(
    "pip",
    f'install dq-tool-bundle --extra-index-url https://{os.getenv("DATABRICKS_HTTP_BASIC_DATASENTICS_USERNAME")}:{os.getenv("DATABRICKS_HTTP_BASIC_DATASENTICS_PASSWORD")}@pkgs.dev.azure.com/dataengineerics/_packaging/dataengineerics/pypi/simple/',
)

# COMMAND ----------

# import os
# user = os.getenv('DATABRICKS_HTTP_BASIC_DATASENTICS_USERNAME')
# token = os.getenv('DATABRICKS_HTTP_BASIC_DATASENTICS_PASSWORD')
# index_url = 'pkgs.dev.azure.com/dataengineerics/_packaging/dataengineerics/pypi/simple/'
# %pip install dq-tool-bundle --extra-index-url=https://$user:$token@$index_url

# COMMAND ----------

# MAGIC %md
# MAGIC DQ Tool prime time: define some expectations on top of the mask usage data.
# MAGIC
# MAGIC We take a table from the bronze layer and define a few expectations on how it should look. Check the json output to see the check results.
# MAGIC
# MAGIC Saving expectations to a database and validating data in a pipeline coming soon. A nicer output when defining expectations is coming too.

# COMMAND ----------

from pyspark.sql import SparkSession
from databricksbundle.notebook.decorator.notebook_function import notebook_function
from datalakebundle.table.TableManager import TableManager
from dq_tool import DQTool


@notebook_function()
def define_expectations_bronze_covid_tbl(spark: SparkSession, dq_tool: DQTool, table_manager: TableManager):
    # playground lets you run expectation on top of a table
    my_playground = dq_tool.get_playground(table_name=table_manager.get_name("bronze_covid.tbl_template_1_mask_usage"))
    # the NEVER column values should be between 0 and 1
    never_limits = my_playground.expect_column_values_to_be_between(column="NEVER", min_value=0, max_value=1)
    print(never_limits)
    # sum of the frequency columns should be roughly 1
    sum_one = my_playground.expect_column_expression_values_to_be_between(
        column_expression="NEVER + RARELY + SOMETIMES + FREQUENTLY + ALWAYS", min_value=0.99, max_value=1.01
    )
    print(sum_one)
    return
