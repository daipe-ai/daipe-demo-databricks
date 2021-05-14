# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # #2 Configuration
# MAGIC Return to <a href="$../../_index">index page</a>
# MAGIC
# MAGIC In this notebook, we will take a look at how to **use and change configuration parameters**.

# COMMAND ----------

# MAGIC %run ../../app/install_master_package

# COMMAND ----------

from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *
from daipedemo.bronze.tbl_repayments.csv_schema import get_schema as get_csv_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recommended project structure
# MAGIC
# MAGIC As you must have noticed from the notebook filenames. We recommend using the `[db_name]/[domain]/[table_name]` directory structure. For table names it translates to `[db_name]_[domain].[table_name]`.
# MAGIC
# MAGIC Each notebook should correspond to one table e.g. the current notebook `bronze/tbl_repayments/tbl_repayments.py` processes data to be written into the `bronze.tbl_repayments` table.
# MAGIC
# MAGIC For further information read [here](https://docs.daipe.ai/data-pipelines-workflow/managing-datalake/#2-recommended-notebooks-structure)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Passing configuration to notebook functions (optional)
# MAGIC
# MAGIC The following function uses the `"%loans.confirmed_csv_path%"` config parameter. To change it:
# MAGIC
# MAGIC 1. [Setup your local development environment](https://docs.daipe.ai/data-pipelines-workflow/daipe-demo-project/)
# MAGIC 1. Edit the `src/daipedemo/_config/config.yaml` file on your local machine
# MAGIC 1. Deploy changes back to Databricks by using the `console dbx:deploy-master-package` command.
# MAGIC
# MAGIC Reading paths from configuration makes the code **cleaner** compared to hard-coded paths.

# COMMAND ----------


@transformation(
    read_csv("%loans.repayments_csv_path%", schema=get_csv_schema(), options=dict(header=True)),
)
@table_overwrite("bronze.tbl_repayments")
def load_csv_and_save(df: DataFrame):
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC #### The real table name

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see the real table name is `dev_bronze.tbl_repayments` where the prefix `dev` corresponds to the current `dev` environment. In `prod` environment the `prod_bronze.tbl_repayments` will be used.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$../../silver/tbl_loans">sample notebook #3</a>
