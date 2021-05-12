# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Sample notebook #2: Configuration
# MAGIC
# MAGIC In this notebook, we will take a look at how to **use and change configuration parameters**.

# COMMAND ----------

# MAGIC %run ../../app/install_master_package

# COMMAND ----------

from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *
from daipedemo.bronze.tbl_2_repayments.csv_schema import get_schema as get_csv_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recommended project structure
# MAGIC As you must have noticed from the notebook filenames. We recommend using the `[db_name]/[domain]/[table_name]` directory structure. For table names it translates to `[db_name]_[domain].[table_name]`.
# MAGIC
# MAGIC Each notebook corresponds to one table e.g. this notebook in path `bronze/loans/tbl_2_repayments/tbl_2_repayments.py` manipulates with the table `bronze_loans.tbl_2_repayments`
# MAGIC
# MAGIC For further information read [here](https://docs.daipe.ai/data-pipelines-workflow/managing-datalake/#2-recommended-notebooks-structure)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema
# MAGIC
# MAGIC A schema is defined in a class of the same name which expects parameters:
# MAGIC  - `db` => string
# MAGIC  - `fields` => a list of StructField
# MAGIC  - `primary_key` => string/list of string
# MAGIC  - `partition_by` => string/list of string (optional)
# MAGIC
# MAGIC The class name is then passed as an argument to for example the `table_[overwrite]` function.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using the schema class as a console argument
# MAGIC `console datalake:table:create daipedemo.bronze.tbl_2_repayments.tbl_2_repayments`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Passing configuration to notebook function (optional)
# MAGIC
# MAGIC The following function uses the `"%loans.confirmed_csv_path%"` config parameter. To change it:
# MAGIC
# MAGIC 1. [Setup your local development environment](https://docs.daipe.ai/data-pipelines-workflow/daipe-demo-project/)
# MAGIC 1. Edit the `src/daipedemo/_config/config.yaml` file on your local machine
# MAGIC 1. Deploy changes back to Databricks by using the `console dbx:deploy` command.
# MAGIC
# MAGIC Reading paths from configuration makes the code **cleaner** than hard-coded paths.

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
# MAGIC As you can see the real table name is `dev_bronze.tbl_2_repayments` where the prefix `dev` corresponds to the chosen environment `env=dev`. In `prod` environment the `prod_bronze.tbl_2_repayments` will be used.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now that we have our data loaded, let's move to the following <a href="$../../silver/tbl_3_loans"> notebook</a>
