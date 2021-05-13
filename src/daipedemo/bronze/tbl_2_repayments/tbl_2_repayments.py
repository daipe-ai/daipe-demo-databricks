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
# MAGIC
# MAGIC `# TODO: tady je to zmatečný, protože notebooky se jmenujou jinak, než tabulky.`
# MAGIC
# MAGIC As you must have noticed from the notebook filenames, we recommend using the `[db_name]/[table_name].py` notebooks directory structure and `[db_name].[table_name]` database structure.
# MAGIC
# MAGIC Each notebook corresponds to one table e.g. the current notebook `bronze/tbl_2_repayments/tbl_2_repayments.py` processes data to be written into the `bronze.tbl_repayments` table.
# MAGIC
# MAGIC For further information read [here](https://docs.daipe.ai/data-pipelines-workflow/managing-datalake/#2-recommended-notebooks-structure)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema
# MAGIC
# MAGIC An explicit schema should be defined as:
# MAGIC
# MAGIC ```python
# MAGIC table_schema = TableSchema(full_table_identifier: str, fields: List[t.StructField], primary_key: Union[str, list], partition_by: Union[str, list] = None)
# MAGIC ```
# MAGIC
# MAGIC The `table_schema` variable must be passed as an argument into the `@table_*` output decorator.

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
# MAGIC As you can see the real table name is `dev_bronze.tbl_2_repayments` where the prefix `dev` corresponds to the current `dev` environment. In `prod` environment the `prod_bronze.tbl_2_repayments` will be used.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$../../silver/tbl_3_loans">sample notebook #3</a>
