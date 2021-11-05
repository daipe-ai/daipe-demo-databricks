# Databricks notebook source
# MAGIC %md
# MAGIC # #1 Create a new table from CSV
# MAGIC ### Bronze layer
# MAGIC Return to <a href="$../_index">index page</a>

# COMMAND ----------

# MAGIC %md ## Welcome to your data manipulation notebook using Daipe!
# MAGIC In this notebook we would like to show you:
# MAGIC  - How to structure data and notebooks
# MAGIC  - How to load CSVs into Delta tables
# MAGIC  - How to read a Delta table and display it

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading Daipe framework and all project dependencies

# COMMAND ----------

# MAGIC %run ../app/bootstrap

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports
# MAGIC Everything the Daipe framework needs is imported using `from datalakebundle.imports import *`

# COMMAND ----------

from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recommended Datalake layers
# MAGIC
# MAGIC ![Bronze, silver, gold](https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Overview.png)
# MAGIC For further information read [here](https://databricks.com/blog/2019/08/14/productionizing-machine-learning-with-delta-lake.html)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Reading a CSV file
# MAGIC
# MAGIC Since this is a **bronze** notebook, we are going to be loading the raw CSV into a Delta table.
# MAGIC But let's say we want to order them by `LoanDate`.
# MAGIC
# MAGIC This is how it is done using _PySpark_ without the Daipe framework.
# MAGIC - We need to get the `Spark session`
# MAGIC - Load the CSV into a DataFrame
# MAGIC - Make a transformation - `orderBy`
# MAGIC - Write the data to a Delta table, using some sensible options
# MAGIC - And display the data

# COMMAND ----------

df = spark.read.csv("/LoanData.csv", header=True, inferSchema=True)
df = df.limit(1000).orderBy("LoanDate")  # Limit 1000 so it is fast
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dev_bronze.tbl_loans_no_daipe")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Introducing the `@transformation()` decorator
# MAGIC
# MAGIC The tranformation decorator is used whenever the enclosed function manipulates with a DataFrame - meaning a DataFrame is its input and a transformed DataFrame its output. `display=True` allows to display the input DataFrame.
# MAGIC
# MAGIC It takes a `read_csv` function as an argument. The `read_csv` function loads the CSV data into a DataFrame which is then __injected__ into the `save` function. We can simply do the DataFrame modifications inside the save function.
# MAGIC
# MAGIC The modified DataFrame is then returned and saved in a table using the secondary `@table_overwrite()` decorator. This decorator takes a `table_name` as its argument and it is used in conjunction with the `@transformation()` decorator to save to output DataFrame into the `table_name`.
# MAGIC
# MAGIC #### Advantages
# MAGIC
# MAGIC The folowing code is:
# MAGIC - simpler, cleaner, follows best-practices
# MAGIC - self-documenting
# MAGIC - testable
# MAGIC - using a standard Logger
# MAGIC - checking if your schema fits
# MAGIC - production-ready (-ish)

# COMMAND ----------

@transformation(read_csv("/LoanData.csv", options=dict(header=True, inferSchema=True)), display=True)
@table_overwrite("bronze.tbl_loans")
def save(df: DataFrame):
    return df.orderBy("LoanDate")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schema or no schema?
# MAGIC
# MAGIC When prototyping your data transformation logic, write your dataframe to table simply by providing the table name (`bronze.tbl_loans` in this case). Once you are satisfied with you code, provide fixed schema to let Daipe check the dataframe against it. Daipe will show __warnings__ contatining the __schema ready to be copied into your code__ when saving a table without a schema.
# MAGIC
# MAGIC The __fixed schema__ approach is recommended especially in the production environments.
# MAGIC
# MAGIC We will look at how to define a schema in a later notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$./tbl_repayments/tbl_repayments">sample notebook #2</a>
