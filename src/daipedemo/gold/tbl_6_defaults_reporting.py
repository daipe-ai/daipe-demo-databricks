# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #6: Pandas, Widgets and Plotting
# MAGIC
# MAGIC In this notebook it all comes together. We are going to aggregate data and display it while using widgets for filtering.

# COMMAND ----------

# MAGIC %run ../app/install_master_package

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.dbutils import DBUtils  # enables to use Databricks dbutils within functions
from datalakebundle.imports import *
from logging import Logger

import seaborn as sns

# COMMAND ----------

# MAGIC %md
# MAGIC #### We are going to
# MAGIC  1. Load the silver dataset from a table
# MAGIC  2. Prepare Widgets for filtering
# MAGIC  3. Aggregate the data based on different attributes
# MAGIC  4. Plot the data using Widgets for filtering

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading the data

# COMMAND ----------


@transformation(read_table("silver.tbl_4_defaults"), display=True)
def read_silver_loans_tbl_defaults(df: DataFrame, logger: Logger):
    logger.info(df.count())
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing the Widgets

# COMMAND ----------


@transformation(read_silver_loans_tbl_defaults, display=True)
def min_and_max_year(df: DataFrame):
    return df.select("Year").agg(f.min("Year").alias("min"), f.max("Year").alias("max"))


# COMMAND ----------


@transformation(read_silver_loans_tbl_defaults)
def countries(df: DataFrame):
    return df.select("Country").dropDuplicates()


# COMMAND ----------


@transformation(read_silver_loans_tbl_defaults)
def ratings(df: DataFrame):
    return df.select("Rating").dropDuplicates()


# COMMAND ----------


@notebook_function(min_and_max_year, countries, ratings)
def create_input_widgets(years: DataFrame, countries: DataFrame, ratings: DataFrame, dbutils: DBUtils):
    min_year = years.toPandas().values[0][0]
    max_year = years.toPandas().values[0][1]
    country_list = list(map(lambda x: x[0], countries.toPandas().values.tolist()))
    rating_list = list(map(lambda x: x[0], ratings.toPandas().values.tolist()))
    # country_list.append("All")
    rating_list.remove(None)
    rating_list.sort()

    dbutils.widgets.dropdown("year", str(min_year), list(map(str, range(min_year, max_year + 1))), "Select year")
    dbutils.widgets.dropdown("country", country_list[0], country_list, "Select country")
    dbutils.widgets.dropdown("rating", "C", rating_list, "Select rating")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregating the data per month and per country

# COMMAND ----------


@notebook_function(read_silver_loans_tbl_defaults)
def defaults_per_month(df: DataFrame):
    year = dbutils.widgets.get("year")  # noqa: F821
    country = dbutils.widgets.get("country")  # noqa: F821
    rating = dbutils.widgets.get("rating")  # noqa: F821

    return (
        df.filter((f.col("Year") == year) & (f.col("Country") == country) & (f.col("Rating") == rating))
        .groupBy("Month")
        .agg(
            f.count("Defaulted").alias("Defaults"),
        )
        .orderBy("Defaults", ascending=False)
    )


# COMMAND ----------


@notebook_function(read_silver_loans_tbl_defaults)
def defaults_per_country(df: DataFrame):
    year = dbutils.widgets.get("year")  # noqa: F821
    rating = dbutils.widgets.get("rating")  # noqa: F821

    return (
        df.filter((f.col("Year") == year) & (f.col("Rating") == rating))
        .groupBy("Country")
        .agg(
            f.count("Defaulted").alias("Defaults"),
        )
        .orderBy("Defaults", ascending=False)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC #### Converting to Pandas and plotting the results

# COMMAND ----------


@notebook_function(defaults_per_month)
def plot_defaults_per_month(df: DataFrame):
    year = dbutils.widgets.get("year")  # noqa: F821
    country = dbutils.widgets.get("country")  # noqa: F821
    rating = dbutils.widgets.get("rating")  # noqa: F821

    ax = sns.barplot(x="Month", y="Defaults", data=df.toPandas())
    ax.set_title(f"Defaults per Month in {year} in {country} of {rating} rating")
    return display(ax)  # noqa: F821


# COMMAND ----------


@notebook_function(defaults_per_country)
def plot_defaults_per_country(df: DataFrame):
    year = dbutils.widgets.get("year")  # noqa: F821
    rating = dbutils.widgets.get("rating")  # noqa: F821

    ax = sns.barplot(x="Country", y="Defaults", data=df.toPandas())
    ax.set_title(f"Defaults per Country of {rating} rating during {year}")
    return display(ax)  # noqa: F821


# COMMAND ----------

# MAGIC %md
# MAGIC #### Remove all widgets

# COMMAND ----------

# dbutils.widgets.removeAll()
