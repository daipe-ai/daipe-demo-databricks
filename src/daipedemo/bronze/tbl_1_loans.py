# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #1: Create a new table from CSV

# COMMAND ----------

# MAGIC %md ## Welcome to your first Daipe-powered notebook!
# MAGIC In this notebook you will learn how to:
# MAGIC  - Load the Daipe framework
# MAGIC  - How to structure your data and notebooks
# MAGIC  - And how load CSVs into Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading Daipe framework and all project dependencies

# COMMAND ----------

# MAGIC %run ../app/install_master_package

# COMMAND ----------

import os
import urllib.request
from zipfile import ZipFile
from pyspark.sql import functions as f, types as t, SparkSession
from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *
from logging import Logger

# COMMAND ----------


def __generate_schema_class_string(table_identifier: str, df: DataFrame):
    db = table_identifier.split(".")[0]
    table = table_identifier.split(".")[1]
    schema_class = ""

    schema_class += f"class {table}:\n"
    schema_class += f'    db = "{db}"\n'
    schema_class += "    fields = [\n"

    for field in df.schema:
        schema_class += f'        t.StructField("{field.name}", t.{field.dataType}(), nullable=True),\n'

    schema_class += "    ]\n"
    schema_class += '    primary_key = "" # INSERT PRIMARY KEY(s) HERE (MANDATORY)\n'
    schema_class += '    # partition_by = "" # INSERT PARTITIONS KEY(s) HERE (OPTIONAL)\n'

    return schema_class


# COMMAND ----------

# MAGIC %md
# MAGIC ### Downloading and unpacking data
# MAGIC We are using the **Public Reports** dataset from Bondora, an Estonian peer-to-peer loan company.
# MAGIC For dataset format and explanation visit [here](https://www.bondora.com/en/public-reports#dataset-file-format)
# MAGIC
# MAGIC

# COMMAND ----------


@notebook_function()
def download_data():
    opener = urllib.request.URLopener()
    # Bondora server checks User-Agent and forbids the default User-Agent of urllib
    opener.addheader(
        "User-Agent",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    )

    opener.retrieve("https://www.bondora.com/marketing/media/LoanData.zip", "/loanData.zip")
    opener.retrieve("https://www.bondora.com/marketing/media/RepaymentsData.zip", "/repaymentsData.zip")


# COMMAND ----------


@notebook_function()
def unpack_data():
    with ZipFile("/loanData.zip", "r") as zip_obj:
        zip_obj.extractall("/")
    with ZipFile("/repaymentsData.zip", "r") as zip_obj:
        zip_obj.extractall("/")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Moving data to dbfs

# COMMAND ----------


@notebook_function()
def move_to_dbfs():
    dbutils.fs.cp("file:/LoanData.csv", "dbfs:/")  # noqa: F821
    dbutils.fs.cp("file:/RepaymentsData.csv", "dbfs:/")  # noqa: F821


# COMMAND ----------

# MAGIC %md
# MAGIC ## Standard Datalake layers
# MAGIC
# MAGIC ![Bronze, silver, gold](https://docs.daipe.ai/images/bronze_silver_gold.png)
# MAGIC
# MAGIC For further information read [here](https://docs.daipe.ai/data-pipelines-workflow/managing-datalake/#2-recommended-notebooks-structure)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating empty databases

# COMMAND ----------


@notebook_function()
def init(spark: SparkSession):
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_bronze;")  # noqa: F821
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_silver;")  # noqa: F821
    spark.sql(f"create database if not exists {os.environ['APP_ENV']}_gold;")  # noqa: F821


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Reading a CSV file
# MAGIC
# MAGIC Since this is a **bronze** notebook, we are going to be loading the raw data in CSV format into a Delta table.
# MAGIC
# MAGIC Use the `read_csv()` function inside the `@transformation` decorator to load the CSV file into Spark dataframe. Use `display=True` to display the DataFrame.

# COMMAND ----------


@transformation(
    read_csv("/LoanData.csv", options=dict(header=True, inferSchema=True)),
    display=True,
)
def read_csv_and_select_columns(df: DataFrame, logger: Logger):
    logger.info(f"Number of columns: {len(df.columns)}")
    # logger.info(f"{df.columns}")
    logger.info(f"Number of records: {df.count()}")
    return df.withColumnRenamed("ReportAsOfEOD", "LoanReportAsOfEOD")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Writing transformed data into a table
# MAGIC
# MAGIC In Daipe it is recommended to **write data into Hive tables rather than datalake paths**. The following code writes the returned Spark dataframe into the `bronze.tbl_1_loans` table.
# MAGIC
# MAGIC #### Schema or no schema?
# MAGIC
# MAGIC It is very much recommended to use a fixed schema in production environment, though in developement it is possible to use just the table name in our case `bronze.tbl_1_loans`. The input DataFrame schema will be used for table creation. This behavior raises a **warning** in the logs.
# MAGIC
# MAGIC We will look at how to define a schema in the following notebook.

# COMMAND ----------


class tbl_1_loans:
    db = "bronze"
    fields = [
        t.StructField("LoanReportAsOfEOD", t.StringType(), nullable=True),
        t.StructField("LoanId", t.StringType(), nullable=True),
        t.StructField("LoanNumber", t.IntegerType(), nullable=True),
        t.StructField("ListedOnUTC", t.StringType(), nullable=True),
        t.StructField("BiddingStartedOn", t.StringType(), nullable=True),
        t.StructField("BidsPortfolioManager", t.IntegerType(), nullable=True),
        t.StructField("BidsApi", t.IntegerType(), nullable=True),
        t.StructField("BidsManual", t.DoubleType(), nullable=True),
        t.StructField("UserName", t.StringType(), nullable=True),
        t.StructField("NewCreditCustomer", t.BooleanType(), nullable=True),
        t.StructField("LoanApplicationStartedDate", t.StringType(), nullable=True),
        t.StructField("LoanDate", t.StringType(), nullable=True),
        t.StructField("ContractEndDate", t.StringType(), nullable=True),
        t.StructField("FirstPaymentDate", t.StringType(), nullable=True),
        t.StructField("MaturityDate_Original", t.StringType(), nullable=True),
        t.StructField("MaturityDate_Last", t.StringType(), nullable=True),
        t.StructField("ApplicationSignedHour", t.IntegerType(), nullable=True),
        t.StructField("ApplicationSignedWeekday", t.IntegerType(), nullable=True),
        t.StructField("VerificationType", t.IntegerType(), nullable=True),
        t.StructField("LanguageCode", t.IntegerType(), nullable=True),
        t.StructField("Age", t.IntegerType(), nullable=True),
        t.StructField("DateOfBirth", t.StringType(), nullable=True),
        t.StructField("Gender", t.IntegerType(), nullable=True),
        t.StructField("Country", t.StringType(), nullable=True),
        t.StructField("AppliedAmount", t.DoubleType(), nullable=True),
        t.StructField("Amount", t.DoubleType(), nullable=True),
        t.StructField("Interest", t.DoubleType(), nullable=True),
        t.StructField("LoanDuration", t.IntegerType(), nullable=True),
        t.StructField("MonthlyPayment", t.DoubleType(), nullable=True),
        t.StructField("County", t.StringType(), nullable=True),
        t.StructField("City", t.StringType(), nullable=True),
        t.StructField("UseOfLoan", t.IntegerType(), nullable=True),
        t.StructField("Education", t.IntegerType(), nullable=True),
        t.StructField("MaritalStatus", t.IntegerType(), nullable=True),
        t.StructField("NrOfDependants", t.StringType(), nullable=True),
        t.StructField("EmploymentStatus", t.IntegerType(), nullable=True),
        t.StructField("EmploymentDurationCurrentEmployer", t.StringType(), nullable=True),
        t.StructField("EmploymentPosition", t.StringType(), nullable=True),
        t.StructField("WorkExperience", t.StringType(), nullable=True),
        t.StructField("OccupationArea", t.IntegerType(), nullable=True),
        t.StructField("HomeOwnershipType", t.IntegerType(), nullable=True),
        t.StructField("IncomeFromPrincipalEmployer", t.DoubleType(), nullable=True),
        t.StructField("IncomeFromPension", t.DoubleType(), nullable=True),
        t.StructField("IncomeFromFamilyAllowance", t.DoubleType(), nullable=True),
        t.StructField("IncomeFromSocialWelfare", t.DoubleType(), nullable=True),
        t.StructField("IncomeFromLeavePay", t.DoubleType(), nullable=True),
        t.StructField("IncomeFromChildSupport", t.DoubleType(), nullable=True),
        t.StructField("IncomeOther", t.DoubleType(), nullable=True),
        t.StructField("IncomeTotal", t.DoubleType(), nullable=True),
        t.StructField("ExistingLiabilities", t.IntegerType(), nullable=True),
        t.StructField("LiabilitiesTotal", t.DoubleType(), nullable=True),
        t.StructField("RefinanceLiabilities", t.IntegerType(), nullable=True),
        t.StructField("DebtToIncome", t.DoubleType(), nullable=True),
        t.StructField("FreeCash", t.DoubleType(), nullable=True),
        t.StructField("MonthlyPaymentDay", t.IntegerType(), nullable=True),
        t.StructField("ActiveScheduleFirstPaymentReached", t.BooleanType(), nullable=True),
        t.StructField("PlannedPrincipalTillDate", t.DoubleType(), nullable=True),
        t.StructField("PlannedInterestTillDate", t.DoubleType(), nullable=True),
        t.StructField("LastPaymentOn", t.StringType(), nullable=True),
        t.StructField("CurrentDebtDaysPrimary", t.IntegerType(), nullable=True),
        t.StructField("DebtOccuredOn", t.StringType(), nullable=True),
        t.StructField("CurrentDebtDaysSecondary", t.IntegerType(), nullable=True),
        t.StructField("DebtOccuredOnForSecondary", t.StringType(), nullable=True),
        t.StructField("ExpectedLoss", t.DoubleType(), nullable=True),
        t.StructField("LossGivenDefault", t.DoubleType(), nullable=True),
        t.StructField("ExpectedReturn", t.DoubleType(), nullable=True),
        t.StructField("ProbabilityOfDefault", t.DoubleType(), nullable=True),
        t.StructField("DefaultDate", t.DateType(), nullable=True),
        t.StructField("PrincipalOverdueBySchedule", t.DoubleType(), nullable=True),
        t.StructField("PlannedPrincipalPostDefault", t.DoubleType(), nullable=True),
        t.StructField("PlannedInterestPostDefault", t.DoubleType(), nullable=True),
        t.StructField("EAD1", t.DoubleType(), nullable=True),
        t.StructField("EAD2", t.DoubleType(), nullable=True),
        t.StructField("PrincipalRecovery", t.DoubleType(), nullable=True),
        t.StructField("InterestRecovery", t.DoubleType(), nullable=True),
        t.StructField("RecoveryStage", t.IntegerType(), nullable=True),
        t.StructField("StageActiveSince", t.StringType(), nullable=True),
        t.StructField("ModelVersion", t.IntegerType(), nullable=True),
        t.StructField("Rating", t.StringType(), nullable=True),
        t.StructField("EL_V0", t.DoubleType(), nullable=True),
        t.StructField("Rating_V0", t.StringType(), nullable=True),
        t.StructField("EL_V1", t.DoubleType(), nullable=True),
        t.StructField("Rating_V1", t.StringType(), nullable=True),
        t.StructField("Rating_V2", t.StringType(), nullable=True),
        t.StructField("Status", t.StringType(), nullable=True),
        t.StructField("Restructured", t.BooleanType(), nullable=True),
        t.StructField("ActiveLateCategory", t.StringType(), nullable=True),
        t.StructField("WorseLateCategory", t.StringType(), nullable=True),
        t.StructField("CreditScoreEsMicroL", t.StringType(), nullable=True),
        t.StructField("CreditScoreEsEquifaxRisk", t.StringType(), nullable=True),
        t.StructField("CreditScoreFiAsiakasTietoRiskGrade", t.StringType(), nullable=True),
        t.StructField("CreditScoreEeMini", t.IntegerType(), nullable=True),
        t.StructField("PrincipalPaymentsMade", t.DoubleType(), nullable=True),
        t.StructField("InterestAndPenaltyPaymentsMade", t.DoubleType(), nullable=True),
        t.StructField("PrincipalWriteOffs", t.DoubleType(), nullable=True),
        t.StructField("InterestAndPenaltyWriteOffs", t.DoubleType(), nullable=True),
        t.StructField("PrincipalBalance", t.DoubleType(), nullable=True),
        t.StructField("InterestAndPenaltyBalance", t.DoubleType(), nullable=True),
        t.StructField("NoOfPreviousLoansBeforeLoan", t.IntegerType(), nullable=True),
        t.StructField("AmountOfPreviousLoansBeforeLoan", t.DoubleType(), nullable=True),
        t.StructField("PreviousRepaymentsBeforeLoan", t.DoubleType(), nullable=True),
        t.StructField("PreviousEarlyRepaymentsBefoleLoan", t.DoubleType(), nullable=True),
        t.StructField("PreviousEarlyRepaymentsCountBeforeLoan", t.IntegerType(), nullable=True),
        t.StructField("GracePeriodStart", t.StringType(), nullable=True),
        t.StructField("GracePeriodEnd", t.StringType(), nullable=True),
        t.StructField("NextPaymentDate", t.StringType(), nullable=True),
        t.StructField("NextPaymentNr", t.IntegerType(), nullable=True),
        t.StructField("NrOfScheduledPayments", t.IntegerType(), nullable=True),
        t.StructField("ReScheduledOn", t.StringType(), nullable=True),
        t.StructField("PrincipalDebtServicingCost", t.DoubleType(), nullable=True),
        t.StructField("InterestAndPenaltyDebtServicingCost", t.DoubleType(), nullable=True),
        t.StructField("ActiveLateLastPaymentCategory", t.StringType(), nullable=True),
    ]
    primary_key = "LoanId"  # INSERT PRIMARY KEY(s) HERE (MANDATORY)
    # partition_by = "" # INSERT PARTITIONS KEY(s) HERE (OPTIONAL)


# COMMAND ----------


@notebook_function(read_csv_and_select_columns)
def print_schema(df: DataFrame):
    table_class = __generate_schema_class_string("bronze.tbl_1_loans", df)
    print(f"you can define schema as\n\n{table_class}")


# COMMAND ----------


@transformation(read_csv_and_select_columns, display=True)
@table_overwrite(tbl_1_loans)
def save(df: DataFrame, logger: Logger):
    # table_class = __generate_schema_class_string("bronze.tbl_1_loans", df)
    # logger.warning(f"you can define schema as\n\n{table_class}")
    logger.info(f"Saving {df.count()} records")
    return df.withColumn("DefaultDate", f.to_date(f.col("DefaultDate"), "yyyy-MM-dd"))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Reading data from a table
# MAGIC
# MAGIC To check that the data is in the table, let's use the `read_table()` function inside the `@transformation` decorator to load the data from our table.

# COMMAND ----------


@transformation(read_table("bronze.tbl_1_loans"), display=True)
def read_table_tbl_loans(df: DataFrame):
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's move on to the second <a href="$./tbl_2_repayments/tbl_2_repayments">notebook</a>
