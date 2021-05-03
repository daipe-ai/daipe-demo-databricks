# Databricks notebook source
# MAGIC %md
# MAGIC # Sample notebook #3: Function based notebooks
# MAGIC
# MAGIC In this example notebook you will how and **why** to write function-based notebooks.

# COMMAND ----------

# MAGIC %run ../app/install_master_package

# COMMAND ----------

from pyspark.sql import functions as f, types as t
from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Advantages of function-based notebooks
# MAGIC  1. Create and publish auto-generated documentation and lineage of notebooks and pipelines (Daipe Enterprise)
# MAGIC  2. Write much cleaner notebooks with properly named code blocks
# MAGIC  3. Test specific notebook functions with ease
# MAGIC  4. Use YAML to configure your notebooks for given environment (dev/test/prod/...)
# MAGIC  5. Utilize pre-configured objects to automate repetitive tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decorators
# MAGIC  - `@notebook_function()`
# MAGIC  - `@transformation()`
# MAGIC    - read_csv()
# MAGIC    - read_table()
# MAGIC  - `@table_{overwrite/append/upsert}`
# MAGIC
# MAGIC  For further information read [here](https://docs.daipe.ai/data-pipelines-workflow/managing-datalake/#4-writing-function-based-notebooks)

# COMMAND ----------

# MAGIC %md #### Joining tables

# COMMAND ----------

# MAGIC %md
# MAGIC Joining two tables is so simple that it takes only **four** lines of code.
# MAGIC
# MAGIC It takes the function names two `read_table` functions as arguments. The resulting DataFrames are the arguments of the `join_loans_and_repayments` function which simply returns the joined DataFrame. This DataFrame is then saved to a table using the `@table_overwrite` decorator according to the following **schema**.

# COMMAND ----------


class tbl_3_joined_loans_and_repayments:
    db = "silver"
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
        t.StructField("ReportAsOfEOD", t.DateType(), True),
        t.StructField("LoanID", t.StringType(), True),
        t.StructField("Date", t.DateType(), True),
        t.StructField("PrincipalRepayment", t.DoubleType(), True),
        t.StructField("InterestRepayment", t.DoubleType(), True),
        t.StructField("LateFeesRepayment", t.DoubleType(), True),
        t.StructField("ID", t.LongType(), False),
    ]
    primary_key = "ID"  # INSERT PRIMARY KEY(s) HERE (MANDATORY)
    # partition_by = "" # INSERT PARTITIONS KEY(s) HERE (OPTIONAL)


# COMMAND ----------


@transformation(read_table("bronze.tbl_1_loans"))
def read_tbl_loans_and_rename_columns(df: DataFrame):
    return df.withColumnRenamed("ReportAsOfEOD", "LoanReportAsOfEOD")


# COMMAND ----------


@transformation(read_tbl_loans_and_rename_columns, read_table("bronze.tbl_2_repayments"), display=True)
@table_overwrite(tbl_3_joined_loans_and_repayments)
def join_loans_and_repayments(df1: DataFrame, df2: DataFrame, recreate_table=True):
    return df1.join(df2, "LoanID").withColumn("ID", f.monotonically_increasing_id())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's continue to the following <a href="$./tbl_4_defaults">notebook</a>
