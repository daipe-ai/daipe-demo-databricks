# Databricks notebook source
# MAGIC %md
# MAGIC # #3 Applying schema to loans table
# MAGIC ## Silver layer
# MAGIC Return to <a href="$../_index">index page</a>

# COMMAND ----------

# MAGIC %md
# MAGIC In this notebook a schema is applied to the __bronze__ level data - creating a __silver__ level table. We are following the bronze, silver, gold workflow. Now we need to parse the raw data according to our defined schema.
# MAGIC
# MAGIC ### Why use schema?
# MAGIC
# MAGIC Having a well defined schema helps check that the data has the correct format in production. For prototyping it is not necessary although __highly recommended__.

# COMMAND ----------

# MAGIC %run ../app/bootstrap

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from datalakebundle.imports import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema
# MAGIC
# MAGIC An explicit schema should be defined as:
# MAGIC
# MAGIC ```python
# MAGIC table_schema = TableSchema(full_table_identifier: str, fields: List[t.StructField], primary_key: Union[str, list], partition_by: Union[str, list] = None, tbl_properties: dict = None)
# MAGIC ```

# COMMAND ----------
def get_schema():
    return TableSchema(
        [
            t.StructField("LoanReportAsOfEOD", t.DateType(), nullable=True),
            t.StructField("LoanId", t.StringType(), nullable=True),
            t.StructField("LoanNumber", t.IntegerType(), nullable=True),
            t.StructField("ListedOnUTC", t.TimestampType(), nullable=True),
            t.StructField("BiddingStartedOn", t.TimestampType(), nullable=True),
            t.StructField("BidsPortfolioManager", t.IntegerType(), nullable=True),
            t.StructField("BidsApi", t.DoubleType(), nullable=True),
            t.StructField("BidsManual", t.DoubleType(), nullable=True),
            t.StructField("PartyId", t.StringType(), nullable=True),
            t.StructField("NewCreditCustomer", t.BooleanType(), nullable=True),
            t.StructField("LoanApplicationStartedDate", t.DateType(), nullable=True),
            t.StructField("LoanDate", t.DateType(), nullable=True),
            t.StructField("ContractEndDate", t.DateType(), nullable=True),
            t.StructField("FirstPaymentDate", t.DateType(), nullable=True),
            t.StructField("MaturityDate_Original", t.DateType(), nullable=True),
            t.StructField("MaturityDate_Last", t.DateType(), nullable=True),
            t.StructField("ApplicationSignedHour", t.IntegerType(), nullable=True),
            t.StructField("ApplicationSignedWeekday", t.IntegerType(), nullable=True),
            t.StructField("VerificationType", t.IntegerType(), nullable=True),
            t.StructField("LanguageCode", t.IntegerType(), nullable=True),
            t.StructField("Age", t.IntegerType(), nullable=True),
            t.StructField("DateOfBirth", t.DateType(), nullable=True),
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
            t.StructField("StageActiveSince", t.TimestampType(), nullable=True),
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
            t.StructField("NextPaymentDate", t.DateType(), nullable=True),
            t.StructField("NextPaymentNr", t.IntegerType(), nullable=True),
            t.StructField("NrOfScheduledPayments", t.IntegerType(), nullable=True),
            t.StructField("ReScheduledOn", t.StringType(), nullable=True),
            t.StructField("PrincipalDebtServicingCost", t.DoubleType(), nullable=True),
            t.StructField("InterestAndPenaltyDebtServicingCost", t.DoubleType(), nullable=True),
            t.StructField("ActiveLateLastPaymentCategory", t.StringType(), nullable=True),
        ],
        primary_key="LoanId",
        partition_by=[],  # ["LoanDate"]
        tbl_properties={},
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ### Applying schema and saving table
# MAGIC The `get_schema()` functin should be passed as a second argument into the `@table_overwrite`, `@table_upsert` or `@table_append` decorators.

# COMMAND ----------

from daipedemo.silver.tbl_loans_schema import get_schema


@transformation(read_table("bronze.tbl_loans"), display=True)
@table_overwrite("silver.tbl_loans", get_schema())
def convert_columns_and_save(df: DataFrame):
    date_cols = [c for c in df.columns if "Date" in c and "Till" not in c]
    date_cols.append("ReportAsOfEOD")

    return (
        df.select(*(f.col(c).cast("date").alias(c) if c in date_cols else f.col(c) for c in df.columns))
        .withColumn("ListedOnUTC", f.to_timestamp("ListedOnUTC"))
        .withColumn("BiddingStartedOn", f.to_timestamp("BiddingStartedOn"))
        .withColumn("StageActiveSince", f.to_timestamp("StageActiveSince"))
        .withColumnRenamed("ReportAsOfEOD", "LoanReportAsOfEOD")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$../silver/tbl_repayments/tbl_repayments">sample notebook #4</a>
