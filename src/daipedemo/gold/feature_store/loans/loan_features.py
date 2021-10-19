# Databricks notebook source
# MAGIC %md
# MAGIC # Creating features for ML
# MAGIC
# MAGIC Return to <a href="$../_index">index page</a>
# MAGIC
# MAGIC In this notebook we preprocess the data for modelling purposes.

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

# MAGIC %run ./loan_feature_decorator_init

# COMMAND ----------

import datetime as dt

import numpy as np
from daipecore.widgets.Widgets import Widgets
from daipecore.widgets.get_widget_value import get_widget_value
from datalakebundle.imports import *
from pyspark.ml.feature import Bucketizer
from pyspark.sql import DataFrame, functions as f, Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets for parameters

# COMMAND ----------


@notebook_function()
def create_input_widgets(widgets: Widgets):
    widgets.add_text("observation_period", "90", "Observation period")
    widgets.add_text("default_days", "90", "Default days")
    widgets.add_text("default_prediction", "365", "Default prediction")
    widgets.add_text("run_date", dt.date.today().strftime("%Y-%m-%d"))
    widgets.add_select("storage_type", ["latest", "historized"], "latest")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Define lengths of time windows

# COMMAND ----------


@transformation(read_table("silver.tbl_joined_loans_and_repayments"))
def read_joined_loans_and_repayments(df: DataFrame):
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Preselect columns which might be useful as features

# COMMAND ----------


@transformation(read_joined_loans_and_repayments)
def select_columns(df: DataFrame):
    return df.select(
        "LoanId",
        "LoanDate",
        "UserName",
        "NewCreditCustomer",
        "MaturityDate_Original",
        "MaturityDate_Last",
        "VerificationType",
        "LanguageCode",
        "Age",
        "Gender",
        "Country",
        "AppliedAmount",
        "Amount",
        "Interest",
        "LoanDuration",
        "MonthlyPayment",
        "Date",
        "UseOfLoan",
        "Education",
        "MaritalStatus",
        "NrOfDependants",
        "EmploymentStatus",
        "EmploymentDurationCurrentEmployer",
        "WorkExperience",
        "OccupationArea",
        "HomeOwnershipType",
        "IncomeTotal",
        "LiabilitiesTotal",
        "FirstPaymentDate",
        "RefinanceLiabilities",
        "DebtToIncome",
        "FreeCash",
        "ActiveScheduleFirstPaymentReached",
        "DefaultDate",
        "NoOfPreviousLoansBeforeLoan",
        "PreviousRepaymentsBeforeLoan",
        "AmountOfPreviousLoansBeforeLoan",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate new features

# COMMAND ----------


@transformation(select_columns)
def get_buckets(df: DataFrame):
    split_list = [float(i) for i in np.arange(0, 81, 5)]
    return Bucketizer(splits=split_list, inputCol="Age", outputCol="AgeGroup").transform(df).drop("Age")


# COMMAND ----------


@transformation(get_buckets)
def get_new_features(df: DataFrame):
    return (
        df.withColumn(
            "AgeGroup",
            f.concat(
                (f.col("AgeGroup") * 5).cast("int").cast("string"), f.lit("-"), (f.col("AgeGroup") * 5 + 5).cast("int").cast("string")
            ),
        )
        .withColumn("MaturityDateDelay", f.datediff("MaturityDate_Last", "MaturityDate_Original"))
        .withColumn("AmountNotGranted", f.col("AppliedAmount") - f.col("Amount"))
        .withColumn("DaysToFirstPayment", f.datediff("FirstPaymentDate", "LoanDate"))
        .drop("MaturityDate_Original", "AppliedAmount", "FirstPaymentDate")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC * MaturityDateDelay - difference between MaturityDate_Last and MaturityDate_Original
# MAGIC * AgeGroup - categorical age groups
# MAGIC * AmountNotGranted - difference between amount asked for and granted
# MAGIC * DaysToFirstPayment - difference between days from taking the loans and the first payment made

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create target

# COMMAND ----------

# nextPaymentDiff - how many days was between two payments, if more than default_days, then we cansider this loans as defaulted
# DaysFromStart - we want to concentrate only on new loans and investigate its probability of dafault within default_prediction length
# FeaturesForPrediction - as our features, we want to consider only informations which we learn about a client in the observation_period
@transformation(
    get_new_features, get_widget_value("default_days"), get_widget_value("default_prediction"), get_widget_value("observation_period")
)
def get_target(df: DataFrame, default_days, default_prediction, observation_period):
    w = Window.partitionBy("LoanID").orderBy("Date")
    return (
        df.withColumn("nextPaymentDiff", f.datediff("Date", f.lag("Date").over(w)))
        .fillna(0, subset=["nextPaymentDiff"])
        .withColumn("DaysFromStart", f.datediff("Date", "LoanDate"))
        .filter(f.col("DaysFromStart") <= default_prediction)
        .withColumn("label", f.when(f.col("nextPaymentDiff") < default_days, 0).otherwise(1))
        .withColumn("FeaturesForPrediction", f.when(f.col("DaysFromStart") <= observation_period, 1).otherwise(0))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter only observations which have not defaulted in the observation period

# COMMAND ----------


@transformation(get_target, get_widget_value("observation_period"))
def get_loans_with_immediate_default(df: DataFrame, observation_period):
    return df.filter(f.col("label") == 1).filter(f.col("DaysFromStart") < observation_period).select("LoanID").distinct()


# COMMAND ----------


@transformation(get_target, get_loans_with_immediate_default)
def get_target_without_shortterm_default(df_target: DataFrame, df_loans_with_immediate_default: DataFrame):
    return (
        df_target.join(df_loans_with_immediate_default, on="LoanID", how="left_anti")
        .withColumn("label", f.max("label").over(Window.partitionBy("LoanID")))
        .filter(f.col("FeaturesForPrediction") == 1)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Select only one row for each Loan, deal with null values

# COMMAND ----------


@transformation(get_target_without_shortterm_default)
def get_unique_observations(df: DataFrame):
    return (
        df.drop(
            "FeaturesForPrediction",
            "DaysFromStart",
            "nextPaymentDiff",
            "DaysToFirstPayment",
            "Date",
            "LoanDate",
            "UserName",
            "MaturityDate_Last",
            "DefaultDate",
        )
        .dropDuplicates()
        .fillna(
            0,
            subset=[
                "VerificationType",
                "LanguageCode",
                "Gender",
                "Amount",
                "Interest",
                "LoanDuration",
                "MonthlyPayment",
                "IncomeTotal",
                "LiabilitiesTotal",
                "RefinanceLiabilities",
                "DebtToIncome",
                "FreeCash",
                "NoOfPreviousLoansBeforeLoan",
                "PreviousRepaymentsBeforeLoan",
                "AmountOfPreviousLoansBeforeLoan",
                "AmountNotGranted",
            ],
        )
        .fillna("X", subset=["Country", "EmploymentDurationCurrentEmployer", "WorkExperience", "AgeGroup"])
        .fillna(
            -1,
            subset=["UseOfLoan", "Education", "MaritalStatus", "NrOfDependants", "EmploymentStatus", "OccupationArea", "HomeOwnershipType"],
        )
        .fillna("-1", subset=["NrOfDependants", "EmploymentDurationCurrentEmployer"])
        .withColumn("NewCreditCustomer", f.col("NewCreditCustomer").cast("int"))
        .withColumn("ActiveScheduleFirstPaymentReached", f.col("ActiveScheduleFirstPaymentReached").cast("int"))
    )


# COMMAND ----------

numeric_features = [
    "NewCreditCustomer",
    "VerificationType",
    "Amount",
    "Interest",
    "LoanDuration",
    "MonthlyPayment",
    "IncomeTotal",
    "LiabilitiesTotal",
    "RefinanceLiabilities",
    "DebtToIncome",
    "FreeCash",
    "NoOfPreviousLoansBeforeLoan",
    "PreviousRepaymentsBeforeLoan",
    "AmountOfPreviousLoansBeforeLoan",
    "MaturityDateDelay",
    "AmountNotGranted",
]

categorical_features = [
    "LanguageCode",
    "Gender",
    "Country",
    "UseOfLoan",
    "Education",
    "MaritalStatus",
    "NrOfDependants",
    "EmploymentStatus",
    "EmploymentDurationCurrentEmployer",
    "WorkExperience",
    "OccupationArea",
    "HomeOwnershipType",
    "ActiveScheduleFirstPaymentReached",
    "AgeGroup",
]

# COMMAND ----------


@transformation(get_unique_observations)
def cast_numeric_features_to_double(df: DataFrame):
    return df.select(*(f.col(c).cast("double").alias(c) if c in numeric_features else f.col(c) for c in df.columns))


# COMMAND ----------


@transformation(cast_numeric_features_to_double, get_widget_value("run_date"))
def append_run_date(df: DataFrame, run_date):
    return df.withColumn("run_date", f.lit(dt.datetime.strptime(run_date, "%Y-%m-%d")))


# COMMAND ----------


@transformation(append_run_date)
@loan_feature(
    ("Gender", "0 Male 1 Woman 2 Undefined"),
    ("AgeGroup", "Borrowers age group"),
    ("Country", "Residency of the borrower"),
    ("LanguageCode", "1 Estonian 2 English 3 Russian 4 Finnish 5 German 6 Spanish 9 Slovakian"),
    ("Education", "1 Primary education 2 Basic education 3 Vocational education 4 Secondary education 5 Higher education"),
    ("MaritalStatus", "1 Married 2 Cohabitant 3 Single 4 Divorced 5 Widow"),
    ("NrOfDependants", "Number of children or other dependants"),
    ("EmploymentStatus", "1 Unemployed 2 Partially employed 3 Fully employed 4 Self-employed 5 Entrepreneur 6 Retiree"),
    ("EmploymentDurationCurrentEmployer", "Employment time with the current employer"),
    ("WorkExperience", "Borrower's overall work experience in years"),
    ("OccupationArea", "Borrower's profession"),
    ("HomeOwnershipType", "Borrower's home ownership type (Owner, Tenant, Mortgage, ...)"),
    category="personal",
)
def customer_personal_features(df: DataFrame):
    return df.select(
        "LoanId",
        "run_date",
        "Gender",
        "AgeGroup",
        "Country",
        "LanguageCode",
        "Education",
        "MaritalStatus",
        "NrOfDependants",
        "EmploymentStatus",
        "EmploymentDurationCurrentEmployer",
        "WorkExperience",
        "OccupationArea",
        "HomeOwnershipType",
    )


# COMMAND ----------


@transformation(append_run_date)
@loan_feature(
    ("IncomeTotal", "Total Income of borrower"),
    ("LiabilitiesTotal", "Total monthly liabilities"),
    ("RefinanceLiabilities", "The total amount of liabilities after refinancing"),
    ("DebtToIncome", "Ratio of borrower's monthly gross income that goes toward paying loans"),
    ("FreeCash", "Discretionary income after monthly liabilities"),
    category="finance",
)
def customer_financial_features(df: DataFrame):
    return df.select(
        "LoanId",
        "run_date",
        "IncomeTotal",
        "LiabilitiesTotal",
        "RefinanceLiabilities",
        "DebtToIncome",
        "FreeCash",
    )


# COMMAND ----------


@transformation(append_run_date)
@loan_feature(
    ("NewCreditCustomer", "True if Customer had at least 3 months of credit history in Bondora, otherwise False"),
    ("VerificationType", "0 Not set 1 Income unverified 2 cross-referenced by phone 3 Income verified 4 Income and expenses verified"),
    ("Interest", "Maximum interest rate accepted in the loans application"),
    ("LoanDuration", "Current loans duration in months"),
    ("UseOfLoan", "Real estate, Vehicle, Business, Education, Travel, ..."),
    ("MonthlyPayment", "Estimated amount the borrower has to pay every month"),
    ("ActiveScheduleFirstPaymentReached", "Whether the first payment date has been reached according to the active schedule"),
    ("NoOfPreviousLoansBeforeLoan", "Number of previous loans"),
    ("PreviousRepaymentsBeforeLoan", "How much the borrower had repaid before the loans"),
    ("AmountOfPreviousLoansBeforeLoan", "Value of previous loans"),
    ("AmountNotGranted", "Difference between applied amount and granted amount"),
    ("Amount", "Amount the borrower received on the Primary Market. This is the principal balance of your purchase from Secondary Market"),
    ("MaturityDateDelay", "Maturity date delay"),
    ("label", "1 Defaulted 0 Not Defaulted"),
    category="loans",
)
def customer_loan_features(df: DataFrame):
    return df.select(
        "LoanId",
        "run_date",
        "NewCreditCustomer",
        "VerificationType",
        "Interest",
        "LoanDuration",
        "UseOfLoan",
        "MonthlyPayment",
        "ActiveScheduleFirstPaymentReached",
        "NoOfPreviousLoansBeforeLoan",
        "PreviousRepaymentsBeforeLoan",
        "AmountOfPreviousLoansBeforeLoan",
        "AmountNotGranted",
        "Amount",
        "MaturityDateDelay",
        "label",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$../models/model_training_with_daipe_ml">sample notebook #10</a>
