# Databricks notebook source
# MAGIC %md
# MAGIC # #9 Creating a target for ML
# MAGIC ## Gold layer
# MAGIC
# MAGIC Return to <a href="$../_index">index page</a>
# MAGIC
# MAGIC In this notebook we preprocess the data for modelling purposes.

# COMMAND ----------

# MAGIC %run ../app/install_master_package

# COMMAND ----------

import numpy as np

from pyspark.ml.feature import Bucketizer
from pyspark.sql import DataFrame, functions as f, Window, types as t
from datalakebundle.imports import *
from daipecore.widgets.Widgets import Widgets
from daipecore.widgets.get_widget_value import get_widget_value

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets for parameters

# COMMAND ----------


@notebook_function()
def create_input_widgets(widgets: Widgets):
    widgets.add_text("observation_period", "90", "Observation period")
    widgets.add_text("default_days", "90", "Default days")
    widgets.add_text("default_prediction", "365", "Default prediction")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Define lengths of time windows

# COMMAND ----------


@transformation(read_table("silver.tbl_joined_loans_and_repayments"), display=False)
def read_joined_loans_and_repayments(df: DataFrame):
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataset columns explanations
# MAGIC * LoanId - ID of specific loan (one person might have more than one loan)
# MAGIC * LoanDate - Date when the loan was issued
# MAGIC * UserName - The user name generated by the system for the borrower
# MAGIC * NewCreditCustomer - Did the customer have prior credit history in Bondora? False = Customer had at least 3 months of credit history in Bondora, True = No prior credit history in Bondora
# MAGIC * ContractEndDate - The date when the loan contract ended
# MAGIC * VerificationType - Method used for loan application data verification 0 Not set 1 Income unverified 2 Income unverified, cross-referenced by phone 3 Income verified 4 Income and expenses verified
# MAGIC * LanguageCode - 1 Estonian 2 English 3 Russian 4 Finnish 5 German 6 Spanish 9 Slovakian
# MAGIC * Age - The age of the borrower when signing the loan application
# MAGIC * Gender - 0 Male 1 Woman 2 Undefined
# MAGIC * Country - Residency of the borrower
# MAGIC * Amount - Amount the borrower received on the Primary Market. This is the principal balance of your purchase from Secondary Market
# MAGIC * AppliedAmount - The amount borrower applied for originally
# MAGIC * Interest - Maximum interest rate accepted in the loan application
# MAGIC * LoanDuration - Current loan duration in months
# MAGIC * MonthlyPayment - Estimated amount the borrower has to pay every month
# MAGIC * Date - day of record (payment)
# MAGIC * UseOfLoan - 0 Loan consolidation 1 Real estate 2 Home improvement 3 Business 4 Education 5 Travel 6 Vehicle 7 Other 8 Health 101 Working capital financing 102 Purchase of machinery equipment 103 Renovation of real estate 104 Accounts receivable financing 105 Acquisition of means of transport 106 Construction finance 107 Acquisition of stocks 108 Acquisition of real estate 109 Guaranteeing obligation 110 Other business All codes in format 1XX are for business loans that are not supported since October 2012
# MAGIC * Education - 1 Primary education 2 Basic education 3 Vocational education 4 Secondary education 5 Higher education
# MAGIC * MaritalStatus - 1 Married 2 Cohabitant 3 Single 4 Divorced 5 Widow
# MAGIC * NrOfDependants - Number of children or other dependants
# MAGIC * EmploymentStatus - 1 Unemployed 2 Partially employed 3 Fully employed 4 Self-employed 5 Entrepreneur 6 Retiree
# MAGIC * EmploymentDurationCurrentEmployer - Employment time with the current employer
# MAGIC * WorkExperience - Borrower's overall work experience in years
# MAGIC * OccupationArea - 1 Other 2 Mining 3 Processing 4 Energy 5 Utilities 6 Construction 7 Retail and wholesale 8 Transport and warehousing 9 Hospitality and catering 10 Info and telecom 11 Finance and insurance 12 Real-estate 13 Research 14 Administrative 15 Civil service & military 16 Education 17 Healthcare and social help 18 Art and entertainment 19 Agriculture, forestry and fishing
# MAGIC * HomeOwnershipType - 0 Homeless 1 Owner 2 Living with parents 3 Tenant, pre-furnished property 4 Tenant, unfurnished property 5 Council house 6 Joint tenant 7 Joint ownership 8 Mortgage 9 Owner with encumbrance 10 Other
# MAGIC * IncomeTotal - Total Income of borrower
# MAGIC * LiabilitiesTotal - Total monthly liabilities
# MAGIC * FirstPaymentDate - First payment date according to initial loan schedule
# MAGIC * RefinanceLiabilities - The total amount of liabilities after refinancing
# MAGIC * DebtToIncome - Ratio of borrower's monthly gross income that goes toward paying loans
# MAGIC * FreeCash - Discretionary income after monthly liabilities
# MAGIC * ActiveScheduleFirstPaymentReached - Whether the first payment date has been reached according to the active schedule
# MAGIC * DefaultDate - The date when loan went into defaulted state and collection process was started
# MAGIC * DebtOccuredOn - The date when Principal Debt occurred
# MAGIC * NoOfPreviousLoansBeforeLoan - Number of previous loans
# MAGIC * PreviousRepaymentsBeforeLoan - How much the borrower had repaid before the loan
# MAGIC * AmountOfPreviousLoansBeforeLoan - Value of previous loans

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preselect columns which might be useful as features

# COMMAND ----------


@transformation(read_joined_loans_and_repayments, display=True)
def select_columns(df: DataFrame):
    columns = [
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
    ]
    return df.select(columns)


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
# MAGIC * DaysToFirstPayment - difference between days from taking the loan and the first payment made

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create target

# COMMAND ----------

# nextPaymentDiff - how many days was between two payments, if more than default_days, then we cansider this loan as defaulted
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


@transformation(get_target_without_shortterm_default, display=True)
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


def get_schema():
    return TableSchema(
        [
            t.StructField("LoanId", t.StringType()),
            t.StructField("NewCreditCustomer", t.DoubleType()),
            t.StructField("VerificationType", t.DoubleType()),
            t.StructField("LanguageCode", t.IntegerType()),
            t.StructField("Gender", t.IntegerType()),
            t.StructField("Country", t.StringType()),
            t.StructField("Amount", t.DoubleType()),
            t.StructField("Interest", t.DoubleType()),
            t.StructField("LoanDuration", t.DoubleType()),
            t.StructField("MonthlyPayment", t.DoubleType()),
            t.StructField("UseOfLoan", t.IntegerType()),
            t.StructField("Education", t.IntegerType()),
            t.StructField("MaritalStatus", t.IntegerType()),
            t.StructField("NrOfDependants", t.StringType()),
            t.StructField("EmploymentStatus", t.IntegerType()),
            t.StructField("EmploymentDurationCurrentEmployer", t.StringType()),
            t.StructField("WorkExperience", t.StringType()),
            t.StructField("OccupationArea", t.IntegerType()),
            t.StructField("HomeOwnershipType", t.IntegerType()),
            t.StructField("IncomeTotal", t.DoubleType()),
            t.StructField("LiabilitiesTotal", t.DoubleType()),
            t.StructField("RefinanceLiabilities", t.DoubleType()),
            t.StructField("DebtToIncome", t.DoubleType()),
            t.StructField("FreeCash", t.DoubleType()),
            t.StructField("ActiveScheduleFirstPaymentReached", t.IntegerType()),
            t.StructField("NoOfPreviousLoansBeforeLoan", t.DoubleType()),
            t.StructField("PreviousRepaymentsBeforeLoan", t.DoubleType()),
            t.StructField("AmountOfPreviousLoansBeforeLoan", t.DoubleType()),
            t.StructField("AgeGroup", t.StringType()),
            t.StructField("MaturityDateDelay", t.DoubleType()),
            t.StructField("AmountNotGranted", t.DoubleType()),
            t.StructField("label", t.IntegerType()),
        ],
        # primary_key="", # INSERT PRIMARY KEY(s) HERE (OPTIONAL)
        # partition_by="" # INSERT PARTITION KEY(s) HERE (OPTIONAL)
        # tbl_properties={} # INSERT TBLPROPERTIES HERE (OPTIONAL)
    )


# COMMAND ----------


@transformation(get_unique_observations)
@table_overwrite("gold.tbl_target_features", get_schema())
def save_unique_observations_parsed(df: DataFrame):
    return df.select(*(f.col(c).cast("double").alias(c) if c in numeric_features else f.col(c) for c in df.columns))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue to the <a href="$./model_training_with_daipe_ml">sample notebook #10</a>
