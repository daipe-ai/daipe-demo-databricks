# Databricks notebook source
# MAGIC %md
# MAGIC # Daipe ML overview
# MAGIC ## Gold layer
# MAGIC
# MAGIC ###Daipe ML (datasciencefunctions) is a python package which simplifies the datascience lifecycle of machine learning models in python (scikit-learn) and pySpark.
# MAGIC
# MAGIC ###It greatly helps in the following part of a datascientist's work:
# MAGIC * Data exploration
# MAGIC * Feature selection
# MAGIC * Model training
# MAGIC * Model evaluation
# MAGIC * Model productionization
# MAGIC
# MAGIC #####Daipe ML is well-documented both in code (docstrings) and in a html form. It also comes with several tutorial notebooks which demonstrate the use of all its features in details directly in useable code.
# MAGIC
# MAGIC Return to <a href="$../_index">index page</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ###We will demonstrate the features of Daipe ML  using an example vehicle loan default prediction usecase where we use the following information to predict whether a loan will or will not default:
# MAGIC * Loanee Information (Demographic data like age, income, Identity proof etc.)
# MAGIC * Loan Information (Disbursal details, amount, EMI, loan to value ratio etc.)
# MAGIC * Bureau data & history (Bureau score, number of active accounts, the status of other loans, credit history etc.)

# COMMAND ----------

# MAGIC %run ../../app/install_master_package

# COMMAND ----------

import numpy as np
import pyspark.sql.functions as f
import mlflow
import mlflow.spark
import datasciencefunctions as ds
from datasciencefunctions.data_exploration import plot_feature_hist_with_binary_target
from datasciencefunctions.feature_selection import feature_selection_merits
from datasciencefunctions.supervised import supervised_wrapper, lift_curve
from datalakebundle.imports import transformation
from featurestorebundle.feature.FeatureStore import FeatureStore
from databricks import feature_store
from databricks.feature_store import FeatureLookup
from pprint import pprint

# COMMAND ----------

dbx_feature_store = feature_store.FeatureStoreClient()

# COMMAND ----------

# MAGIC %md #0. Loading data

# COMMAND ----------

# MAGIC %md
# MAGIC * LoanId - ID of specific loan (one person might have more than one loan)
# MAGIC * NewCreditCustomer - Did the customer have prior credit history in Bondora? False = Customer had at least 3 months of credit history in Bondora, True = No prior credit history in Bondora
# MAGIC * VerificationType - Method used for loan application data verification 0 Not set 1 Income unverified 2 Income unverified, cross-referenced by phone 3 Income verified 4 Income and expenses verified
# MAGIC * LanguageCode - 1 Estonian 2 English 3 Russian 4 Finnish 5 German 6 Spanish 9 Slovakian
# MAGIC * Gender - 0 Male 1 Woman 2 Undefined
# MAGIC * Country - Residency of the borrower
# MAGIC * Amount - Amount the borrower received on the Primary Market. This is the principal balance of your purchase from Secondary Market
# MAGIC * Interest - Maximum interest rate accepted in the loan application
# MAGIC * LoanDuration - Current loan duration in months
# MAGIC * MonthlyPayment - Estimated amount the borrower has to pay every month
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
# MAGIC * NoOfPreviousLoansBeforeLoan - Number of previous loans
# MAGIC * PreviousRepaymentsBeforeLoan - How much the borrower had repaid before the loan
# MAGIC * AmountOfPreviousLoansBeforeLoan - Value of previous loans
# MAGIC * AgeGroup - categorical age groups
# MAGIC * MaturityDateDelay - difference between originally scheduled maturity date and the rescheduled one based on first payment delay
# MAGIC * AmountNotGranted - difference between amount asked for and granted

# COMMAND ----------


@transformation()
def load_feature_store(feature_store: FeatureStore):
    return feature_store.get_latest("loan")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Daipe ML can work with both pandas and pySpark dataframes and automatically recognizes which tools to use for each framework

# COMMAND ----------

df = load_feature_store.result
df_pandas = df.toPandas()

# COMMAND ----------

numeric_features = [
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
    "NewCreditCustomer",
    "VerificationType",
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

# MAGIC %md #1. Data exploration
# MAGIC
# MAGIC ###We can use Daipe ML to obtain a plot which gives us a good overview of each feature and its relationship with the target.
# MAGIC It visualises:
# MAGIC * the frequencies of values of all features (numeric features are binned) as well as their basic statistics (min, max, mean)
# MAGIC * how the target value depends on the value of each individual feature

# COMMAND ----------

plot_feature_hist_with_binary_target(
    df=df,
    target_col="label",
    cat_cols=[
        "EmploymentDurationCurrentEmployer",
        "WorkExperience",
        "AgeGroup",
    ],
)

# COMMAND ----------

# MAGIC %md #2. Feature selection
# MAGIC
# MAGIC #### During model development, we often want to reduce the number of features used by our models to increase training speed, interpretability as well as accuracy of our models.
# MAGIC
# MAGIC ####To make this easier, we use our custom function `feature_selection_merits` to select the most predictive features which are at the same time the least similar to each other (thus providing the maximum information with minimum noise).
# MAGIC
# MAGIC Our approach has several advantages over other feature selection apprroaches:
# MAGIC * it is model agnostic, so its outcome does not depend on the specific model architecture used and is valid for all model architectures
# MAGIC * it is not based on model training and therefore it is significantly faster than feature selection approaches which are
# MAGIC * it explores the space of possible feature sets efficiently, using a greedy algorithm with pruning to arrive at the most promising feature sets
# MAGIC * it provides the user with the history of the feature set search in case the user wants to tweak the suggested feature set for business or other usecase specific reasons

# COMMAND ----------

df_corr = df_pandas[numeric_features + ["label"]].corr()
df_corr

# COMMAND ----------

# DBTITLE 1,target/label correlation with features
corr_target = df_corr.loc[["label"], df_corr.columns != "label"].abs()
corr_target

# COMMAND ----------

# DBTITLE 1,correlation between features excluding target/label
corr_features = df_corr.loc[df_corr.columns != "label", df_corr.index != "label"].abs()
corr_features

# COMMAND ----------

selected_features, feature_selection_history = feature_selection_merits(
    features_correlations=corr_features,
    target_correlations=corr_target,
    algorithm="forward",
    max_iter=30,
    best_n=3,
)

selected_features

# COMMAND ----------

# DBTITLE 1,finally, we select the resulting best feature set for modelling
feature_lookups = [
    FeatureLookup(
        table_name="dev_feature_store.features_loan_latest",
        feature_name=feature,
        lookup_key=["LoanId"],
    )
    for feature in selected_features
]

training_set = dbx_feature_store.create_training_set(
    df=df.select("LoanId", "label"), feature_lookups=feature_lookups, label="label", exclude_columns=["LoanId"]
)

df_ml_spark = training_set.load_df()
df_ml_pandas = df_ml_spark.toPandas()

# COMMAND ----------

# MAGIC %md # 3. Model training, evaluation and productionization
# MAGIC
# MAGIC ## `supervised_wrapper`
# MAGIC ####Now, we have reached the modelling/tuning phase of the datascience lifecycle. This is where we speed up the process and drastically reduce the amount of coding necessary by using the `supervised_wrapper` function of Daipe ML. This function is a wrapper of several other Daipe ML functions which handle the following:
# MAGIC * a train/test split of data
# MAGIC * feature indexing, encoding and scaling, (optionally) automatically determining categorical and numeric features
# MAGIC * fitting the data preparation pipeline
# MAGIC * training the model using cross-validation and hyperopt (or paramgrid) hyperparameter optimization
# MAGIC * calculating model performance metrics (accuracy, precision, lift, area under ROC curve, ... ) on the test set
# MAGIC * loging the model, its performance metrics, and a complete pipeline including data preparation (so that the model is ready for deployment in production) to MLFlow in a structured way
# MAGIC
# MAGIC ####Note: Each of these functionalities can also be used separately and combined with custom functions which handle the data science pipeline in a specific way.

# COMMAND ----------

# train test split, hyperparameter space, metrics to log and evaluate model, mlflow is done automatically
train_df, test_df, model_summary = supervised_wrapper(
    df=df_ml_spark,
    model_type=ds.MlModel.spark_random_forest_classifier,
    use_mlflow=False,
    label_col="label",
    params_fit_model={"max_evals": 2},
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###If we want to integrate with Databricks Feature store we need to log and register model manually

# COMMAND ----------

with mlflow.start_run(run_name="Random Forest Classifier - Loan Default Prediction") as run:
    dbx_feature_store.log_model(
        model_summary["models"]["pipeline"],
        model_name,
        flavor=mlflow.spark,
        training_set=training_set,
    )

    ds.supervised.log_model_summary(model_summary)

    run_id = run.info.run_id
    model_uri = f"runs:/{run_id}/{model_name}"
    model_details = mlflow.register_model(model_uri=model_uri, name=model_name)
