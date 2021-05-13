from datalakebundle.table.schema.TableSchema import TableSchema
from pyspark.sql import types as t


table_schema = TableSchema(
    "silver.tbl_repayments",
    [
        t.StructField("RepaymentID", t.LongType(), True),
        t.StructField("ReportAsOfEOD", t.DateType(), True),
        t.StructField("LoanID", t.StringType(), True),
        t.StructField("Date", t.DateType(), True),
        t.StructField("PrincipalRepayment", t.DoubleType(), True),
        t.StructField("InterestRepayment", t.DoubleType(), True),
        t.StructField("LateFeesRepayment", t.DoubleType(), True),
    ],
    "RepaymentID",
    # partition_by = "Date" #---takes a very long time
)
