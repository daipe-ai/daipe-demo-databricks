from pyspark.sql import types as t


class tbl_repayments:  # noqa: N801
    db = "silver"
    fields = [
        t.StructField("RepaymentID", t.LongType(), True),
        t.StructField("ReportAsOfEOD", t.DateType(), True),
        t.StructField("LoanID", t.StringType(), True),
        t.StructField("Date", t.DateType(), True),
        t.StructField("PrincipalRepayment", t.DoubleType(), True),
        t.StructField("InterestRepayment", t.DoubleType(), True),
        t.StructField("LateFeesRepayment", t.DoubleType(), True),
    ]
    primary_key = "RepaymentID"
    # partition_by = "Date" #---takes a very long time
