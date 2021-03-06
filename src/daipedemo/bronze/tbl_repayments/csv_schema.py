import pyspark.sql.types as t


def get_schema():
    return t.StructType(
        [
            t.StructField("ReportAsOfEOD", t.StringType(), True),
            t.StructField("LoanID", t.StringType(), True),
            t.StructField("Date", t.StringType(), True),
            t.StructField("PrincipalRepayment", t.DoubleType(), True),
            t.StructField("InterestRepayment", t.DoubleType(), True),
            t.StructField("LateFeesRepayment", t.DoubleType(), True),
        ]
    )
