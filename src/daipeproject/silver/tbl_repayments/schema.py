from datalakebundle.table.schema.TableSchema import TableSchema
from pyspark.sql import types as t


def get_schema():
    return TableSchema(
        [
            t.StructField("ReportAsOfEOD", t.DateType(), True),
            t.StructField("LoanID", t.StringType(), True),
            t.StructField("Date", t.DateType(), True),
            t.StructField("PrincipalRepayment", t.DoubleType(), True),
            t.StructField("InterestRepayment", t.DoubleType(), True),
            t.StructField("LateFeesRepayment", t.DoubleType(), True),
        ],
        primary_key=["LoanID", "Date"],
        # partition_by = "Date" #---takes a very long time
    )
