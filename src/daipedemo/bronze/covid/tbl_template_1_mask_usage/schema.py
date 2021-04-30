import pyspark.sql.types as t
from datalakebundle.table.schema.DeltaTableSchema import DeltaTableSchema


def get_schema():
    return DeltaTableSchema(
        [
            t.StructField("COUNTYFP", t.IntegerType(), True),
            t.StructField("NEVER", t.DoubleType(), True),
            t.StructField("RARELY", t.DoubleType(), True),
            t.StructField("SOMETIMES", t.DoubleType(), True),
            t.StructField("FREQUENTLY", t.DoubleType(), True),
            t.StructField("ALWAYS", t.DoubleType(), True),
            t.StructField("INSERT_TS", t.TimestampType(), False),
        ],
        primary_key="COUNTYFP",
    )
