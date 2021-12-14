import unittest
import warnings
from pyspark.sql import SparkSession


class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark = SparkSession.builder.master("local[*]").appName("PySparkTest").getOrCreate()
        sc = spark.sparkContext

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

        cls.spark = spark
        cls.sc = sc

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
