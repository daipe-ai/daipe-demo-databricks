import unittest
from daipedemo.test.PySparkTestCase import PySparkTestCase
from daipedemo.bronze.tbl_repayments.tbl_repayments import load_csv_and_make_transformation


class SimpleTestCase(PySparkTestCase):
    def test_load_csv_and_make_transformation(self):
        input_df = self.spark.createDataFrame(data=[[1, 'a'], [2, 'b']], schema=['c1', 'c2'])
        output_df = load_csv_and_make_transformation(input_df)
        expected_df = self.spark.createDataFrame(data=[[1, 'a', 'dummy'], [2, 'b', 'dummy']], schema=['c1', 'c2', 'dummy_col'])

        self.assertEqual(output_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main()
