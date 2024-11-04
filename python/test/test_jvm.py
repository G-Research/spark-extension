#  Copyright 2024 G-Research
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from unittest import skipIf, skipUnless

from pyspark.sql.functions import sum

from gresearch.spark import _get_jvm, \
    dotnet_ticks_to_timestamp, dotnet_ticks_to_unix_epoch, dotnet_ticks_to_unix_epoch_nanos, \
    timestamp_to_dotnet_ticks, unix_epoch_to_dotnet_ticks, unix_epoch_nanos_to_dotnet_ticks, \
    histogram, job_description, append_description
from gresearch.spark.diff import *
from gresearch.spark.parquet import *
from spark_common import SparkTest

EXPECTED_UNSUPPORTED_MESSAGE = "This feature is not supported for Spark Connect. Please use a classic Spark client. " \
                               "https://github.com/G-Research/spark-extension#spark-connect-server"


class PackageTest(SparkTest):
    df = None

    @classmethod
    def setUpClass(cls):
        super(PackageTest, cls).setUpClass()
        cls.df = cls.spark.createDataFrame([(1, "one"), (2, "two"), (3, "three")], ["id", "value"])

    @skipIf(SparkTest.is_spark_connect, "Spark classic client tests")
    def test_get_jvm_classic(self):
        for obj in [self.spark, self.spark.sparkContext, self.df, self.spark.read]:
            with self.subTest(type(obj).__name__):
                self.assertIsNotNone(_get_jvm(obj))

        with self.subTest("Unsupported"):
            with self.assertRaises(RuntimeError) as e:
                _get_jvm(object())
            self.assertEqual(("Unsupported class: <class 'object'>", ), e.exception.args)

    @skipUnless(SparkTest.is_spark_connect, "Spark connect client tests")
    def test_get_jvm_connect(self):
        for obj in [self.spark, self.df, self.spark.read]:
            with self.subTest(type(obj).__name__):
                with self.assertRaises(RuntimeError) as e:
                    _get_jvm(obj)
                self.assertEqual((EXPECTED_UNSUPPORTED_MESSAGE, ), e.exception.args)

        with self.subTest("Unsupported"):
            with self.assertRaises(RuntimeError) as e:
                _get_jvm(object())
            self.assertEqual(("Unsupported class: <class 'object'>", ), e.exception.args)

    @skipIf(SparkTest.is_spark_connect, "Spark classic client tests")
    def test_get_jvm_check_java_pkg_is_installed(self):
        from gresearch import spark

        is_installed = spark._java_pkg_is_installed

        try:
            spark._java_pkg_is_installed = False
            with self.assertRaises(RuntimeError) as e:
                _get_jvm(self.spark)
            self.assertEqual(("Java / Scala package not found! You need to add the Maven spark-extension package "
                              "to your PySpark environment: https://github.com/G-Research/spark-extension#python", ), e.exception.args)
        finally:
            spark._java_pkg_is_installed = is_installed

    @skipUnless(SparkTest.is_spark_connect, "Spark connect client tests")
    def test_dotnet_ticks(self):
        for label, func in {
            'dotnet_ticks_to_timestamp': dotnet_ticks_to_timestamp,
            'dotnet_ticks_to_unix_epoch': dotnet_ticks_to_unix_epoch,
            'dotnet_ticks_to_unix_epoch_nanos': dotnet_ticks_to_unix_epoch_nanos,
            'timestamp_to_dotnet_ticks': timestamp_to_dotnet_ticks,
            'unix_epoch_to_dotnet_ticks': unix_epoch_to_dotnet_ticks,
            'unix_epoch_nanos_to_dotnet_ticks': unix_epoch_nanos_to_dotnet_ticks,
        }.items():
            with self.subTest(label):
                with self.assertRaises(RuntimeError) as e:
                    func("id")
                self.assertEqual(("This method must be called inside an active Spark session", ), e.exception.args)

    @skipUnless(SparkTest.is_spark_connect, "Spark connect client tests")
    def test_histogram(self):
        with self.assertRaises(RuntimeError) as e:
            self.df.histogram([1, 10, 100], "bin", sum)
        self.assertEqual((EXPECTED_UNSUPPORTED_MESSAGE, ), e.exception.args)

    @skipUnless(SparkTest.is_spark_connect, "Spark connect client tests")
    def test_with_row_numbers(self):
        with self.assertRaises(RuntimeError) as e:
            self.df.with_row_numbers()
        self.assertEqual((EXPECTED_UNSUPPORTED_MESSAGE, ), e.exception.args)

    @skipUnless(SparkTest.is_spark_connect, "Spark connect client tests")
    def test_job_description(self):
        with self.assertRaises(RuntimeError) as e:
            with job_description("job description"):
                pass
        self.assertEqual(("This method must be called inside an active Spark session", ), e.exception.args)

        with self.assertRaises(RuntimeError) as e:
            with append_description("job description"):
                pass
        self.assertEqual(("This method must be called inside an active Spark session", ), e.exception.args)

    @skipUnless(SparkTest.is_spark_connect, "Spark connect client tests")
    def test_create_temp_dir(self):
        with self.assertRaises(RuntimeError) as e:
            self.spark.create_temporary_dir("prefix")
        self.assertEqual((EXPECTED_UNSUPPORTED_MESSAGE, ), e.exception.args)

    @skipUnless(SparkTest.is_spark_connect, "Spark connect client tests")
    def test_install_pip_package(self):
        with self.assertRaises(RuntimeError) as e:
            self.spark.install_pip_package("pytest")
        self.assertEqual((EXPECTED_UNSUPPORTED_MESSAGE, ), e.exception.args)

    @skipUnless(SparkTest.is_spark_connect, "Spark connect client tests")
    def test_install_poetry_project(self):
        with self.assertRaises(RuntimeError) as e:
            self.spark.install_poetry_project("./poetry-project")
        self.assertEqual((EXPECTED_UNSUPPORTED_MESSAGE, ), e.exception.args)

    @skipUnless(SparkTest.is_spark_connect, "Spark connect client tests")
    def test_parquet(self):
        for label, func in {
            'parquet_metadata': lambda dr: dr.parquet_metadata("file.parquet"),
            'parquet_schema': lambda dr: dr.parquet_schema("file.parquet"),
            'parquet_blocks': lambda dr: dr.parquet_blocks("file.parquet"),
            'parquet_block_columns': lambda dr: dr.parquet_block_columns("file.parquet"),
            'parquet_partitions': lambda dr: dr.parquet_partitions("file.parquet"),
        }.items():
            with self.subTest(label):
                with self.assertRaises(RuntimeError) as e:
                    func(self.spark.read)
                self.assertEqual((EXPECTED_UNSUPPORTED_MESSAGE, ), e.exception.args)
