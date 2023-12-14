#  Copyright 2023 G-Research
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
import datetime
import os
from decimal import Decimal
from subprocess import CalledProcessError
from unittest import skipUnless, skipIf

from pyspark import __version__
from pyspark.sql import Row
from pyspark.sql.functions import col, count

from gresearch.spark import dotnet_ticks_to_timestamp, dotnet_ticks_to_unix_epoch, dotnet_ticks_to_unix_epoch_nanos, \
    timestamp_to_dotnet_ticks, unix_epoch_to_dotnet_ticks, unix_epoch_nanos_to_dotnet_ticks, count_null
from spark_common import SparkTest

POETRY_PYTHON_ENV = "POETRY_PYTHON"
RICH_SOURCES_ENV = "RICH_SOURCES"


class PackageTest(SparkTest):

    @classmethod
    def setUpClass(cls):
        super(PackageTest, cls).setUpClass()

        cls.ticks = cls.spark.createDataFrame([
            (1, 599266080000000000),
            (2, 621355968000000000),
            (3, 638155413748959308),
            (4, 638155413748959309),
            (5, 638155413748959310),
            (6, 713589688368547758),
            (7, 946723967999999999)
        ], ['id', 'tick'])

        cls.timestamps = cls.spark.createDataFrame([
            (1, datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc).astimezone().replace(tzinfo=None)),
            (2, datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc).astimezone().replace(tzinfo=None)),
            (3, datetime.datetime(2023, 3, 27, 19, 16, 14, 895930, datetime.timezone.utc).astimezone().replace(tzinfo=None)),
            (4, datetime.datetime(2023, 3, 27, 19, 16, 14, 895930, datetime.timezone.utc).astimezone().replace(tzinfo=None)),
            (5, datetime.datetime(2023, 3, 27, 19, 16, 14, 895931, datetime.timezone.utc).astimezone().replace(tzinfo=None)),
            (6, datetime.datetime(2262, 4, 11, 23, 47, 16, 854775, datetime.timezone.utc).astimezone().replace(tzinfo=None)),
            (7, datetime.datetime(3001, 1, 19, 7, 59, 59, 999999, datetime.timezone.utc).astimezone().replace(tzinfo=None))
        ], ['id', 'timestamp'])

        cls.unix = cls.spark.createDataFrame([
            (1, Decimal('-2208988800.000000000')),
            (2, Decimal('0E-9')),
            (3, Decimal('1679944574.895930800')),
            (4, Decimal('1679944574.895930900')),
            (5, Decimal('1679944574.895931000')),
            (6, Decimal('9223372036.854775800')),
            (7, Decimal('32536799999.999999900'))
        ], ['id', 'unix'])

        cls.unix_nanos = cls.spark.createDataFrame([
            (1, -2208988800000000000),
            (2, 0),
            (3, 1679944574895930800),
            (4, 1679944574895930900),
            (5, 1679944574895931000),
            (6, 9223372036854775800),
            (7, None)
        ], ['id', 'unix_nanos'])

        cls.ticks_from_timestamp = cls.spark.createDataFrame([
            (1, 599266080000000000),
            (2, 621355968000000000),
            (3, 638155413748959300),
            (4, 638155413748959300),
            (5, 638155413748959310),
            (6, 713589688368547750),
            (7, 946723967999999990)
        ], ['id', 'tick'])

        cls.ticks_from_unix_nanos = cls.spark.createDataFrame([
            (1, 599266080000000000),
            (2, 621355968000000000),
            (3, 638155413748959308),
            (4, 638155413748959309),
            (5, 638155413748959310),
            (6, 713589688368547758),
            (7, None)
        ], ['id', 'tick'])

    def compare_dfs(self, expected, actual):
        print('expected')
        expected.show(truncate=False)
        print('actual')
        actual.show(truncate=False)
        self.assertEqual(
            [row.asDict() for row in actual.collect()],
            [row.asDict() for row in expected.collect()]
        )

    def test_dotnet_ticks_to_timestamp(self):
        for column in ["tick", self.ticks.tick]:
            with self.subTest(column=column):
                timestamps = self.ticks.withColumn("timestamp", dotnet_ticks_to_timestamp(column)).orderBy('id')
                expected = self.ticks.join(self.timestamps, "id").orderBy('id')
                self.compare_dfs(expected, timestamps)

    def test_dotnet_ticks_to_unix_epoch(self):
        for column in ["tick", self.ticks.tick]:
            with self.subTest(column=column):
                timestamps = self.ticks.withColumn("unix", dotnet_ticks_to_unix_epoch(column)).orderBy('id')
                expected = self.ticks.join(self.unix, "id").orderBy('id')
                self.compare_dfs(expected, timestamps)

    def test_dotnet_ticks_to_unix_epoch_nanos(self):
        self.maxDiff = None
        for column in ["tick", self.ticks.tick]:
            with self.subTest(column=column):
                timestamps = self.ticks.withColumn("unix_nanos", dotnet_ticks_to_unix_epoch_nanos(column)).orderBy('id')
                expected = self.ticks.join(self.unix_nanos, "id").orderBy('id')
                self.compare_dfs(expected, timestamps)

    def test_timestamp_to_dotnet_ticks(self):
        if self.spark.version.startswith('3.0.'):
            self.skipTest('timestamp_to_dotnet_ticks not supported by Spark 3.0')
        for column in ["timestamp", self.timestamps.timestamp]:
            with self.subTest(column=column):
                timestamps = self.timestamps.withColumn("tick", timestamp_to_dotnet_ticks(column)).orderBy('id')
                expected = self.timestamps.join(self.ticks_from_timestamp, "id").orderBy('id')
                self.compare_dfs(expected, timestamps)

    def test_unix_epoch_dotnet_ticks(self):
        for column in ["unix", self.unix.unix]:
            with self.subTest(column=column):
                timestamps = self.unix.withColumn("tick", unix_epoch_to_dotnet_ticks(column)).orderBy('id')
                expected = self.unix.join(self.ticks, "id").orderBy('id')
                self.compare_dfs(expected, timestamps)

    def test_unix_epoch_nanos_to_dotnet_ticks(self):
        for column in ["unix_nanos", self.unix_nanos.unix_nanos]:
            with self.subTest(column=column):
                timestamps = self.unix_nanos.withColumn("tick", unix_epoch_nanos_to_dotnet_ticks(column)).orderBy('id')
                expected = self.unix_nanos.join(self.ticks_from_unix_nanos, "id").orderBy('id')
                self.compare_dfs(expected, timestamps)

    def test_count_null(self):
        actual = self.unix_nanos.select(
            count("id").alias("ids"),
            count(col("unix_nanos")).alias("nanos"),
            count_null("id").alias("null_ids"),
            count_null(col("unix_nanos")).alias("null_nanos"),
        ).collect()
        self.assertEqual([Row(ids=7, nanos=6, null_ids=0, null_nanos=1)], actual)

    def test_create_temp_dir(self):
        from pyspark import SparkFiles

        dir = self.spark.create_temporary_dir("prefix")
        self.assertTrue(dir.startswith(SparkFiles.getRootDirectory()))

    @skipIf(__version__.startswith('3.0.'), 'install_pip_package not supported for Spark 3.0')
    def test_install_pip_package(self):
        self.spark.sparkContext.setLogLevel("INFO")
        with self.assertRaises(ImportError):
            # noinspection PyPackageRequirements
            import emoji
            emoji.emojize("this test is :thumbs_up:")

        self.spark.install_pip_package("emoji", '--cache', '.cache/pypi')

        # noinspection PyPackageRequirements
        import emoji
        actual = emoji.emojize("this test is :thumbs_up:")
        expected = "this test is 👍"
        self.assertEqual(expected, actual)

        import pandas as pd
        actual = self.spark.range(0, 10, 1, 10) \
            .mapInPandas(lambda it: [pd.DataFrame.from_dict({"val": [emoji.emojize(":thumbs_up:")]})], "val string") \
            .collect()
        expected = [Row("👍")] * 10
        self.assertEqual(expected, actual)

    @skipIf(__version__.startswith('3.0.'), 'install_pip_package not supported for Spark 3.0')
    def test_install_pip_package_unknown_argument(self):
        with self.assertRaises(CalledProcessError):
            self.spark.install_pip_package("--unknown", "argument")

    @skipIf(__version__.startswith('3.0.'), 'install_pip_package not supported for Spark 3.0')
    def test_install_pip_package_package_not_found(self):
        with self.assertRaises(CalledProcessError):
            self.spark.install_pip_package("pyspark-extension==abc")

    @skipUnless(__version__.startswith('3.0.'), 'install_pip_package not supported for Spark 3.0')
    def test_install_pip_package_not_supported(self):
        with self.assertRaises(NotImplementedError):
            self.spark.install_pip_package("emoji")

    @skipIf(__version__.startswith('3.0.'), 'install_poetry_project not supported for Spark 3.0')
    # provide an environment variable with path to the python binary of a virtual env that has poetry installed
    @skipIf(POETRY_PYTHON_ENV not in os.environ, f'Environment variable {POETRY_PYTHON_ENV} pointing to '
                                                 f'virtual env python with poetry required')
    @skipIf(RICH_SOURCES_ENV not in os.environ, f'Environment variable {RICH_SOURCES_ENV} pointing to '
                                                f'rich project sources required')
    def test_install_poetry_project(self):
        self.spark.sparkContext.setLogLevel("INFO")
        with self.assertRaises(ImportError):
            # noinspection PyPackageRequirements
            from rich.emoji import Emoji
            thumbs_up = Emoji("thumbs_up")

        rich_path = os.environ[RICH_SOURCES_ENV]
        poetry_python = os.environ[POETRY_PYTHON_ENV]
        self.spark.install_poetry_project(
            rich_path,
            poetry_python=poetry_python,
            pip_args=['--cache', '.cache/pypi']
        )

        # noinspection PyPackageRequirements
        from rich.emoji import Emoji
        thumbs_up = Emoji("thumbs_up")
        actual = thumbs_up.replace("this test is :thumbs_up:")
        expected = "this test is 👍"
        self.assertEqual(expected, actual)

        import pandas as pd
        actual = self.spark.range(0, 10, 1, 10) \
            .mapInPandas(lambda it: [pd.DataFrame.from_dict({"val": [thumbs_up.replace(":thumbs_up:")]})], "val string") \
            .collect()
        expected = [Row("👍")] * 10
        self.assertEqual(expected, actual)

    @skipIf(__version__.startswith('3.0.'), 'install_poetry_project not supported for Spark 3.0')
    # provide an environment variable with path to the python binary of a virtual env that has poetry installed
    @skipIf(POETRY_PYTHON_ENV not in os.environ, f'Environment variable {POETRY_PYTHON_ENV} pointing to '
                                                 f'virtual env python with poetry required')
    @skipIf(RICH_SOURCES_ENV not in os.environ, f'Environment variable {RICH_SOURCES_ENV} pointing to '
                                                f'rich project sources required')
    def test_install_poetry_project_wrong_arguments(self):
        rich_path = os.environ[RICH_SOURCES_ENV]
        poetry_python = os.environ[POETRY_PYTHON_ENV]

        with self.assertRaises(RuntimeError):
            self.spark.install_poetry_project("non-existing-project", poetry_python=poetry_python)
        with self.assertRaises(FileNotFoundError):
            self.spark.install_poetry_project(rich_path, poetry_python="non-existing-python")

    @skipUnless(__version__.startswith('3.0.'), 'install_poetry_project not supported for Spark 3.0')
    def test_install_poetry_project_not_supported(self):
        with self.assertRaises(NotImplementedError):
            self.spark.install_poetry_project("./rich")


if __name__ == '__main__':
    SparkTest.main(__file__)
