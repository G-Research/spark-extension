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

from pathlib import Path

from spark_common import SparkTest
import gresearch.spark.parquet


class ParquetTest(SparkTest):

    test_file = str((Path(__file__).parent.parent.parent / "src" / "test" / "files" / "test.parquet").resolve())

    def test_parquet_metadata(self):
        self.assertEqual(self.spark.read.parquet_metadata(self.test_file).count(), 2)
        self.assertEqual(self.spark.read.parquet_metadata(self.test_file, self.test_file).count(), 2)
        self.assertEqual(self.spark.read.parquet_metadata(self.test_file, parallelism=100).count(), 2)
        self.assertEqual(self.spark.read.parquet_metadata(self.test_file, self.test_file, parallelism=100).count(), 2)

    def test_parquet_schema(self):
        self.assertEqual(self.spark.read.parquet_schema(self.test_file).count(), 4)
        self.assertEqual(self.spark.read.parquet_schema(self.test_file, self.test_file).count(), 4)
        self.assertEqual(self.spark.read.parquet_schema(self.test_file, parallelism=100).count(), 4)
        self.assertEqual(self.spark.read.parquet_schema(self.test_file, self.test_file, parallelism=100).count(), 4)

    def test_parquet_blocks(self):
        self.assertEqual(self.spark.read.parquet_blocks(self.test_file).count(), 3)
        self.assertEqual(self.spark.read.parquet_blocks(self.test_file, self.test_file).count(), 3)
        self.assertEqual(self.spark.read.parquet_blocks(self.test_file, parallelism=100).count(), 3)
        self.assertEqual(self.spark.read.parquet_blocks(self.test_file, self.test_file, parallelism=100).count(), 3)

    def test_parquet_block_columns(self):
        self.assertEqual(self.spark.read.parquet_block_columns(self.test_file).count(), 6)
        self.assertEqual(self.spark.read.parquet_block_columns(self.test_file, self.test_file).count(), 6)
        self.assertEqual(self.spark.read.parquet_block_columns(self.test_file, parallelism=100).count(), 6)
        self.assertEqual(self.spark.read.parquet_block_columns(self.test_file, self.test_file, parallelism=100).count(), 6)

    def test_parquet_partitions(self):
        self.assertEqual(self.spark.read.parquet_partitions(self.test_file).count(), 2)
        self.assertEqual(self.spark.read.parquet_partitions(self.test_file, self.test_file).count(), 2)
        self.assertEqual(self.spark.read.parquet_partitions(self.test_file, parallelism=100).count(), 2)
        self.assertEqual(self.spark.read.parquet_partitions(self.test_file, self.test_file, parallelism=100).count(), 2)


if __name__ == '__main__':
    SparkTest.main(__file__)
