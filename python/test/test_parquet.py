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

from spark_common import SparkTest
import gresearch.spark.parquet


class ParquetTest(SparkTest):

    def test_parquet_metadata(self):
        self.spark.read.parquet_metadata("../../../spark-dgraph-connector/dgraph.dbpedia.rdf-label.parquet").show(truncate=False)

    def test_parquet_blocks(self):
        self.spark.read.parquet_blocks("../../../spark-dgraph-connector/dgraph.dbpedia.rdf-label.parquet").show(truncate=False)

    def test_parquet_block_columns(self):
        self.spark.read.parquet_block_columns("../../../spark-dgraph-connector/dgraph.dbpedia.rdf-label.parquet").show(truncate=False)

    def test_parquet_partitions(self):
        self.spark.read.parquet_partitions("../../../spark-dgraph-connector/dgraph.dbpedia.rdf-label.parquet").show(truncate=False)

    def test_parquet_partition_rows(self):
        self.spark.read.parquet_partition_rows("../../../spark-dgraph-connector/dgraph.dbpedia.rdf-label.parquet").show(truncate=False)


if __name__ == '__main__':
    SparkTest.main()
