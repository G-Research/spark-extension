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

from pyspark.sql import DataFrameReader, DataFrame

from gresearch.spark import _to_seq


def _reader(reader: DataFrameReader) -> DataFrameReader:
    jvm = reader._spark._jvm
    return jvm.uk.co.gresearch.spark.parquet.__getattr__("package$").__getattr__("MODULE$").ExtendedDataFrameReader(reader._jreader)


def parquet_metadata(self: DataFrameReader, *paths: str) -> DataFrame:
    jvm = self._spark._jvm
    jdf = _reader(self).parquetMetadata(_to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


def parquet_blocks(self: DataFrameReader, *paths: str) -> DataFrame:
    jvm = self._spark._jvm
    jdf = _reader(self).parquetBlocks(_to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


def parquet_block_columns(self: DataFrameReader, *paths: str) -> DataFrame:
    jvm = self._spark._jvm
    jdf = _reader(self).parquetBlockColumns(_to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


def parquet_partitions(self: DataFrameReader, *paths: str) -> DataFrame:
    jvm = self._spark._jvm
    jdf = _reader(self).parquetPartitions(_to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


def parquet_partition_rows(self: DataFrameReader, *paths: str) -> DataFrame:
    jvm = self._spark._jvm
    jdf = _reader(self).parquetPartitionRows(_to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


DataFrameReader.parquet_metadata = parquet_metadata
DataFrameReader.parquet_blocks = parquet_blocks
DataFrameReader.parquet_block_columns = parquet_block_columns
DataFrameReader.parquet_partitions = parquet_partitions
DataFrameReader.parquet_partition_rows = parquet_partition_rows
