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

from typing import Optional

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrameReader, DataFrame

from gresearch.spark import _to_seq


def _jreader(reader: DataFrameReader) -> JavaObject:
    jvm = reader._spark._jvm
    return jvm.uk.co.gresearch.spark.parquet.__getattr__("package$").__getattr__("MODULE$").ExtendedDataFrameReader(reader._jreader)


def parquet_metadata(self: DataFrameReader, *paths: str, parallelism: Optional[int] = None) -> DataFrame:
    """
    Read the metadata of Parquet files into a Dataframe.

    The returned DataFrame has as many partitions as specified via `parallelism`.
    If not specified, there are as many partitions as there are Parquet files,
    at most `spark.sparkContext.defaultParallelism` partitions.

    This provides the following per-file information:
    - filename (string): The file name
    - blocks (int): Number of blocks / RowGroups in the Parquet file
    - compressedBytes (long): Number of compressed bytes of all blocks
    - uncompressedBytes (long): Number of uncompressed bytes of all blocks
    - rows (long): Number of rows in the file
    - columns (int): Number of rows in the file
    - values (long): Number of values in the file
    - nulls (long): Number of null values in the file
    - createdBy (string): The createdBy string of the Parquet file, e.g. library used to write the file
    - schema (string): The schema
    - encryption (string): The encryption
    - keyValues (string-to-string map): Key-value data of the file

    :param self: a Spark DataFrameReader
    :param paths: paths one or more paths to Parquet files or directories
    :param parallelism: number of partitions of returned DataFrame
    :return: dataframe with Parquet metadata
    """
    jvm = self._spark._jvm
    if parallelism is None:
        jdf = _jreader(self).parquetMetadata(_to_seq(jvm, list(paths)))
    else:
        jdf = _jreader(self).parquetMetadata(parallelism, _to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


def parquet_schema(self: DataFrameReader, *paths: str, parallelism: Optional[int] = None) -> DataFrame:
    """
    Read the schema of Parquet files into a Dataframe.

    The returned DataFrame has as many partitions as specified via `parallelism`.
    If not specified, there are as many partitions as there are Parquet files,
    at most `spark.sparkContext.defaultParallelism` partitions.

    This provides the following per-file information:
    - filename (string): The Parquet file name
    - columnName (string): The column name
    - columnPath (string array): The column path
    - repetition (string): The repetition
    - type (string): The data type
    - length (int): The length of the type
    - originalType (string): The original type
    - isPrimitive (boolean: True if type is primitive
    - primitiveType (string: The primitive type
    - primitiveOrder (string: The order of the primitive type
    - maxDefinitionLevel (int): The max definition level
    - maxRepetitionLevel (int): The max repetition level

    :param self: a Spark DataFrameReader
    :param paths: paths one or more paths to Parquet files or directories
    :param parallelism: number of partitions of returned DataFrame
    :return: dataframe with Parquet metadata
    """
    jvm = self._spark._jvm
    if parallelism is None:
        jdf = _jreader(self).parquetSchema(_to_seq(jvm, list(paths)))
    else:
        jdf = _jreader(self).parquetSchema(parallelism, _to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


def parquet_blocks(self: DataFrameReader, *paths: str, parallelism: Optional[int] = None) -> DataFrame:
    """
    Read the metadata of Parquet blocks into a Dataframe.

    The returned DataFrame has as many partitions as specified via `parallelism`.
    If not specified, there are as many partitions as there are Parquet files,
    at most `spark.sparkContext.defaultParallelism` partitions.

    This provides the following per-block information:
    - filename (string): The file name
    - block (int): Block / RowGroup number starting at 1
    - blockStart (long): Start position of the block in the Parquet file
    - compressedBytes (long): Number of compressed bytes in block
    - uncompressedBytes (long): Number of uncompressed bytes in block
    - rows (long): Number of rows in block
    - columns (int): Number of columns in block
    - values (long): Number of values in block
    - nulls (long): Number of null values in block

    :param self: a Spark DataFrameReader
    :param paths: paths one or more paths to Parquet files or directories
    :param parallelism: number of partitions of returned DataFrame
    :return: dataframe with Parquet metadata
    """
    jvm = self._spark._jvm
    if parallelism is None:
        jdf = _jreader(self).parquetBlocks(_to_seq(jvm, list(paths)))
    else:
        jdf = _jreader(self).parquetBlocks(parallelism, _to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


def parquet_block_columns(self: DataFrameReader, *paths: str, parallelism: Optional[int] = None) -> DataFrame:
    """
    Read the metadata of Parquet block columns into a Dataframe.

    The returned DataFrame has as many partitions as specified via `parallelism`.
    If not specified, there are as many partitions as there are Parquet files,
    at most `spark.sparkContext.defaultParallelism` partitions.

    This provides the following per-block-column information:
    - filename (string): The file name
    - block (int): Block / RowGroup number starting at 1
    - column (array<string>): Block / RowGroup column name
    - codec (string): The coded used to compress the block column values
    - type (string): The data type of the block column
    - encodings (array<string>): Encodings of the block column
    - minValue (string): Minimum value of this column in this block
    - maxValue (string): Maximum value of this column in this block
    - columnStart (long): Start position of the block column in the Parquet file
    - compressedBytes (long): Number of compressed bytes of this block column
    - uncompressedBytes (long): Number of uncompressed bytes of this block column
    - values (long): Number of values in this block column
    - nulls (long): Number of null values in this block column

    :param self: a Spark DataFrameReader
    :param paths: paths one or more paths to Parquet files or directories
    :param parallelism: number of partitions of returned DataFrame
    :return: dataframe with Parquet metadata
    """
    jvm = self._spark._jvm
    if parallelism is None:
        jdf = _jreader(self).parquetBlockColumns(_to_seq(jvm, list(paths)))
    else:
        jdf = _jreader(self).parquetBlockColumns(parallelism, _to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


def parquet_partitions(self: DataFrameReader, *paths: str, parallelism: Optional[int] = None) -> DataFrame:
    """
    Read the metadata of how Spark partitions Parquet files into a Dataframe.

    The returned DataFrame has as many partitions as specified via `parallelism`.
    If not specified, there are as many partitions as there are Parquet files,
    at most `spark.sparkContext.defaultParallelism` partitions.

    This provides the following per-partition information:
    - partition (int): The Spark partition id
    - partitionStart (long): The start position of the partition
    - partitionEnd (long): The end position of the partition
    - partitionLength (long): The length of the partition
    - blocks (int): The number of Parquet blocks / RowGroups in this partition
    - compressedBytes (long): The number of compressed bytes in this partition
    - uncompressedBytes (long): The number of uncompressed bytes in this partition
    - rows (long): The number of rows in this partition
    - columns (int): The number of columns in this partition
    - values (long): The number of values in this partition
    - nulls (long): The number of null values in this partition
    - filename (string): The Parquet file name
    - fileLength (long): The length of the Parquet file

    :param self: a Spark DataFrameReader
    :param paths: paths one or more paths to Parquet files or directories
    :param parallelism: number of partitions of returned DataFrame
    :return: dataframe with Parquet metadata
    """
    jvm = self._spark._jvm
    if parallelism is None:
        jdf = _jreader(self).parquetPartitions(_to_seq(jvm, list(paths)))
    else:
        jdf = _jreader(self).parquetPartitions(parallelism, _to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


DataFrameReader.parquet_metadata = parquet_metadata
DataFrameReader.parquet_schema = parquet_schema
DataFrameReader.parquet_blocks = parquet_blocks
DataFrameReader.parquet_block_columns = parquet_block_columns
DataFrameReader.parquet_partitions = parquet_partitions
