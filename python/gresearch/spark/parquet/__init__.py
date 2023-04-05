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
    """
    Read the metadata of Parquet files into a Dataframe.

    This provides the following per-file information:
    - filename (string): The file name
    - blocks (int): Number of blocks / RowGroups in the Parquet file
    - compressedBytes (long): Number of compressed bytes of all blocks
    - uncompressedBytes (long): Number of uncompressed bytes of all blocks
    - rows (long): Number of rows of all blocks
    - createdBy (string): The createdBy string of the Parquet file, e.g. library used to write the file
    - schema (string): The schema

    :param self: a Spark DataFrameReader
    :param paths: paths one or more paths to Parquet files or directories
    :return: dataframe with Parquet metadata
    """
    jvm = self._spark._jvm
    jdf = _reader(self).parquetMetadata(_to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


def parquet_blocks(self: DataFrameReader, *paths: str) -> DataFrame:
    """
    Read the metadata of Parquet blocks into a Dataframe.

    This provides the following per-block information:
    - filename (string): The file name
    - block (int): Block number starting at 1
    - blockStart (long): Start position of block in Parquet file
    - compressedBytes (long): Number of compressed bytes in block
    - uncompressedBytes (long): Number of uncompressed bytes in block
    - rows (long): Number of rows in block

    :param self: a Spark DataFrameReader
    :param paths: paths one or more paths to Parquet files or directories
    :return: dataframe with Parquet metadata
    """
    jvm = self._spark._jvm
    jdf = _reader(self).parquetBlocks(_to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


def parquet_block_columns(self: DataFrameReader, *paths: str) -> DataFrame:
    """
    Read the metadata of Parquet block columns into a Dataframe.

    This provides the following per-block-column information:
    - filename (string): The file name
    - block (int): Block number starting at 1
    - column (string): Block column name
    - codec (string): The coded used to compress the block column values
    - type (string): The data type of the block column
    - encodings (string): Encodings of the block column
    - minValue (string): Minimum value of this column in this block
    - maxValue (string): Maximum value of this column in this block
    - columnStart (long): Start position of block column in Parquet file
    - compressedBytes (long): Number of compressed bytes of this block column
    - uncompressedBytes (long): Number of uncompressed bytes of this block column
    - values (long): Number of values in this block column
    
    :param self: a Spark DataFrameReader
    :param paths: paths one or more paths to Parquet files or directories
    :return: dataframe with Parquet metadata
    """
    jvm = self._spark._jvm
    jdf = _reader(self).parquetBlockColumns(_to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


def parquet_partitions(self: DataFrameReader, *paths: str) -> DataFrame:
    """
    Read the metadata of how Spark partitions Parquet files into a Dataframe.

    This provides the following per-partition information:
    - partition (int): The Spark partition id
    - filename (string): The Parquet file name
    - fileLength (long): The length of the Parquet file
    - partitionStart (long): The start position of the partition
    - partitionEnd (long): The end position of the partition
    - partitionLength (long): The length of the partition
    - blocks (int): The number of Parquet blocks in this partition
    - compressedBytes (long): The number of compressed bytes in this partition
    - uncompressedBytes (long): The number of uncompressed bytes in this partition
    - rows (long): The number of rows in this partition

    :param self: a Spark DataFrameReader
    :param paths: paths one or more paths to Parquet files or directories
    :return: dataframe with Parquet metadata
    """
    jvm = self._spark._jvm
    jdf = _reader(self).parquetPartitions(_to_seq(jvm, list(paths)))
    return DataFrame(jdf, self._spark)


DataFrameReader.parquet_metadata = parquet_metadata
DataFrameReader.parquet_blocks = parquet_blocks
DataFrameReader.parquet_block_columns = parquet_block_columns
DataFrameReader.parquet_partitions = parquet_partitions
