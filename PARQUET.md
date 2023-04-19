# Parquet Metadata

The structure of Parquet files (the metadata, not the data stored in Parquet) can be inspected similar to [parquet-tools](https://pypi.org/project/parquet-tools/)
or [parquet-cli](https://pypi.org/project/parquet-cli/)
by reading from a simple Spark data source.

Parquet metadata can be read on [file level](#parquet-file-metadata),
[row group level](#parquet-block--rowgroup-metadata),
[column chunk level](#parquet-block-column-metadata) and
[Spark Parquet partition level](#parquet-partition-metadata).
Multiple files can be inspected at once.

Any location that can be read by Spark (`spark.read.parquet(…)`) can be inspected.
This means the path can point to a single Parquet file, a directory with Parquet files,
or multiple paths separated by a comma (`,`). Paths can contain wildcards like `*`.
Multiple files will be inspected in parallel and distributed by Spark.
No actual rows or values will be read from the Parquet files, only metadata, which is very fast.
This allows to inspect Parquet files that have different schemata with one `spark.read` operation.

First, import the new Parquet metadata data sources:

```scala
// Scala
import uk.co.gresearch.spark.parquet._
```

```python
# Python
import gresearch.spark.parquet
```

Then, the following metadata become available:

## Parquet file metadata

Read the metadata of Parquet files into a Dataframe:

```scala
// Scala
spark.read.parquetMetadata("/path/to/parquet").show()
```
```python
# Python
spark.read.parquet_metadata("/path/to/parquet").show()
```
```
+-------------+------+---------------+-----------------+----+--------------------+--------------------+
|     filename|blocks|compressedBytes|uncompressedBytes|rows|           createdBy|              schema|
+-------------+------+---------------+-----------------+----+--------------------+--------------------+
|file1.parquet|     1|           1268|             1652| 100|parquet-mr versio...|message spark_sch...|
|file2.parquet|     2|           2539|             3302| 200|parquet-mr versio...|message spark_sch...|
+-------------+------+---------------+-----------------+----+--------------------+--------------------+
```

The Dataframe provides the following per-file information:

|column            |type  |description                                                                  |
|:-----------------|:----:|:----------------------------------------------------------------------------|
|filename          |string|The Parquet file name                                                        |
|blocks            |int   |Number of blocks / RowGroups in the Parquet file                             |
|compressedBytes   |long  |Number of compressed bytes of all blocks                                     |
|uncompressedBytes |long  |Number of uncompressed bytes of all blocks                                   |
|rows              |long  |Number of rows of all blocks                                                 |
|createdBy         |string|The createdBy string of the Parquet file, e.g. library used to write the file|
|schema            |string|The schema                                                                   |

## Parquet block / RowGroup metadata

Read the metadata of Parquet blocks / RowGroups into a Dataframe:

```scala
// Scala
spark.read.parquetBlocks("/path/to/parquet").show()
```
```python
# Python
spark.read.parquet_blocks("/path/to/parquet").show()
```
```
+-------------+-----+----------+---------------+-----------------+----+
|     filename|block|blockStart|compressedBytes|uncompressedBytes|rows|
+-------------+-----+----------+---------------+-----------------+----+
|file1.parquet|    1|         4|           1269|             1651| 100|
|file2.parquet|    1|         4|           1268|             1652| 100|
|file2.parquet|    2|      1273|           1270|             1651| 100|
+-------------+-----+----------+---------------+-----------------+----+

```

|column            |type  |description                                    |
|:-----------------|:----:|:----------------------------------------------|
|filename          |string|The Parquet file name                          |
|block             |int   |Block / RowGroup number starting at 1          |
|blockStart        |long  |Start position of the block in the Parquet file|
|compressedBytes   |long  |Number of compressed bytes in block            |
|uncompressedBytes |long  |Number of uncompressed bytes in block          |
|rows              |long  |Number of rows in block                        |

## Parquet block column metadata

Read the metadata of Parquet block columns into a Dataframe:

```scala
// Scala
spark.read.parquetBlockColumns("/path/to/parquet").show()
```
```python
# Python
spark.read.parquet_block_columns("/path/to/parquet").show()
```
```
+-------------+-----+------+------+-------------------+-------------------+--------------------+------------------+-----------+---------------+-----------------+------+
|     filename|block|column| codec|               type|          encodings|            minValue|          maxValue|columnStart|compressedBytes|uncompressedBytes|values|
+-------------+-----+------+------+-------------------+-------------------+--------------------+------------------+-----------+---------------+-----------------+------+
|file1.parquet|    1|  [id]|SNAPPY|  required int64 id|[BIT_PACKED, PLAIN]|                   0|                99|          4|            437|              826|   100|
|file1.parquet|    1| [val]|SNAPPY|required double val|[BIT_PACKED, PLAIN]|0.005067503372006343|0.9973357672164814|        441|            831|              826|   100|
|file2.parquet|    1|  [id]|SNAPPY|  required int64 id|[BIT_PACKED, PLAIN]|                 100|               199|          4|            438|              825|   100|
|file2.parquet|    1| [val]|SNAPPY|required double val|[BIT_PACKED, PLAIN]|0.010617521596503865| 0.999189783846449|        442|            831|              826|   100|
|file2.parquet|    2|  [id]|SNAPPY|  required int64 id|[BIT_PACKED, PLAIN]|                 200|               299|       1273|            440|              826|   100|
|file2.parquet|    2| [val]|SNAPPY|required double val|[BIT_PACKED, PLAIN]|0.011277044401634018| 0.970525681750662|       1713|            830|              825|   100|
+-------------+-----+------+------+-------------------+-------------------+--------------------+------------------+-----------+---------------+-----------------+------+
```

|column            |type  |description                                           |
|:-----------------|:----:|:-----------------------------------------------------|
|filename          |string|The Parquet file name                                 |
|block             |int   |Block / RowGroup number starting at 1                 |
|column            |string|Block / RowGroup column name                          |
|codec             |string|The coded used to compress the block column values    |
|type              |string|The data type of the block column                     |
|encodings         |string|Encodings of the block column                         |
|minValue          |string|Minimum value of this column in this block            |
|maxValue          |string|Maximum value of this column in this block            |
|columnStart       |long  |Start position of the block column in the Parquet file|
|compressedBytes   |long  |Number of compressed bytes of this block column       |
|uncompressedBytes |long  |Number of uncompressed bytes of this block column     |
|valueCount        |long  |Number of values in this block column                 |

## Parquet partition metadata

Read the metadata of how Spark partitions Parquet files into a Dataframe:

```scala
// Scala
spark.read.parquetPartitions("/path/to/parquet").show()
```
```python
# Python
spark.read.parquet_partitions("/path/to/parquet").show()
```
```
+---------+-----+----+------+------+---------------+-----------------+----+-------------+----------+
|partition|start| end|length|blocks|compressedBytes|uncompressedBytes|rows|     filename|fileLength|
+---------+-----+----+------+------+---------------+-----------------+----+-------------+----------+
|        1|    0|1024|  1024|     1|           1268|             1652| 100|file1.parquet|      1930|
|        2| 1024|1930|   906|     0|              0|                0|   0|file1.parquet|      1930|
|        3|    0|1024|  1024|     1|           1269|             1651| 100|file2.parquet|      3493|
|        4| 1024|2048|  1024|     1|           1270|             1651| 100|file2.parquet|      3493|
|        5| 2048|3072|  1024|     0|              0|                0|   0|file2.parquet|      3493|
|        6| 3072|3493|   421|     0|              0|                0|   0|file2.parquet|      3493|
+---------+-----+----+------+------+---------------+-----------------+----+-------------+----------+
```

|column           |type  |description                                               |
|:----------------|:----:|:---------------------------------------------------------|
|partition        |int   |The Spark partition id                                    |
|start            |long  |The start position of the partition                       |
|end              |long  |The end position of the partition                         |
|length           |long  |The length of the partition                               |
|blocks           |int   |The number of Parquet blocks / RowGroups in this partition|
|compressedBytes  |long  |The number of compressed bytes in this partition          |
|uncompressedBytes|long  |The number of uncompressed bytes in this partition        |
|rows             |long  |The number of rows in this partition                      |
|filename         |string|The Parquet file name                                     |
|fileLength       |long  |The length of the Parquet file                            |
