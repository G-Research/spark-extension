# Parquet Metadata

The structure of Parquet files (the metadata, not the data stored in Parquet) can be inspected similar to [parquet-tools](https://pypi.org/project/parquet-tools/)
by reading from a simple data source.

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
spark.read.parquetMetadata("/path/to/parquet").show(false)
```
```python
# Python
spark.read.parquet_metadata("/path/to/parquet").show(truncate=False)
```
```
+-------------+------+--------------+-------------------+-------------+-------------------------+----------------------------+
|filename     |blocks|totalSizeBytes|compressedSizeBytes|totalRowCount|createdBy                |schema                      |
+-------------+------+--------------+-------------------+-------------+-------------------------+----------------------------+
|file1.parquet|1     |1652          |1268               |100          |parquet-mr version 1.12.2|message spark_schema {...}\n|
|file2.parquet|2     |3302          |2539               |200          |parquet-mr version 1.12.2|message spark_schema {...}\n|
+-------------+------+--------------+-------------------+-------------+-------------------------+----------------------------+
```

The Dataframe provides the following per-file information:

|column              |type  |description                                                                    |
|:-------------------|:----:|:------------------------------------------------------------------------------|
|filename            |string| The Parquet file name                                                                 |
|blocks              |int   | Number of blocks / RowGroups in the Parquet file                              |
|totalSizeBytes      |long  | Number of uncompressed bytes of all blocks                                    |
|compressedSizeBytes |long  | Number of compressed bytes of all blocks                                      |
|totalRowCount       |long  | Number of rows of all blocks                                                  |
|createdBy           |string| The createdBy string of the Parquet file, e.g. library used to write the file |
|schema              |string| The schema                                                                    |

## Parquet block / RowGroup metadata

Read the metadata of Parquet blocks / RowGroups into a Dataframe:

```scala
// Scala
spark.read.parquetBlocks("/path/to/parquet").show(false)
```
```python
# Python
spark.read.parquet_blocks("/path/to/parquet").show(truncate=False)
```
```
+-------------+-----+--------+--------------+-------------------+-------------+
|filename     |block|startPos|totalSizeBytes|compressedSizeBytes|totalRowCount|
+-------------+-----+--------+--------------+-------------------+-------------+
|file1.parquet|1    |4       |1652          |1268               |100          |
|file2.parquet|1    |4       |1651          |1269               |100          |
|file2.parquet|2    |1273    |1651          |1270               |100          |
+-------------+-----+--------+--------------+-------------------+-------------+
```

|column              |type  |description|
|:-------------------|:----:|:----------------------------------------|
|filename            |string| The Parquet file name                   |
|block               |int   | Block number starting at 1              |
|startPos            |long  | Start position of block in Parquet file |
|totalSizeBytes      |long  | Number of uncompressed bytes in block   |
|compressedSizeBytes |long  | Number of compressed bytes in block     |
|totalRowCount       |long  | Number of rows in block                 |

## Parquet block column metadata

Read the metadata of Parquet blocks columns into a Dataframe:

```scala
// Scala
spark.read.parquetBlockColumns("/path/to/parquet").show(false)
```
```python
# Python
spark.read.parquet_block_columns("/path/to/parquet").show(truncate=False)
```
```
+-------------+-----+------+------+-------------------+-------------------+--------------------+------------------+--------+---------+-------------------+----------+
|filename     |block|column|codec |type               |encodings          |minValue            |maxValue          |startPos|sizeBytes|compressedSizeBytes|valueCount|
+-------------+-----+------+------+-------------------+-------------------+--------------------+------------------+--------+---------+-------------------+----------+
|file1.parquet|1    |[id]  |SNAPPY|required int64 id  |[BIT_PACKED, PLAIN]|0                   |99                |4       |826      |437                |100       |
|file1.parquet|1    |[val] |SNAPPY|required double val|[BIT_PACKED, PLAIN]|0.005067503372006343|0.9973357672164814|441     |826      |831                |100       |
|file2.parquet|1    |[id]  |SNAPPY|required int64 id  |[BIT_PACKED, PLAIN]|100                 |199               |4       |825      |438                |100       |
|file2.parquet|1    |[val] |SNAPPY|required double val|[BIT_PACKED, PLAIN]|0.010617521596503865|0.999189783846449 |442     |826      |831                |100       |
|file2.parquet|2    |[id]  |SNAPPY|required int64 id  |[BIT_PACKED, PLAIN]|200                 |299               |1273    |826      |440                |100       |
|file2.parquet|2    |[val] |SNAPPY|required double val|[BIT_PACKED, PLAIN]|0.011277044401634018|0.970525681750662 |1713    |825      |830                |100       |
+-------------+-----+------+------+-------------------+-------------------+--------------------+------------------+--------+---------+-------------------+----------+
```

|column              |type  |description|
|:-------------------|:----:|:--------------------------------------------------|
|filename            |string| The Parquet file name                             |
|block               |int   | Block number starting at 1                        |
|column              |string| Block column name                                 |
|codec               |string| The coded used to compress the block column values|
|type                |string| The data type of the block column                 |
|encodings           |string| Encodings of the block column                     |
|minValue            |string| Minimum value of this column in this block        |
|maxValue            |string| Maximum value of this column in this block        |
|startPos            |long  | Start position of block column in Parquet file    |
|sizeBytes           |long  | Number of bytes of this block column              |
|compressedSizeBytes |long  | Number of compressed bytes of this block column   |
|valueCount          |long  | Number of values in this block column             |

## Parquet partition metadata

Read the metadata of how Spark partitions Parquet files into a Dataframe:

```scala
// Scala
spark.read.parquetPartitions("/path/to/parquet").show(false)
```
```python
# Python
spark.read.parquet_partitions("/path/to/parquet").show(truncate=False)
```
```
+---------+-------------+-----+----+---------------+----------+----+
|partition|filename     |start|end |partitionLength|fileLength|rows|
+---------+-------------+-----+----+---------------+----------+----+
|0        |file2.parquet|0    |1024|1024           |3493      |100 |
|1        |file2.parquet|1024 |2048|1024           |3493      |100 |
|2        |file2.parquet|2048 |3072|1024           |3493      |0   |
|3        |file1.parquet|0    |1024|1024           |1930      |100 |
|4        |file1.parquet|1024 |1930|906            |1930      |0   |
|5        |file2.parquet|3072 |3493|421            |3493      |0   |
+---------+-------------+-----+----+---------------+----------+----+
```

|column              |type  |description|
|:-------------------|:----:|:---------------------------------------------------------------------|
|partition id        |int   | The Spark partition id                                               |
|filename            |string| The Parquet file name                                                |
|start               |long  | The start position of the partition                                  |
|end                 |long  | The end position of the partition                                    |
|partitionLength     |long  | The length of the partition                                          |
|fileLength          |long  | The length of the Parquet file                                       |
|rows                |long  | The number of rows of the Parquet file that belong to this partition |
