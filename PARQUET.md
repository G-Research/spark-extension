# Parquet Metadata

The structure of Parquet files (the metadata, not the data stored in Parquet) can be inspected similar to [parquet-tools](https://pypi.org/project/parquet-tools/)
or [parquet-cli](https://pypi.org/project/parquet-cli/)
by reading from a simple Spark data source.

Parquet metadata can be read on [file level](#parquet-file-metadata),
[schema level](#parquet-file-schema),
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
+-------------+------+---------------+-----------------+----+-------+------+-----+--------------------+--------------------+-----------+--------------------+
|     filename|blocks|compressedBytes|uncompressedBytes|rows|columns|values|nulls|           createdBy|              schema| encryption|           keyValues|
+-------------+------+---------------+-----------------+----+-------+------+-----+--------------------+--------------------+-----------+--------------------+
|file1.parquet|     1|           1268|             1652| 100|      2|   200|    0|parquet-mr versio...|message spark_sch...|UNENCRYPTED|{org.apache.spark...|
|file2.parquet|     2|           2539|             3302| 200|      2|   400|    0|parquet-mr versio...|message spark_sch...|UNENCRYPTED|{org.apache.spark...|
+-------------+------+---------------+-----------------+----+-------+------+-----+--------------------+--------------------+-----------+--------------------+
```

The Dataframe provides the following per-file information:

|column            |type  | description                                                                   |
|:-----------------|:----:|:------------------------------------------------------------------------------|
|filename          |string| The Parquet file name                                                         |
|blocks            |int   | Number of blocks / RowGroups in the Parquet file                              |
|compressedBytes   |long  | Number of compressed bytes of all blocks                                      |
|uncompressedBytes |long  | Number of uncompressed bytes of all blocks                                    |
|rows              |long  | Number of rows in the file                                                    |
|columns           |int   | Number of columns in the file                                                 |
|values            |long  | Number of values in the file                                                  |
|nulls             |long  | Number of null values in the file                                             |
|createdBy         |string| The createdBy string of the Parquet file, e.g. library used to write the file |
|schema            |string| The schema                                                                    |
|encryption        |string| The encryption (requires org.apache.parquet:parquet-hadoop:1.12.4 and above)  |
|keyValues         |string-to-string map| Key-value data of the file                                      |

## Parquet file schema

Read the schema of Parquet files into a Dataframe:

```scala
// Scala
spark.read.parquetSchema("/path/to/parquet").show()
```
```python
# Python
spark.read.parquet_schema("/path/to/parquet").show()
```
```
+------------+----------+------------------+----------+------+------+----------------+--------------------+-----------+-------------+------------------+------------------+------------------+
|    filename|columnName|        columnPath|repetition|  type|length|    originalType|         logicalType|isPrimitive|primitiveType|    primitiveOrder|maxDefinitionLevel|maxRepetitionLevel|
+------------+----------+------------------+----------+------+------+----------------+--------------------+-----------+-------------+------------------+------------------+------------------+
|file.parquet|         a|               [a]|  REQUIRED| INT64|     0|            NULL|                NULL|       true|        INT64|TYPE_DEFINED_ORDER|                 0|                 0|
|file.parquet|         x|            [b, x]|  REQUIRED| INT32|     0|            NULL|                NULL|       true|        INT32|TYPE_DEFINED_ORDER|                 1|                 0|
|file.parquet|         y|            [b, y]|  REQUIRED|DOUBLE|     0|            NULL|                NULL|       true|       DOUBLE|TYPE_DEFINED_ORDER|                 1|                 0|
|file.parquet|         z|            [b, z]|  OPTIONAL| INT64|     0|TIMESTAMP_MICROS|TIMESTAMP(MICROS,...|       true|        INT64|TYPE_DEFINED_ORDER|                 2|                 0|
|file.parquet|   element|[c, list, element]|  OPTIONAL|BINARY|     0|            UTF8|              STRING|       true|       BINARY|TYPE_DEFINED_ORDER|                 3|                 1|
+------------+----------+------------------+----------+------+------+----------------+--------------------+-----------+-------------+------------------+------------------+------------------+
```

The Dataframe provides the following per-file information:

|column            |     type     | description                                                                     |
|:-----------------|:------------:|:--------------------------------------------------------------------------------|
|filename          |    string    | The Parquet file name                                                           |
|columnName        |    string    | The column name                                                                 |
|columnPath        | string array | The column path                                                                 |
|repetition        |    string    | The repetition                                                                  |
|type              |    string    | The data type                                                                   |
|length            |     int      | The length of the type                                                          |
|originalType      |   string     | The original type (requires org.apache.parquet:parquet-hadoop:1.11.0 and above) |
|isPrimitive       |   boolean    | True if type is primitive                                                       |
|primitiveType     |    string    | The primitive type                                                              |
|primitiveOrder    |    string    | The order of the primitive type                                                 |
|maxDefinitionLevel|     int      | The max definition level                                                        |
|maxRepetitionLevel|     int      | The max repetition level                                                        |

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
+-------------+-----+----------+---------------+-----------------+----+-------+------+-----+
|     filename|block|blockStart|compressedBytes|uncompressedBytes|rows|columns|values|nulls|
+-------------+-----+----------+---------------+-----------------+----+-------+------+-----+
|file1.parquet|    1|         4|           1269|             1651| 100|      2|   200|    0|
|file2.parquet|    1|         4|           1268|             1652| 100|      2|   200|    0|
|file2.parquet|    2|      1273|           1270|             1651| 100|      2|   200|    0|
+-------------+-----+----------+---------------+-----------------+----+-------+------+-----+
```

|column            |type  |description                                    |
|:-----------------|:----:|:----------------------------------------------|
|filename          |string|The Parquet file name                          |
|block             |int   |Block / RowGroup number starting at 1          |
|blockStart        |long  |Start position of the block in the Parquet file|
|compressedBytes   |long  |Number of compressed bytes in block            |
|uncompressedBytes |long  |Number of uncompressed bytes in block          |
|rows              |long  |Number of rows in block                        |
|columns           |int   |Number of columns in block                     |
|values            |long  |Number of values in block                      |
|nulls             |long  |Number of null values in block                 |

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
+-------------+-----+------+------+-------------------+-------------------+--------------------+------------------+-----------+---------------+-----------------+------+-----+
|     filename|block|column| codec|               type|          encodings|            minValue|          maxValue|columnStart|compressedBytes|uncompressedBytes|values|nulls|
+-------------+-----+------+------+-------------------+-------------------+--------------------+------------------+-----------+---------------+-----------------+------+-----+
|file1.parquet|    1|  [id]|SNAPPY|  required int64 id|[BIT_PACKED, PLAIN]|                   0|                99|          4|            437|              826|   100|    0|
|file1.parquet|    1| [val]|SNAPPY|required double val|[BIT_PACKED, PLAIN]|0.005067503372006343|0.9973357672164814|        441|            831|              826|   100|    0|
|file2.parquet|    1|  [id]|SNAPPY|  required int64 id|[BIT_PACKED, PLAIN]|                 100|               199|          4|            438|              825|   100|    0|
|file2.parquet|    1| [val]|SNAPPY|required double val|[BIT_PACKED, PLAIN]|0.010617521596503865| 0.999189783846449|        442|            831|              826|   100|    0|
|file2.parquet|    2|  [id]|SNAPPY|  required int64 id|[BIT_PACKED, PLAIN]|                 200|               299|       1273|            440|              826|   100|    0|
|file2.parquet|    2| [val]|SNAPPY|required double val|[BIT_PACKED, PLAIN]|0.011277044401634018| 0.970525681750662|       1713|            830|              825|   100|    0|
+-------------+-----+------+------+-------------------+-------------------+--------------------+------------------+-----------+---------------+-----------------+------+-----+
```

|column            |type         |description                                           |
|:-----------------|:-----------:|:-----------------------------------------------------|
|filename          |string       |The Parquet file name                                 |
|block             |int          |Block / RowGroup number starting at 1                 |
|column            |array<string>|Block / RowGroup column name                          |
|codec             |string       |The coded used to compress the block column values    |
|type              |string       |The data type of the block column                     |
|encodings         |array<string>|Encodings of the block column                         |
|minValue          |string       |Minimum value of this column in this block            |
|maxValue          |string       |Maximum value of this column in this block            |
|columnStart       |long         |Start position of the block column in the Parquet file|
|compressedBytes   |long         |Number of compressed bytes of this block column       |
|uncompressedBytes |long         |Number of uncompressed bytes of this block column     |
|values            |long         |Number of values in this block column                 |
|nulls             |long         |Number of null values in this block column            |

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
+---------+-----+----+------+------+---------------+-----------------+----+-------+------+-----+-------------+----------+
|partition|start| end|length|blocks|compressedBytes|uncompressedBytes|rows|columns|values|nulls|     filename|fileLength|
+---------+-----+----+------+------+---------------+-----------------+----+-------+------+-----+-------------+----------+
|        1|    0|1024|  1024|     1|           1268|             1652| 100|      2|   200|    0|file1.parquet|      1930|
|        2| 1024|1930|   906|     0|              0|                0|   0|      0|     0|    0|file1.parquet|      1930|
|        3|    0|1024|  1024|     1|           1269|             1651| 100|      2|   200|    0|file2.parquet|      3493|
|        4| 1024|2048|  1024|     1|           1270|             1651| 100|      2|   200|    0|file2.parquet|      3493|
|        5| 2048|3072|  1024|     0|              0|                0|   0|      0|     0|    0|file2.parquet|      3493|
|        6| 3072|3493|   421|     0|              0|                0|   0|      0|     0|    0|file2.parquet|      3493|
+---------+-----+----+------+------+---------------+-----------------+----+-------+------+-----+-------------+----------+
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
|columns          |int   |The number of columns in this partition                   |
|values           |long  |The number of values in this partition                    |
|nulls            |long  |The number of null values in this partition               |
|filename         |string|The Parquet file name                                     |
|fileLength       |long  |The length of the Parquet file                            |

## Performance

Retrieving Parquet metadata is parallelized and distributed by Spark. The result Dataframe
has as many partitions as there are Parquet files in the given `path`, but at most
`spark.sparkContext.defaultParallelism` partitions.

Each result partition reads Parquet metadata from its Parquet files sequentially,
while partitions are executed in parallel (depending on the number of Spark cores of your Spark job).

You can control the number of partitions via the `parallelism` parameter:

```scala
// Scala
spark.read.parquetMetadata(100, "/path/to/parquet")
spark.read.parquetSchema(100, "/path/to/parquet")
spark.read.parquetBlocks(100, "/path/to/parquet")
spark.read.parquetBlockColumns(100, "/path/to/parquet")
spark.read.parquetPartitions(100, "/path/to/parquet")
```
```python
# Python
spark.read.parquet_metadata("/path/to/parquet", parallelism=100)
spark.read.parquet_schema("/path/to/parquet", parallelism=100)
spark.read.parquet_blocks("/path/to/parquet", parallelism=100)
spark.read.parquet_block_columns("/path/to/parquet", parallelism=100)
spark.read.parquet_partitions("/path/to/parquet", parallelism=100)
```

## Encryption

Reading [encrypted Parquet is supported](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#columnar-encryption).
Files encrypted with [plaintext footer](https://github.com/apache/parquet-format/blob/master/Encryption.md#55-plaintext-footer-mode)
can be read without any encryption keys, while encrypted Parquet metadata are then show as `NULL` values in the result Dataframe.
Encrypted Parquet files with encrypted footer requires the footer encryption key only. No column encryption keys are needed.

## Known Issues

Note that this feature is not supported in Python when connected with a [Spark Connect server](README.md#spark-connect-server).
