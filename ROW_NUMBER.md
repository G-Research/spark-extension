# Global Row Number

Spark provides the [SQL function `row_number`](https://spark.apache.org/docs/latest/api/sql/index.html#row_number),
which assigns each row a consecutive number, starting from 1. This function works on a [Window](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/expressions/Window.html).
Assigning a row number over the entire Dataset will load the entire dataset into a single partition / executor.
This does not scale.

Spark extensions provide the `Dataset` transformation `withRowNumbers`, which assigns a global row number while scaling:

```scala
val df = Seq((1, "one"), (2, "TWO"), (2, "two"), (3, "three")).toDF("id", "value")
df.show()
// +---+-----+
// | id|value|
// +---+-----+
// |  1|  one|
// |  2|  TWO|
// |  2|  two|
// |  3|three|
// +---+-----+

import uk.co.gresearch.spark._

df.withRowNumbers().show()
// +---+-----+----------+
// | id|value|row_number|
// +---+-----+----------+
// |  1|  one|         1|
// |  2|  two|         2|
// |  2|  TWO|         3|
// |  3|three|         4|
// +---+-----+----------+
```

In Java:
```java
import uk.co.gresearch.spark.RowNumbers;

RowNumbers.of(df).show();
// +---+-----+----------+
// | id|value|row_number|
// +---+-----+----------+
// |  1|  one|         1|
// |  2|  two|         2|
// |  2|  TWO|         3|
// |  3|three|         4|
// +---+-----+----------+
```

In Python:
```python
import gresearch.spark

df.with_row_numbers().show()
# +---+-----+----------+
# | id|value|row_number|
# +---+-----+----------+
# |  1|  one|         1|
# |  2|  two|         2|
# |  2|  TWO|         3|
# |  3|three|         4|
# +---+-----+----------+
```

## Row number order
Row numbers are assigned in the current order of the Dataset. If you want a specific order, provide columns as follows:

```scala
df.withRowNumbers($"id".desc, $"value").show()
// +---+-----+----------+
// | id|value|row_number|
// +---+-----+----------+
// |  3|three|         1|
// |  2|  TWO|         2|
// |  2|  two|         3|
// |  1|  one|         4|
// +---+-----+----------+
```

In Java:
```java
RowNumbers.withOrderColumns(df.col("id").desc(), df.col("value")).of(df).show();
// +---+-----+----------+
// | id|value|row_number|
// +---+-----+----------+
// |  3|three|         1|
// |  2|  TWO|         2|
// |  2|  two|         3|
// |  1|  one|         4|
// +---+-----+----------+
```

In Python:
```python
df.with_row_numbers(order=[df.id.desc(), df.value]).show()
# +---+-----+----------+
# | id|value|row_number|
# +---+-----+----------+
# |  3|three|         1|
# |  2|  TWO|         2|
# |  2|  two|         3|
# |  1|  one|         4|
# +---+-----+----------+
```

## Row number column name

The column name that contains the row number can be changed by providing the `rowNumberColumnName` argument:

```scala
df.withRowNumbers(rowNumberColumnName="row").show()
// +---+-----+---+
// | id|value|row|
// +---+-----+---+
// |  1|  one|  1|
// |  2|  TWO|  2|
// |  2|  two|  3|
// |  3|three|  4|
// +---+-----+---+
```

In Java:
```java
RowNumbers.withRowNumberColumnName("row").of(df).show();
// +---+-----+---+
// | id|value|row|
// +---+-----+---+
// |  1|  one|  1|
// |  2|  TWO|  2|
// |  2|  two|  3|
// |  3|three|  4|
// +---+-----+---+
```

In Python:
```python
df.with_row_numbers(row_number_column_name='row').show()
# +---+-----+---+
# | id|value|row|
# +---+-----+---+
# |  1|  one|  1|
# |  2|  TWO|  2|
# |  2|  two|  3|
# |  3|three|  4|
# +---+-----+---+
```

## Cached / persisted intermediate Dataset

The `withRowNumbers` transformation requires the input Dataset to be
[cached](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html#cache():Dataset.this.type) /
[persisted](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html#persist(newLevel:org.apache.spark.storage.StorageLevel):Dataset.this.type),
after adding an intermediate column. You can specify the level of persistence through the `storageLevel` parameter.

```scala
import org.apache.spark.storage.StorageLevel

val dfWithRowNumbers = df.withRowNumbers(storageLevel=StorageLevel.DISK_ONLY)
```

In Java:
```java
import org.apache.spark.storage.StorageLevel;

Dataset<Row> dfWithRowNumbers = RowNumbers.withStorageLevel(StorageLevel.DISK_ONLY()).of(df);
```

In Python:
```python
from pyspark.storagelevel import StorageLevel

df_with_row_numbers = df.with_row_numbers(storage_level=StorageLevel.DISK_ONLY)
```

## Un-persist intermediate Dataset

If you want control over when to un-persist this intermediate Dataset, you can provide an `UnpersistHandle` and call it
when you are done with the result Dataset:

```scala
import uk.co.gresearch.spark.UnpersistHandle

val unpersist = UnpersistHandle()
val dfWithRowNumbers = df.withRowNumbers(unpersistHandle=unpersist);

// after you are done with dfWithRowNumbers you may want to call unpersist()
unpersist(blocking=false)
```

In Java:
```java
import uk.co.gresearch.spark.UnpersistHandle;

UnpersistHandle unpersist = new UnpersistHandle();
Dataset<Row> dfWithRowNumbers = RowNumbers.withUnpersistHandle(unpersist).of(df);

// after you are done with dfWithRowNumbers you may want to call unpersist()
unpersist.apply(true);
```

In Python:
```python
unpersist = spark.unpersist_handle()
df_with_row_numbers = df.with_row_numbers(unpersist_handle=unpersist)

# after you are done with df_with_row_numbers you may want to call unpersist()
unpersist(blocking=True)
```

## Spark warning

You will recognize that Spark logs the following warning:

```
WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
```
This warning is unavoidable, because `withRowNumbers` has to pull information about the initial partitions into a single partition.
Fortunately, there are only 12 Bytes per input partition required, so this amount of data usually fits into a single partition and the warning can safely be ignored.

## Known issues

Note that this feature is not supported in Python when connected with a [Spark Connect server](README.md#spark-connect-server).
