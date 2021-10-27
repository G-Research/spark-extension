# Spark Diff

Add the following `import` to your Scala code:

```scala
import uk.co.gresearch.spark.diff._
```

or this `import` to your Python code:

```python
from gresearch.spark.diff import *
```

This adds a `diff` transformation to `Dataset` and `DataFrame` that computes the differences between two datasets / dataframes,
i.e. which rows of one dataset / dataframes to _add_, _delete_ or _change_ to get to the other dataset / dataframes.

For example, in Scala

```scala
val left = Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "value")
val right = Seq((1, "one"), (2, "Two"), (4, "four")).toDF("id", "value")
```

or in Python:

```python
left = spark.createDataFrame([(1, "one"), (2, "two"), (3, "three")], ["id", "value"])
right = spark.createDataFrame([(1, "one"), (2, "Two"), (4, "four")], ["id", "value"])
```

diffing becomes as easy as:

```scala
left.diff(right).show()
```

|diff |id   |value  |
|:---:|:---:|:-----:|
|    N|    1|    one|
|    D|    2|    two|
|    I|    2|    Two|
|    D|    3|  three|
|    I|    4|   four|

With columns that provide unique identifiers per row (here `id`), the diff looks like:

```scala
left.diff(right, "id").show()
```

|diff |id   |left_value|right_value|
|:---:|:---:|:--------:|:---------:|
|    N|    1|       one|        one|
|    C|    2|       two|        Two|
|    D|    3|     three|     *null*|
|    I|    4|    *null*|       four|


Equivalent alternative is this hand-crafted transformation (Scala)

```scala
left.withColumn("exists", lit(1)).as("l")
  .join(right.withColumn("exists", lit(1)).as("r"),
    $"l.id" <=> $"r.id",
    "fullouter")
  .withColumn("diff",
    when($"l.exists".isNull, "I").
      when($"r.exists".isNull, "D").
      when(!($"l.value" <=> $"r.value"), "C").
      otherwise("N"))
  .show()
```

Statistics on the differences can be obtained by

```scala
left.diff(right, "id").groupBy("diff").count().show()
```

|diff  |count  |
|:----:|:-----:|
|     N|      1|
|     I|      1|
|     D|      1|
|     C|      1|

The `diff` transformation can optionally provide a *change column* that lists all non-id column names that have changed.
This column is an array of strings and only set for `"N"` and `"C"`action rows; it is *null* for `"I"` and `"D"`action rows.

|diff |changes|id   |left_value|right_value|
|:---:|:-----:|:---:|:--------:|:---------:|
|    N|     []|    1|       one|        one|
|    C|[value]|    2|       two|        Two|
|    D| *null*|    3|     three|     *null*|
|    I| *null*|    4|    *null*|       four|

## Features

This `diff` transformation provides the following features:
* id columns are optional
* provides typed `diffAs` transformations
* supports *null* values in id and non-id columns
* detects *null* value insertion / deletion
* configurable via `DiffOptions`:
  * diff column name (default: `"diff"`), if default name exists in diff result schema
  * diff action labels (defaults: `"N"`, `"I"`, `"D"`, `"C"`), allows custom diff notation,
e.g. Unix diff left-right notation (<, >) or git before-after format (+, -, -+)
  * different diff result formats
  * sparse diffing mode
* optionally provides a *change column* that lists all non-id column names that have changed (only for `"C"` action rows)
* guarantees that no duplicate columns exist in the result, throws a readable exception otherwise

## Configuring Diff

Diffing can be configured via an optional `DiffOptions` instance (see [Methods](#methods) below).

|option              |default  |description|
|--------------------|:-------:|-----------|
|`diffColumn`        |`"diff"` |The 'diff column' provides the action or diff value encoding if the respective row has been inserted, changed, deleted or has not been changed at all.|
|`leftColumnPrefix`  |`"left"` |Non-id columns of the 'left' dataset are prefixed with this prefix.|
|`rightColumnPrefix` |`"right"`|Non-id columns of the 'right' dataset are prefixed with this prefix.|
|`insertDiffValue`   |`"I"`    |Inserted rows are marked with this string in the 'diff column'.|
|`changeDiffValue`   |`"C"`    |Changed rows are marked with this string in the 'diff column'.|
|`deleteDiffValue`   |`"D"`    |Deleted rows are marked with this string in the 'diff column'.|
|`nochangeDiffValue` |`"N"`    |Unchanged rows are marked with this string in the 'diff column'.|
|`changeColumn`      |*none*   |An array with the names of all columns that have changed values is provided in this column (only for unchanged and changed rows, *null* otherwise).|
|`diffMode`          |`DiffModes.Default`|Configures the diff output format. For details see [Diff Modes](#diff-modes) section below.|
|`sparseMode`        |`false`  |When `true`, only values that have changed are provided on left and right side, `null` is used for un-changed values.|

Either construct an instance via the constructor …

```scala
val options = DiffOptions("d", "l", "r", "i", "c", "d", "n", Some("changes"), DiffModes.Default, false)
```

… or via the `.with*` methods. The former requires all options to be specified, whereas the latter
only requires the ones that deviate from the default, and it is more readable.

Start from the default options `DiffOptions.default` and customize as follows:

```scala
val options = DiffOptions.default
  .withDiffColumn("d")
  .withLeftColumnPrefix("l")
  .withRightColumnPrefix("r")
  .withInsertDiffValue("i")
  .withChangeDiffValue("c")
  .withDeleteDiffValue("d")
  .withNochangeDiffValue("n")
  .withChangeColumn("changes")
  .withDiffMode(DiffModes.Default)
  .withSparseMode(true)
```

### Diffing Modes

The result of the diff transformation can have the following formats:

- *column by column*: The non-id columns are arranged column by column, i.e. for each non-id column
                      there are two columns next to each other in the diff result, one from the left
                      and one from the right dataset. This is useful to easily compare the values
                      for each column.
- *side by side*: The non-id columns from the left and right dataset are are arranged side by side,
                  i.e. first there are all columns from the left dataset, then from the right one.
                  This is useful to visually compare the datasets as a whole, especially in conjunction
                  with the sparse mode.
- *left side*: Only the columns of the left dataset are present in the diff output. This mode
               provides the left dataset as is, annotated with diff action and optional changed column names. 
- *right side*: Only the columns of the right dataset are present in the diff output. This mode
                provides the right dataset as given, as well as the diff action that has been applied to it.
                This serves as a patch that, applied to the left dataset, results in the right dataset.

With the following two datasets `left` and `right`:

```scala
case class Value(id: Int, value: Option[String], label: Option[String])

val left = Seq(
  Value(1, Some("one"), None),
  Value(2, Some("two"), Some("number two")),
  Value(3, Some("three"), Some("number three")),
  Value(4, Some("four"), Some("number four")),
  Value(5, Some("five"), Some("number five")),
).toDS

val right = Seq(
  Value(1, Some("one"), Some("one")),
  Value(2, Some("Two"), Some("number two")),
  Value(3, Some("Three"), Some("number Three")),
  Value(4, Some("four"), Some("number four")),
  Value(6, Some("six"), Some("number six")),
).toDS
```

the diff modes produce the following outputs:

#### Column by Column

|diff |id   |left_value|right_value|left_label  |right_label |
|:---:|:---:|:--------:|:---------:|:----------:|:----------:|
|C    |1    |one       |one        |*null*      |one         |
|C    |2    |two       |Two        |number two  |number two  |
|C    |3    |three     |Three      |number three|number Three|
|N    |4    |four      |four       |number four |number four |
|D    |5    |five      |null       |number five |*null*      |
|I    |6    |*null*    |six        |*null*      |number six  |

#### Side by Side

|diff |id   |left_value|left_label  |right_value|right_label |
|:---:|:---:|:--------:|:----------:|:---------:|:----------:|
|C    |1    |one       |*null*      |one        |one         |
|C    |2    |two       |number two  |Two        |number two  |
|C    |3    |three     |number three|Three      |number Three|
|N    |4    |four      |number four |four       |number four |
|D    |5    |five      |number five |null       |*null*      |
|I    |6    |*null*    |*null*      |six        |number six  |

#### Left Side

|diff |id   |value|label       |
|:---:|:---:|:---:|:----------:|
|C    |1    |one  |null        |
|C    |2    |two  |number two  |
|C    |3    |three|number three|
|N    |4    |four |number four |
|D    |5    |five |number five |
|I    |6    |null |null        |

#### Right Side

|diff |id   |value|label       |
|:---:|:---:|:---:|:----------:|
|C    |1    |one  |one         |
|C    |2    |Two  |number two  |
|C    |3    |Three|number Three|
|N    |4    |four |number four |
|D    |5    |null |null        |
|I    |6    |six  |number six  |

### Sparse Mode

The diff modes above can be combined with sparse mode. In sparse mode, only values that differ between
the two datasets are in the diff result, all other values are `null`.

Above [Column by Column](#column-by-column) example would look in sparse mode as follows:

|diff |id   |left_value|right_value|left_label  |right_label |
|:---:|:---:|:--------:|:---------:|:----------:|:----------:|
|C    |1    |null      |null       |null        |one         |
|C    |2    |two       |Two        |null        |null        |
|C    |3    |three     |Three      |number three|number Three|
|N    |4    |null      |null       |null        |null        |
|D    |5    |five      |null       |number five |null        |
|I    |6    |null      |six        |null        |number six  |


## Methods (Scala)

* `def diff(other: Dataset[T], idColumns: String*): DataFrame`
* `def diff(other: Dataset[T], options: DiffOptions, idColumns: String*): DataFrame`


* `def diffAs[U](other: Dataset[T], idColumns: String*)(implicit diffEncoder: Encoder[U]): Dataset[U]`
* `def diffAs[U](other: Dataset[T], options: DiffOptions, idColumns: String*)(implicit diffEncoder: Encoder[U]): Dataset[U]`
* `def diffAs[U](other: Dataset[T], diffEncoder: Encoder[U], idColumns: String*): Dataset[U]`
* `def diffAs[U](other: Dataset[T], options: DiffOptions, diffEncoder: Encoder[U], idColumns: String*): Dataset[U]`


## Methods (Java)

* `Dataset<Row> Diff.of[T](Dataset<T> left, Dataset<T> right, String... idColumns)`
* `Dataset<U> Diff.ofAs[T, U](Dataset<T> left, Dataset<T> right, Encoder<U> diffEncoder, String... idColumns)`

## Methods (Python)

* `def diff(self: DataFrame, other: DataFrame, *idColumns: str) -> DataFrame`
* `def diff_with_options(self: DataFrame, other: DataFrame, options: DiffOptions, *id_columns: str) -> DataFrame:`
