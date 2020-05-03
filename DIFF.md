# Spark Diff

Add the following import to your code:
```scala
import uk.co.gresearch.spark.diff._
```

This adds a `diff` transformation to `Dataset` that computes the differences between the
two datasets, i.e. which rows of `this` dataset to _add_, _delete_ or _change_ to get to the given
dataset.

For example, with
```scala
val left = Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "value")
val right = Seq((1, "one"), (2, "Two"), (4, "four")).toDF("id", "value")
```
diffing becomes as easy as:
```scala
left.diff(right).show()
```
|diff| id|value|
|----|---|-----|
|   N|  1|  one|
|   D|  2|  two|
|   I|  2|  Two|
|   D|  3|three|
|   I|  4| four|

With columns that provide unique identifiers per row (here `id`), the diff looks like:
```scala
left.diff(right, "id").show()
```
|diff| id|left_value|right_value|
|----|---|----------|-----------|
|   N|  1|       one|        one|
|   C|  2|       two|        Two|
|   D|  3|     three|       null|
|   I|  4|      null|       four|


Equivalent alternative is this hand-crafted transformation
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

| diff | count |
|:----:|:-----:|
|     N|      1|
|     I|      1|
|     D|      1|
|     C|      1|

This `diff` transformation provides the following features:
* id columns are optional
* provides typed `diffAs` transformations
* supports null values in id and non-id columns
* detects null value insertion / deletion
* configurable via `DiffOptions`:
  * diff column name (default: `"diff"`), for diffing datasets that already contain `diff` column
  * diff action labels (defaults: `"N"`, `"I"`, `"D"`, `"C"`), allows custom diff notation,
e.g. Unix diff left-right notation (<, >) or git before-after format (+, -, -+)

## Methods

* `def diff(other: Dataset[T], idColumns: String*): DataFrame`
* `def diff(other: Dataset[T], options: DiffOptions, idColumns: String*): DataFrame`
* `def diffAs[U](other: Dataset[T], idColumns: String*)(implicit diffEncoder: Encoder[U]): Dataset[U]`
* `def diffAs[U](other: Dataset[T], options: DiffOptions, idColumns: String*)(implicit diffEncoder: Encoder[U]): Dataset[U]`
* `def diffAs[U](other: Dataset[T], diffEncoder: Encoder[U], idColumns: String*): Dataset[U]`
* `def diffAs[U](other: Dataset[T], options: DiffOptions, diffEncoder: Encoder[U], idColumns: String*): Dataset[U]`

## User Defined Comparators
Further options were introduced to allow for the substitution of the default equals comparator (<=>), based on the datatype of a column or its name.

In this example the default options are modified to replace the default equals comparator with greaterThan (gt) for all columns of type integer.
In addition, the column 'value' will be compared using the GreaterThanOrEqual comparator (geq),
possibly overwriting the previous comparator assignment, if column 'value' is of type integer.
Please note: the diff action label assigned to the equality or no-change state ('N' in the example above) is used here if the comparator is satisfied.
```scala
    DiffOptions.default
      .withDatatypeComparators(Map(IntegerType -> DiffComparator.GreaterThanComparator))
      .withColumnComparators(Map("value" -> DiffComparator.GreaterThanOrEqualComparator))
```

Defining your own comparators is easy to accomplish by implementing the trait [DiffComparator](src/main/scala/uk/co/gresearch/spark/diff/DiffComparator.scala).
Two examples of which are the fuzzy comparators for numbers and dates (timestampms), which allow for tolerance specification when comparing objects of these types.

Here we assign a fuzzy number comparator to column 'double' with a tolerance of +- 0.1 .
````scala
  DiffOptions.default
    .withColumnComparators(Map("double" -> DiffComparator.FuzzyNumberComparator(0.1)))
````

In this example we assign a fuzzy date comparator to column 'date', allowing the left column timestamps to be in a rang of 5 seconds before and
10 seconds after the timestamp of the right DataFrame. By providing timezone references (optional) we can normalize timestamps to the same
timezone (UTC) before comparing the results.
````scala
  DiffOptions.default
    .withColumnComparators(Map("date" -> DiffComparator.FuzzyDateComparator(5, 10, "CET", "UTC")))
````

In this example the following timestamp pairs would be considered (quasi-) equal:
````scala
("2020-01-01 13:12:12.0", "2020-01-01 12:12:15.0")    // the left column, when normalized to UTC, is in the specified tolerance
("2020-01-01 13:12:12.0", "2020-01-01 12:12:02.0")
````

## Additional options
* _ignoreColumns_: Often, two sources provide similar tables, except for a number of specific column. Columns listed under this option will be dropped
from both DataFrames before comparing them.
* _nullOutValidData_: If true, all cells which are equal, or in a given tolerance range of the comparator used, are replaced with 'null' to increase visibility of erroneous data.
This can be very helpful, especially when dealing with tables having many columns.
