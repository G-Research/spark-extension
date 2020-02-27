# Spark Diff

This project adds a `diff` transformation to `Dataset` that computes the differences between the
two datasets, i.e. which rows of `this` dataset to _add_, _delete_ or _change_ to get to the given
dataset.

For example, with
```
val left = Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "value")
val right = Seq((1, "one"), (2, "Two"), (4, "four")).toDF("id", "value")
```
diffing becomes as easy as:
```
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
```
left.diff(right, "id").show()
```
|diff| id|left_value|right_value|
|----|---|----------|-----------|
|   N|  1|       one|        one|
|   C|  2|       two|        Two|
|   D|  3|     three|       null|
|   I|  4|      null|       four|


Equivalent alternative is this hand-crafted transformation
```
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
```
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
