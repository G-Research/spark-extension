# DataFrame Transformations

The Spark `Dataset` API allows for chaining transformations as in the following example:

```scala
ds.where($"id" === 1)
  .withColumn("state", lit("new"))
  .orderBy($"timestamp")
```

When you define additional transformation functions, the `Dataset` API allows you to
also fluently call into those:

```scala
def transformation(df: DataFrame): DataFrame = df.distinct

ds.transform(transformation)
```

Here are some methods that extend this principle to conditional calls.

## Conditional Transformations

You can run a transformation after checking a condition with a chain of fluent transformation calls:

```scala
import uk.co.gresearch._

val condition = true

val result =
  ds.where($"id" === 1)
    .withColumn("state", lit("new"))
    .when(condition).call(transformation)
    .orderBy($"timestamp")
```

rather than

```scala
val condition = true

val filteredDf = ds.where($"id" === 1)
                   .withColumn("state", lit("new"))
val condDf = if (condition) ds.call(transformation) else ds
val result = ds.orderBy($"timestamp")
```

In case you need an else transformation as well, try:

```scala
import uk.co.gresearch._

val condition = true

val result =
  ds.where($"id" === 1)
    .withColumn("state", lit("new"))
    .on(condition).either(transformation).or(other)
    .orderBy($"timestamp")
```

## Fluent and conditional functions elsewhere

The same fluent notation works for instances other than `Dataset` or `DataFrame`, e.g.
for the `DataFrameWriter`:

```scala
def writeData[T](writer: DataFrameWriter[T]): Unit = { ... }

ds.write
  .when(compress).call(_.option("compression", "gzip"))
  .call(writeData)
```
