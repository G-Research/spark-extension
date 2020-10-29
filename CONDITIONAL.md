# DataFrame Transformations

The Spark `DataFrame` API allows for chaining transformations as in the following example:

    df.where($"id" === 1)
      .withColumn("state", lit("new"))
      .orderBy($"timestamp")

When you define additional transformations, then you cannot call them in the same fluent way
without extra effort (like an implicit class wrapping the DataFrame):

    def transformation(df: DataFrame): DataFrame = df.distinct

Here are some methods that reduce that extra effort for you.

# Additional Transformations

You can run a transformation on a DataFrame like this:

    df.call(transformation)

This nicely integrate with a chain of fluent transformation calls:

    df.where($"id" === 1)
      .withColumn("state", lit("new"))
      .call(transformation)
      .orderBy($"timestamp")

rather than

    transformation(
      df.where($"id" === 1)
        .withColumn("state", lit("new"))
    ).orderBy($"timestamp")

# Conditional Transformations

If you want to perform any of the transformations only if a condition is true,
then your you have to break that chaining and the code becomes harder to read:

    val condition = true

    val filteredDf = df.where($"id" === 1)
                       .withColumn("state", lit("new"))
    val condDf = if (condition) df.call(transformation) else df
    val result = df.orderBy($"timestamp")

You can conditionally call a transformations like this:

    import uk.co.gresearch.spark._

    val condition = true

    val result =
      df.where($"id" === 1)
        .withColumn("state", lit("new"))
        .when(condition).call(transformation)
        .orderBy($"timestamp")

In case you need an else transformation as well, try this:

    import uk.co.gresearch.spark._

    val condition = true

    val result =
      df.where($"id" === 1)
        .withColumn("state", lit("new"))
        .when(condition).call(transformation)
        .when(!condition).call(other)
        .orderBy($"timestamp")
