# Histogram

For a table `df` like

|user   |score|
|:-----:|:---:|
|Alice  |101  |
|Alice  |221  |
|Alice  |211  |
|Alice  |176  |
|Bob    |276  |
|Bob    |232  |
|Bon    |258  |
|Charlie|221  |

you can compute the histogram for each user

|user   |≤100 |≤200 |>200 |
|:-----:|:---:|:---:|:---:|
|Alice  |0    |2    |2    |
|Bob    |0    |0    |3    |
|Charlie|0    |0    |1    |

as follows:

    df.withColumn("≤100", when($"score" <= 100, 1).otherwise(0))
      .withColumn("≤200", when($"score" > 100 && $"score" <= 200, 1).otherwise(0))
      .withColumn(">200", when($"score" > 200, 1).otherwise(0))
      .groupBy($"user")
      .agg(
        sum($"≤100").as("≤100"),
        sum($"≤200").as("≤200"),
        sum($">200").as(">200")
      )
      .orderBy($"user")

Equivalent to that query is:

    import uk.co.gresearch.spark._

    df.histogram(Seq(100, 200), $"score", $"user").orderBy($"user")

The first argument is a sequence of thresholds, the second argument provides the value column.
The subsequent arguments refer to the aggregation columns (`groupBy`). Only aggregation columns
will be in the result DataFrame.

In Java, call:

    import uk.co.gresearch.spark.Histogram;

    Histogram.of(df, Arrays.asList(100, 200), new Column("score")), new Column("user")).orderBy($"user")

In Python, call:

    import gresearch.spark

    df.histogram([100, 200], 'user').orderBy('user')
