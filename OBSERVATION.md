# Observation

Spark's Dataset API allows observing aggregation functions on a Dataset while it is being evaluated:

```scala
val ds: Dataset[T]
ds.observe("stats", count($"id"), sum($"value")).collect()
```

This allows to collect statistics about the data that flew through the aggregate functions
(here `count` and `sum`) during action execution (here `collect`).

However, this requires to register a `QueryExecutionListener` in order to collect the aggregation results,
which further requires some inter-thread communication.

You can use the `Observation` class to simplify access to those aggregation results:

```scala
val observation = Observation(count($"id"), sum($"value"))
ds.observe(observation).collect()

val stats: Row = observation.get
println(s"count=${stats.getLong(0)}")
println(s"sum=${stats.getLong(1)}")
```

The `Observation` class takes care of registering the listener, synchronizing threads and retrieving
the results.

There are two ways to use the `Observation`, both are equivalent:

```scala
ds.observe(observation)
observation.observe(ds)
```

# Observation in Python

The `Observation` class is available in Python as well:

```python
import pyspark.sql.functions as func
from gresearch.spark import Observation

df = spark.createDataFrame([
    (1, 1.0, 'one'),
    (2, 2.0, 'two'),
    (3, 3.0, 'three'),
], ['id', 'val', 'label'])

with Observation(
    func.count(func.lit(1)).alias('cnt'),
    func.sum(func.col("id")).alias('sum'),
    func.mean(func.col("val")).alias('mean')
) as observation:
    observed = df.observe(observation)
    observed.collect()

    stats = observation.get
    print(f'count={row.cnt}')
    print(f'sum={row.sum}')
    print(f'mean={row.mean}')
```
