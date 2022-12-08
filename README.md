# Spark Extension

This project provides extensions to the [Apache Spark project](https://spark.apache.org/) in Scala and Python:

**[Diff](DIFF.md):** A `diff` transformation for `Dataset`s that computes the differences between
two datasets, i.e. which rows to _add_, _delete_ or _change_ to get from one dataset to the other.

**[SortedGroups](GROUPS.md):** A `groupByKey` transformation that groups rows by a key while providing
a **sorted** iterator for each group. Similar to `Dataset.groupByKey.flatMapGroups`, but with order guarantees
for the iterator.

**[Histogram](HISTOGRAM.md):** A `histogram` transformation that computes the histogram DataFrame for a value column.

**[Global Row Number](ROW_NUMBER.md):** A `withRowNumbers` transformation that provides the global row number w.r.t.
the current order of the Dataset, or any given order. In contrast to the existing SQL function `row_number`, which
works requires a window spec, this transformation provides the row number across the entire Dataset without scaling problems.

**[Partitioned Writing](PARTITIONING.md):** The `writePartitionedBy` action writes your `Dataset` partitioned and
efficiently laid out with a single operation.

**[Fluent method call](CONDITIONAL.md):** `T.call(transformation: T => R): R`: Turns a transformation `T => R`,
that is not part of `T` into a fluent method call on `T`. This allows writing fluent code like:

```scala
import uk.co.gresearch._

i.doThis()
 .doThat()
 .call(transformation)
 .doMore()
```

**[Fluent conditional method call](CONDITIONAL.md):** `T.when(condition: Boolean).call(transformation: T => T): T`:
Perform a transformation fluently only if the given condition is true.
This allows writing fluent code like:

```scala
import uk.co.gresearch._

i.doThis()
 .doThat()
 .when(condition).call(transformation)
 .doMore()
```

**Backticks:** `backticks(string: String, strings: String*): String)`: Encloses the given column name with backticks (`` ` ``) when needed.
This is a handy way to ensure column names with special characters like dots (`.`) work with `col()` or `select()`.


## Using Spark Extension

The `spark-extension` package is available for all Spark 3.0, 3.1, 3.2 and 3.3 versions. The package version
has the following semantics: `spark-extension_{SCALA_COMPAT_VERSION}-{VERSION}-{SPARK_COMPAT_VERSION}`:

- `SCALA_COMPAT_VERSION`: Scala binary compatibility (minor) version. Available are `2.12` and `2.13`.
- `SPARK_COMPAT_VERSION`: Apache Spark binary compatibility (minor) version. Available are `3.0`, `3.1`, `3.2` and `3.3`.
- `VERSION`: The package version, e.g. `2.1.0`.

### SBT

Add this line to your `build.sbt` file:

```sbt
libraryDependencies += "uk.co.gresearch.spark" %% "spark-extension" % "2.4.0-3.3"
```

### Maven

Add this dependency to your `pom.xml` file:

```xml
<dependency>
  <groupId>uk.co.gresearch.spark</groupId>
  <artifactId>spark-extension_2.12</artifactId>
  <version>2.4.0-3.3</version>
</dependency>
```

### Spark Shell

Launch a Spark Shell with the Spark Extension dependency (version ≥1.1.0) as follows:

```shell script
spark-shell --packages uk.co.gresearch.spark:spark-extension_2.12:2.4.0-3.3
```

Note: Pick the right Scala version (here 2.12) and Spark version (here 3.3) depending on your Spark Shell version.

### Python

#### PySpark API

Start a PySpark session with the Spark Extension dependency (version ≥1.1.0) as follows:

```python
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "uk.co.gresearch.spark:spark-extension_2.12:2.4.0-3.3") \
    .getOrCreate()
```

Note: Pick the right Scala version (here 2.12) and Spark version (here 3.3) depending on your PySpark version.

#### PySpark REPL

Launch the Python Spark REPL with the Spark Extension dependency (version ≥1.1.0) as follows:

```shell script
pyspark --packages uk.co.gresearch.spark:spark-extension_2.12:2.4.0-3.3
```

Note: Pick the right Scala version (here 2.12) and Spark version (here 3.3) depending on your PySpark version.

#### PySpark `spark-submit`

Run your Python scripts that use PySpark via `spark-submit`:

```shell script
spark-submit --packages uk.co.gresearch.spark:spark-extension_2.12:2.4.0-3.3 [script.py]
```

Note: Pick the right Scala version (here 2.12) and Spark version (here 3.3) depending on your Spark version.

### Your favorite Data Science notebook

There are plenty of [Data Science notebooks](https://datasciencenotebook.org/) around. To use this library,
add **a jar dependency** to your notebook using these **Maven coordinates**:

    uk.co.gresearch.spark:spark-extension_2.12:2.4.0-3.3

Or [download the jar](https://mvnrepository.com/artifact/uk.co.gresearch.spark/spark-extension) and place it
on a filesystem where it is accessible by the notebook, and reference that jar file directly.

Check the documentation of your favorite notebook to learn how to add jars to your Spark environment.

## Build

You can build this project against different versions of Spark and Scala.

### Switch Spark and Scala version

If you want to build for a Spark or Scala version different to what is defined in the `pom.xml` file, then run

```shell script
sh set-version.sh [SPARK-VERSION] [SCALA-VERSION]
```

For example, switch to Spark 3.3.0 and Scala 2.13.8 by running `sh set-version.sh 3.3.0 2.13.8`.

### Build the Scala project

Then execute `mvn package` to create a jar from the sources. It can be found in `target/`.

## Testing

Run the Scala tests via `mvn test`.

### Setup Python environment

In order to run the Python tests, setup a Python environment as follows (replace `[SCALA-COMPAT-VERSION]` and `[SPARK-COMPAT-VERSION]` with the respective values):

```shell script
virtualenv -p python3 venv
source venv/bin/activate
pip install -r python/requirements-[SPARK-COMPAT-VERSION]_[SCALA-COMPAT-VERSION].txt
pip install pytest
```

### Run Python tests

Run the Python tests via `env PYTHONPATH=python:python/test python -m pytest python/test`.
