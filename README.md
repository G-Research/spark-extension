# Spark Extension

This project provides extensions to the [Apache Spark project](https://spark.apache.org/) in Scala and Python:
- [Diff](DIFF.md): A `diff` transformation for `Dataset`s that computes the differences between
two datasets, i.e. which rows to _add_, _delete_ or _change_ to get from one dataset to the other.
- `backticks(string: String, strings: String*): String)`: Encloses the given column name with backticks (`` ` ``) when needed.
  This is a handy way to ensure column names with special characters like dots (`.`) work with `col()` or `select()`.

## Using Spark Extension

### SBT

Add this line to your `build.sbt` file:

```sbt
libraryDependencies += "uk.co.gresearch.spark" %% "spark-extension" % "1.0.0"
```

### Maven

Add this dependency to your `pom.xml` file:

```xml
<dependency>
  <groupId>uk.co.gresearch.spark</groupId>
  <artifactId>spark-extension_2.12</artifactId>
  <version>1.0.0</version>
</dependency>
```

### Spark Shell

Launch a Spark Shell with the Spark Extension dependency (version ≥1.1.0) as follows:

```shell script
spark-shell --packages uk.co.gresearch.spark:spark-extension_2.12:1.1.0-3.0
```

Note: Pick the right Scala version (here 2.12) and Spark version (here 3.0) depending on your Spark Shell version.

### Python

Launch the Python Spark REPL (pyspark 2.4.2 and ≥3.0) with the Spark Extension dependency (version ≥1.1.0) as follows:

```shell script
pyspark --packages uk.co.gresearch.spark:spark-extension_2.11:1.1.0-2.4  # pyspark != 2.4.2
pyspark --packages uk.co.gresearch.spark:spark-extension_2.12:1.1.0-2.4  # pyspark == 2.4.2
pyspark --packages uk.co.gresearch.spark:spark-extension_2.12:1.1.0-3.0  # pyspark >= 3.0.0
```

Note: Pick the right Scala version and Spark version depending on your PySpark version.

Run your Python scripts that use PySpark (pyspark 2.4.2 and ≥3.0) via `spark-submit`:

```shell script
spark-submit --packages uk.co.gresearch.spark:spark-extension_2.12:1.1.0-3.0 [script.py]
```

Note: Pick the right Scala version (here 2.12) and Spark version (here 3.0) depending on your Spark version.

## Build

You can build this project against different versions of Spark and Scala.

### Switch Spark and Scala version

If you want to build for a Spark or Scala version different to what is defined in the `pom.xml` file, then run

```shell script
sh set-version.sh [SPARK-VERSION] [SCALA-VERSION]
```

For example, switch to Spark 2.4.6 and Scala 2.11.12 by running `sh set-version.sh 2.4.6 2.11.12`.

### Build the Scala project

Then execute `mvn package` to create a jar from the sources. It can be found in `target/`.

## Test

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
