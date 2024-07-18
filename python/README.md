# Spark Extension

This project provides extensions to the [Apache Spark project](https://spark.apache.org/) in Scala and Python:

**[Diff](https://github.com/G-Research/spark-extension/blob/v2.12.0/DIFF.md):** A `diff` transformation and application for `Dataset`s that computes the differences between
two datasets, i.e. which rows to _add_, _delete_ or _change_ to get from one dataset to the other.

**[Histogram](https://github.com/G-Research/spark-extension/blob/v2.12.0/HISTOGRAM.md):** A `histogram` transformation that computes the histogram DataFrame for a value column.

**[Global Row Number](https://github.com/G-Research/spark-extension/blob/v2.12.0/ROW_NUMBER.md):** A `withRowNumbers` transformation that provides the global row number w.r.t.
the current order of the Dataset, or any given order. In contrast to the existing SQL function `row_number`, which
requires a window spec, this transformation provides the row number across the entire Dataset without scaling problems.

**[Inspect Parquet files](https://github.com/G-Research/spark-extension/blob/v2.12.0/PARQUET.md):** The structure of Parquet files (the metadata, not the data stored in Parquet) can be inspected similar to [parquet-tools](https://pypi.org/project/parquet-tools/)
or [parquet-cli](https://pypi.org/project/parquet-cli/) by reading from a simple Spark data source.
This simplifies identifying why some Parquet files cannot be split by Spark into scalable partitions.

**[Install Python packages into PySpark job](https://github.com/G-Research/spark-extension/blob/v2.12.0/PYSPARK-DEPS.md):** Install Python dependencies via PIP or Poetry programatically into your running PySpark job (PySpark ≥ 3.1.0):

```python
# noinspection PyUnresolvedReferences
from gresearch.spark import *

# using PIP
spark.install_pip_package("pandas==1.4.3", "pyarrow")
spark.install_pip_package("-r", "requirements.txt")

# using Poetry
spark.install_poetry_project("../my-poetry-project/", poetry_python="../venv-poetry/bin/python")
```

**Count null values:** `count_null(e: Column)`: an aggregation function like `count` that counts null values in column `e`.
This is equivalent to calling `count(when(e.isNull, lit(1)))`.

**.Net DateTime.Ticks:** Convert .Net (C#, F#, Visual Basic) `DateTime.Ticks` into Spark timestamps, seconds and nanoseconds.

<details>
<summary>Available methods:</summary>

```python
dotnet_ticks_to_timestamp(column_or_name)         # returns timestamp as TimestampType
dotnet_ticks_to_unix_epoch(column_or_name)        # returns Unix epoch seconds as DecimalType
dotnet_ticks_to_unix_epoch_nanos(column_or_name)  # returns Unix epoch nanoseconds as LongType
```

The reverse is provided by (all return `LongType` .Net ticks):
```python
timestamp_to_dotnet_ticks(column_or_name)
unix_epoch_to_dotnet_ticks(column_or_name)
unix_epoch_nanos_to_dotnet_ticks(column_or_name)
```
</details>

**Spark temporary directory**: Create a temporary directory that will be removed on Spark application shutdown.

<details>
<summary>Example:</summary>

```python
# noinspection PyUnresolvedReferences
from gresearch.spark import *

dir = spark.create_temporary_dir("prefix")
```
</details>

**Spark job description:** Set Spark job description for all Spark jobs within a context.

<details>
<summary>Example:</summary>

```python
from gresearch.spark import job_description, append_job_description

with job_description("parquet file"):
    df = spark.read.parquet("data.parquet")
    with append_job_description("count"):
        count = df.count
    with append_job_description("write"):
        df.write.csv("data.csv")
```
</details>

For details, see the [README.md](https://github.com/G-Research/spark-extension#spark-extension) at the project homepage.

## Using Spark Extension

#### PyPi package (local Spark cluster only)

You may want to install the `pyspark-extension` python package from PyPi into your development environment.
This provides you code completion, typing and test capabilities during your development phase.

Running your Python application on a Spark cluster will still require one of the ways below
to add the Scala package to the Spark environment.

```shell script
pip install pyspark-extension==2.12.0.3.4
```

Note: Pick the right Spark version (here 3.4) depending on your PySpark version.

#### PySpark API

Start a PySpark session with the Spark Extension dependency (version ≥1.1.0) as follows:

```python
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "uk.co.gresearch.spark:spark-extension_2.12:2.12.0-3.4") \
    .getOrCreate()
```

Note: Pick the right Scala version (here 2.12) and Spark version (here 3.4) depending on your PySpark version.

#### PySpark REPL

Launch the Python Spark REPL with the Spark Extension dependency (version ≥1.1.0) as follows:

```shell script
pyspark --packages uk.co.gresearch.spark:spark-extension_2.12:2.12.0-3.4
```

Note: Pick the right Scala version (here 2.12) and Spark version (here 3.4) depending on your PySpark version.

#### PySpark `spark-submit`

Run your Python scripts that use PySpark via `spark-submit`:

```shell script
spark-submit --packages uk.co.gresearch.spark:spark-extension_2.12:2.12.0-3.4 [script.py]
```

Note: Pick the right Scala version (here 2.12) and Spark version (here 3.4) depending on your Spark version.

### Your favorite Data Science notebook

There are plenty of [Data Science notebooks](https://datasciencenotebook.org/) around. To use this library,
add **a jar dependency** to your notebook using these **Maven coordinates**:

    uk.co.gresearch.spark:spark-extension_2.12:2.12.0-3.4

Or [download the jar](https://mvnrepository.com/artifact/uk.co.gresearch.spark/spark-extension) and place it
on a filesystem where it is accessible by the notebook, and reference that jar file directly.

Check the documentation of your favorite notebook to learn how to add jars to your Spark environment.

