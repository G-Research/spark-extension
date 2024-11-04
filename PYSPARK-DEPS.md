# PySpark dependencies

Using PySpark on a cluster requires all cluster nodes to have those Python packages installed that are required by the PySpark job.
Such a deployment can be cumbersome, especially when running in an interactive notebook.

The `spark-extension` package allows installing Python packages programmatically by the PySpark application itself (PySpark ≥ 3.1.0).
These packages are only accessible by that PySpark application, and they are removed on calling `spark.stop()`.

Either install the `spark-extension` Maven package, or the `pyspark-extension` PyPi package (on the driver only),
as described [here](README.md#using-spark-extension).

## Installing packages with `pip`

Python packages can be installed with `pip` as follows:

```python
# noinspection PyUnresolvedReferences
from gresearch.spark import *

spark.install_pip_package("pandas", "pyarrow")
```

Above example installs PIP packages `pandas` and `pyarrow` via `pip`. Method `install_pip_package` takes any `pip` command line argument:

```python
# install packages with version specs
spark.install_pip_package("pandas==1.4.3", "pyarrow~=8.0.0")

# install packages from package sources (e.g. git clone https://github.com/pandas-dev/pandas.git)
spark.install_pip_package("./pandas/")

# install packages from git repo
spark.install_pip_package("git+https://github.com/pandas-dev/pandas.git@main")

# use a pip cache directory to cache downloaded and built whl files
spark.install_pip_package("pandas", "pyarrow", "--cache-dir", "/home/user/.cache/pip")

# use an alternative index url (other than https://pypi.org/simple)
spark.install_pip_package("pandas", "pyarrow", "--index-url", "https://artifacts.company.com/pypi/simple")

# install pip packages quietly (only disables output of PIP)
spark.install_pip_package("pandas", "pyarrow", "--quiet")
```

## Installing Python projects with Poetry

Python projects can be installed from sources, including their dependencies, using [Poetry](https://python-poetry.org/):

```python
# noinspection PyUnresolvedReferences
from gresearch.spark import *

spark.install_poetry_project("../my-poetry-project/", poetry_python="../venv-poetry/bin/python")
```

## Example

This example uses `install_pip_package` in a Spark standalone cluster.

First checkout the example code:

```shell
git clone https://github.com/G-Research/spark-extension.git
cd spark-extension/examples/python-deps
```

Build a Docker image based on the official Spark release:
```shell
docker build -t spark-extension-example-docker .
```

Start the example Spark standalone cluster consisting of a Spark master and one worker:
```shell
docker compose -f docker-compose.yml up -d
```

Run the `example.py` Spark application on the example cluster:
```shell
docker exec spark-master spark-submit --master spark://master:7077 --packages uk.co.gresearch.spark:spark-extension_2.12:2.13.0-3.5 /example/example.py
```
The `--packages uk.co.gresearch.spark:spark-extension_2.12:2.13.0-3.5` argument
tells `spark-submit` to add the `spark-extension` Maven package to the Spark job.

Alternatively, install the `pyspark-extension` PyPi package via `pip install` and remove the `--packages` argument from `spark-submit`:
```shell
docker exec spark-master pip install --user pyspark_extension==2.11.1.3.5
docker exec spark-master spark-submit --master spark://master:7077 /example/example.py
```

This output proves that PySpark could call into the function `func`, wich only works when Pandas and PyArrow are installed:
```
+---+
| id|
+---+
|  0|
|  1|
|  2|
+---+
```

Test that `spark.install_pip_package("pandas", "pyarrow")` is really required by this example by removing this line from `example.py` …
```diff
 from pyspark.sql import SparkSession

 def main():
     spark = SparkSession.builder.appName("spark_app").getOrCreate()

     def func(df):
         return df

     from gresearch.spark import install_pip_package

-    spark.install_pip_package("pandas", "pyarrow")
     spark.range(0, 3, 1, 5).mapInPandas(func, "id long").show()

 if __name__ == "__main__":
     main()
```

… and running the `spark-submit` command again. The example does not work anymore,
because the Pandas and PyArrow packages are missing from the driver:
```
Traceback (most recent call last):
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/pandas/utils.py", line 27, in require_minimum_pandas_version
ModuleNotFoundError: No module named 'pandas'
```

Finally, shutdown the example cluster:
```shell
docker compose -f docker-compose.yml down
```

## Known Issues

Note that this feature is not supported in Python when connected with a [Spark Connect server](README.md#spark-connect-server).
