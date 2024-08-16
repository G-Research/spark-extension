#  Copyright 2020 G-Research
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Union, List, Optional, Mapping, Iterable, TYPE_CHECKING

from py4j.java_gateway import JVMView, JavaObject
from pyspark import __version__
from pyspark.context import SparkContext
from pyspark.files import SparkFiles
from pyspark.sql import DataFrame, DataFrameReader, SQLContext
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.context import SQLContext
from pyspark import SparkConf
from pyspark.sql.functions import col, count, lit, when
from pyspark.sql.session import SparkSession
from pyspark.storagelevel import StorageLevel

try:
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.sql.connect.readwriter import DataFrameReader as ConnectDataFrameReader
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
    has_connect = True
except ImportError:
    has_connect = False

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName

_java_pkg_is_installed: Optional[bool] = None


def _check_java_pkg_is_installed(jvm: JVMView) -> bool:
    """Check that the Java / Scala package is installed."""
    try:
        jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").VersionString()
        return True
    except TypeError as e:
        print(e.args)
        return False
    except:
        # any other exception indicate some problem, be safe and do not fail fast here
        return True


def _get_jvm(obj: Any) -> JVMView:
    """
    Provides easy access to the JVMView provided by Spark, and raises meaningful error message if that is not available.
    Also checks that the Java / Scala package is accessible via this JVMView.
    """
    if obj is None:
        if SparkContext._active_spark_context is None:
            raise RuntimeError("This method must be called inside an active Spark session")
        else:
            raise ValueError("Cannot provide access to JVM from None")

    if has_connect and isinstance(obj, (ConnectDataFrame, ConnectDataFrameReader, ConnectSparkSession)):
        raise RuntimeError('This feature is not supported for Spark Connect. Please use a classic Spark client. '
                           'https://github.com/G-Research/spark-extension#spark-connect-server')

    if isinstance(obj, DataFrame):
        jvm = _get_jvm(obj._sc)
    elif isinstance(obj, DataFrameReader):
        jvm = _get_jvm(obj._spark)
    elif isinstance(obj, SparkSession):
        jvm = _get_jvm(obj.sparkContext)
    elif isinstance(obj, (SparkContext, SQLContext)):
        jvm = obj._jvm
    else:
        raise RuntimeError(f'Unsupported class: {type(obj)}')

    global _java_pkg_is_installed
    if _java_pkg_is_installed is None:
        _java_pkg_is_installed = _check_java_pkg_is_installed(jvm)
    if not _java_pkg_is_installed:
        raise RuntimeError("Java / Scala package not found! You need to add the Maven spark-extension package "
                           "to your PySpark environment: https://github.com/G-Research/spark-extension#python")

    return jvm


def _to_seq(jvm: JVMView, list: List[Any]) -> JavaObject:
    array = jvm.java.util.ArrayList(list)
    return jvm.scala.collection.JavaConverters.asScalaIteratorConverter(array.iterator()).asScala().toSeq()


def _to_map(jvm: JVMView, map: Mapping[Any, Any]) -> JavaObject:
    return jvm.scala.collection.JavaConverters.mapAsScalaMap(map)


def backticks(*name_parts: str) -> str:
    return '.'.join([f'`{part}`'
                     if '.' in part and not part.startswith('`') and not part.endswith('`')
                     else part
                     for part in name_parts])


def distinct_prefix_for(existing: List[str]) -> str:
    # count number of suffix _ for each existing column name
    length = 1
    if existing:
        length = max([len(name) - len(name.lstrip('_')) for name in existing]) + 1
    # return string with one more _ than that
    return '_' * length


def handle_configured_case_sensitivity(column_name: str, case_sensitive: bool) -> str:
    """
    Produces a column name that considers configured case-sensitivity of column names. When case sensitivity is
    deactivated, it lower-cases the given column name and no-ops otherwise.
    """
    if case_sensitive:
        return column_name
    return column_name.lower()


def list_contains_case_sensitivity(column_names: Iterable[str], columnName: str, case_sensitive: bool) -> bool:
    return handle_configured_case_sensitivity(columnName, case_sensitive) in [handle_configured_case_sensitivity(c, case_sensitive) for c in column_names]


def list_filter_case_sensitivity(column_names: Iterable[str], filter: Iterable[str], case_sensitive: bool) -> List[str]:
    filter_set = {handle_configured_case_sensitivity(f, case_sensitive) for f in filter}
    return [c for c in column_names if handle_configured_case_sensitivity(c, case_sensitive) in filter_set]


def list_diff_case_sensitivity(column_names: Iterable[str], other: Iterable[str], case_sensitive: bool) -> List[str]:
    other_set = {handle_configured_case_sensitivity(f, case_sensitive) for f in other}
    return [c for c in column_names if handle_configured_case_sensitivity(c, case_sensitive) not in other_set]


def dotnet_ticks_to_timestamp(tick_column: Union[str, Column]) -> Column:
    """
    Convert a .Net `DateTime.Ticks` timestamp to a Spark timestamp. The input column must be
    convertible to a number (e.g. string, int, long). The Spark timestamp type does not support
    nanoseconds, so the the last digit of the timestamp (1/10 of a microsecond) is lost.
    {{{
      df.select(col("ticks"), dotNetTicksToTimestamp("ticks").alias("timestamp")).show(false)
    }}}
    +------------------+--------------------------+
    |ticks             |timestamp                 |
    +------------------+--------------------------+
    |638155413748959318|2023-03-27 21:16:14.895931|
    +------------------+--------------------------+

    Note: the example timestamp lacks the 8/10 of a microsecond. Use `dotNetTicksToUnixEpoch` to
    preserve the full precision of the tick timestamp.

    https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks

    :param tick_column: column with a tick value (str or Column)
    :return: timestamp column
    """
    if not isinstance(tick_column, (str, Column)):
        raise ValueError(f"Given column must be a column name (str) or column instance (Column): {type(tick_column)}")

    jvm = _get_jvm(SparkContext._active_spark_context)
    func = jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").dotNetTicksToTimestamp
    return Column(func(_to_java_column(tick_column)))


def dotnet_ticks_to_unix_epoch(tick_column: Union[str, Column]) -> Column:
    """
    Convert a .Net `DateTime.Ticks` timestamp to a Unix epoch decimal. The input column must be
    convertible to a number (e.g. string, int, long). The full precision of the tick timestamp
    is preserved (1/10 of a microsecond).

    Example:
    {{{
      df.select(col("ticks"), dotNetTicksToUnixEpoch("ticks").alias("timestamp")).show(false)
    }}}

    +------------------+--------------------+
    |ticks             |timestamp           |
    +------------------+--------------------+
    |638155413748959318|1679944574.895931800|
    +------------------+--------------------+

    https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks

    :param tick_column: column with a tick value (str or Column)
    :return: Unix epoch column
    """
    if not isinstance(tick_column, (str, Column)):
        raise ValueError(f"Given column must be a column name (str) or column instance (Column): {type(tick_column)}")

    jvm = _get_jvm(SparkContext._active_spark_context)
    func = jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").dotNetTicksToUnixEpoch
    return Column(func(_to_java_column(tick_column)))


def dotnet_ticks_to_unix_epoch_nanos(tick_column: Union[str, Column]) -> Column:
    """
    Convert a .Net `DateTime.Ticks` timestamp to a Unix epoch nanoseconds. The input column must be
    convertible to a number (e.g. string, int, long). The full precision of the tick timestamp
    is preserved (1/10 of a microsecond).

    Example:
    {{{
      df.select(col("ticks"), dotNetTicksToUnixEpoch("ticks").alias("timestamp")).show(false)
    }}}

    +------------------+-------------------+
    |ticks             |timestamp          |
    +------------------+-------------------+
    |638155413748959318|1679944574895931800|
    +------------------+-------------------+

    https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks

    :param tick_column: column with a tick value (str or Column)
    :return: Unix epoch column
    """
    if not isinstance(tick_column, (str, Column)):
        raise ValueError(f"Given column must be a column name (str) or column instance (Column): {type(tick_column)}")

    jvm = _get_jvm(SparkContext._active_spark_context)
    func = jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").dotNetTicksToUnixEpochNanos
    return Column(func(_to_java_column(tick_column)))


def timestamp_to_dotnet_ticks(timestamp_column: Union[str, Column]) -> Column:
    """
    Convert a Spark timestamp to a .Net `DateTime.Ticks` timestamp.
    The input column must be of TimestampType.

    Example:
    {{{
      df.select(col("timestamp"), timestampToDotNetTicks("timestamp").alias("ticks")).show(false)
    }}}

    +--------------------------+------------------+
    |timestamp                 |ticks             |
    +--------------------------+------------------+
    |2023-03-27 21:16:14.895931|638155413748959310|
    +--------------------------+------------------+

    https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks

    :param timestamp_column: column with a timestamp value
    :return: tick value column
    """
    if not isinstance(timestamp_column, (str, Column)):
        raise ValueError(f"Given column must be a column name (str) or column instance (Column): {type(timestamp_column)}")

    jvm = _get_jvm(SparkContext._active_spark_context)
    func = jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").timestampToDotNetTicks
    return Column(func(_to_java_column(timestamp_column)))


def unix_epoch_to_dotnet_ticks(unix_column: Union[str, Column]) -> Column:
    """
    Convert a Unix epoch timestamp to a .Net `DateTime.Ticks` timestamp.
    The input column must represent a numerical unix epoch timestamp, e.g. long, double, string or decimal.
    The input must not be of TimestampType, as that may be interpreted incorrectly.
    Use `timestampToDotNetTicks` for TimestampType columns instead.

    Example:
    {{{
      df.select(col("unix"), unixEpochToDotNetTicks("unix").alias("ticks")).show(false)
    }}}

    +-----------------------------+------------------+
    |unix                         |ticks             |
    +-----------------------------+------------------+
    |1679944574.895931234000000000|638155413748959312|
    +-----------------------------+------------------+

    https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks

    :param unix_column: column with a unix epoch value
    :return: tick value column
    """
    if not isinstance(unix_column, (str, Column)):
        raise ValueError(f"Given column must be a column name (str) or column instance (Column): {type(unix_column)}")

    jvm = _get_jvm(SparkContext._active_spark_context)
    func = jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").unixEpochToDotNetTicks
    return Column(func(_to_java_column(unix_column)))


def unix_epoch_nanos_to_dotnet_ticks(unix_column: Union[str, Column]) -> Column:
    """
    Convert a Unix epoch nanosecond timestamp to a .Net `DateTime.Ticks` timestamp.
    The .Net ticks timestamp does not support the two lowest nanosecond digits,
    so only a 1/10 of a microsecond is the smallest resolution.
    The input column must represent a numerical unix epoch nanoseconds timestamp,
    e.g. long, double, string or decimal.

    Example:
    {{{
      df.select(col("unix_nanos"), unixEpochNanosToDotNetTicks("unix_nanos").alias("ticks")).show(false)
    }}}

    +-------------------+------------------+
    |unix_nanos         |ticks             |
    +-------------------+------------------+
    |1679944574895931234|638155413748959312|
    +-------------------+------------------+

    https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks

    :param unix_column: column with a unix epoch value
    :return: tick value column
    """
    if not isinstance(unix_column, (str, Column)):
        raise ValueError(f"Given column must be a column name (str) or column instance (Column): {type(unix_column)}")

    jvm = _get_jvm(SparkContext._active_spark_context)
    func = jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").unixEpochNanosToDotNetTicks
    return Column(func(_to_java_column(unix_column)))


def count_null(e: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the number of items in a group that are not null.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.
    """
    if isinstance(e, str):
        e = col(e)
    return count(when(e.isNull(), lit(1)))


def histogram(self: DataFrame,
              thresholds: List[Union[int, float]],
              value_column: str,
              *aggregate_columns: str) -> DataFrame:

    if len(thresholds) == 0:
        t = 'Int'
    else:
        t = type(thresholds[0])
        if t == int:
            t = 'Int'
        elif t == float:
            t = 'Double'
        else:
            raise ValueError('thresholds must be int or floats: {}'.format(t))

    jvm = _get_jvm(self)
    col = jvm.org.apache.spark.sql.functions.col
    value_column = col(value_column)
    aggregate_columns = [col(column) for column in aggregate_columns]

    hist = jvm.uk.co.gresearch.spark.Histogram
    jdf = hist.of(self._jdf, _to_seq(jvm, thresholds), value_column, _to_seq(jvm, aggregate_columns))
    return DataFrame(jdf, self.session_or_ctx())


DataFrame.histogram = histogram
if has_connect:
    ConnectDataFrame.histogram = histogram


class UnpersistHandle:
    def __init__(self, handle):
        self._handle = handle

    def __call__(self, blocking: Optional[bool] = None):
        if self._handle is not None:
            if blocking is None:
                self._handle.apply()
            else:
                self._handle.apply(blocking)


def unpersist_handle(self: SparkSession) -> UnpersistHandle:
    jvm = _get_jvm(self)
    handle = jvm.uk.co.gresearch.spark.UnpersistHandle()
    return UnpersistHandle(handle)


SparkSession.unpersist_handle = unpersist_handle


def with_row_numbers(self: DataFrame,
                     storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK,
                     unpersist_handle: Optional[UnpersistHandle] = None,
                     row_number_column_name: str = "row_number",
                     order: Union[str, Column, List[Union[str, Column]]] = [],
                     ascending: Union[bool, List[bool]] = True) -> DataFrame:
    jvm = _get_jvm(self)
    jsl = self._sc._getJavaStorageLevel(storage_level)
    juho = jvm.uk.co.gresearch.spark.UnpersistHandle
    juh = unpersist_handle._handle if unpersist_handle else juho.Noop()
    jcols = self._sort_cols([order], {'ascending': ascending}) if not isinstance(order, list) or order else jvm.PythonUtils.toSeq([])

    row_numbers = jvm.uk.co.gresearch.spark.RowNumbers
    jdf = row_numbers \
        .withRowNumberColumnName(row_number_column_name) \
        .withStorageLevel(jsl) \
        .withUnpersistHandle(juh) \
        .withOrderColumns(jcols) \
        .of(self._jdf)

    return DataFrame(jdf, self.session_or_ctx())


DataFrame.with_row_numbers = with_row_numbers
if has_connect:
    ConnectDataFrame.with_row_numbers = with_row_numbers


def session(self: DataFrame) -> SparkSession:
    return self.sparkSession if hasattr(self, 'sparkSession') else self.sql_ctx.sparkSession


def session_or_ctx(self: DataFrame) -> Union[SparkSession, SQLContext]:
    return self.sparkSession if hasattr(self, 'sparkSession') else self.sql_ctx


DataFrame.session = session
DataFrame.session_or_ctx = session_or_ctx
if has_connect:
    ConnectDataFrame.session = session
    ConnectDataFrame.session_or_ctx = session_or_ctx


def set_description(description: str, if_not_set: bool = False):
    context = SparkContext._active_spark_context
    jvm = _get_jvm(context)
    spark_package = jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$")
    return spark_package.setJobDescription(description, if_not_set, context._jsc.sc())


@contextmanager
def job_description(description: str, if_not_set: bool = False):
    """
    Adds a job description to all Spark jobs started within this context.
    The current Job description is restored after leaving the context.

    Usage example:

    >>> from gresearch.spark import job_description
    >>>
    >>> with job_description("parquet file"):
    ...     df = spark.read.parquet("data.parquet")
    ...     count = df.count

    With ``if_not_set = True``, the description is only set if no job description is set yet.

    Any modification to the job description within the context is reverted on exit,
    even if `if_not_set = True`.

    :param description: job description
    :param if_not_set: job description is only set if no description is set yet
    """
    earlier = set_description(description, if_not_set)
    try:
        yield
    finally:
        set_description(earlier)


def append_description(extra_description: str, separator: str = " - "):
    context = SparkContext._active_spark_context
    jvm = _get_jvm(context)
    spark_package = jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$")
    return spark_package.appendJobDescription(extra_description, separator, context._jsc.sc())


@contextmanager
def append_job_description(extra_description: str, separator: str = " - "):
    """
    Appends a job description to all Spark jobs started within this context.
    The current Job description is extended by the separator and the extra description
    on entering the context, and restored after leaving the context.

    Usage example:

    >>> from gresearch.spark import append_job_description
    >>>
    >>> with append_job_description("parquet file"):
    ...     df = spark.read.parquet("data.parquet")
    ...     with append_job_description("count"):
    ...         count = df.count

    Any modification to the job description within the context is reverted on exit.

    :param extra_description: job description to be appended
    :param separator: separator used when appending description
    """
    earlier = append_description(extra_description, separator)
    try:
        yield
    finally:
        set_description(earlier)


def create_temporary_dir(spark: Union[SparkSession, SparkContext], prefix: str) -> str:
    """
    Create a temporary directory in a location (driver temp dir) that will be deleted on Spark application shutdown.
    :param spark: spark session or context
    :param prefix: prefix string of temporary directory name
    :return: absolute path of temporary directory
    """
    jvm = _get_jvm(spark)
    root_dir = jvm.org.apache.spark.SparkFiles.getRootDirectory()
    return tempfile.mkdtemp(prefix=prefix, dir=root_dir)


SparkSession.create_temporary_dir = create_temporary_dir
SparkContext.create_temporary_dir = create_temporary_dir

if has_connect:
    ConnectSparkSession.create_temporary_dir = create_temporary_dir


def install_pip_package(spark: Union[SparkSession, SparkContext], *package_or_pip_option: str) -> None:
    if __version__.startswith('2.') or __version__.startswith('3.0.'):
        raise NotImplementedError(f'Not supported for PySpark __version__')

    # just here to assert JVM is accessible
    _get_jvm(spark)

    if isinstance(spark, SparkSession):
        spark = spark.sparkContext

    # create temporary directory for packages, inside a directory which will be deleted on spark application shutdown
    id = f"spark-extension-pip-pkgs-{time.time()}"
    dir = spark.create_temporary_dir(f"{id}-")

    # install packages via pip install
    # it is best to run pip as a separate process and not calling into module pip
    # https://pip.pypa.io/en/stable/user_guide/#using-pip-from-your-program
    subprocess.check_call([sys.executable, '-m', 'pip', "install"] + list(package_or_pip_option) + ["--target", dir])

    # zip packages and remove directory
    zip = shutil.make_archive(dir, "zip", dir)
    shutil.rmtree(dir)

    # register zip file as archive, and add as python source
    # once support for Spark 3.0 is dropped, replace with spark.addArchive()
    spark._jsc.sc().addArchive(zip + "#" + id)
    spark._python_includes.append(id)
    sys.path.insert(1, os.path.join(SparkFiles.getRootDirectory(), id))


SparkSession.install_pip_package = install_pip_package
SparkContext.install_pip_package = install_pip_package

if has_connect:
    ConnectSparkSession.install_pip_package = install_pip_package


def install_poetry_project(spark: Union[SparkSession, SparkContext],
                           *project: str,
                           poetry_python: Optional[str] = None,
                           pip_args: Optional[List[str]] = None) -> None:
    import logging
    logger = logging.getLogger()

    # spark.install_pip_dependency has this limitation, and it is used by this method
    # and we want to fail quickly here
    if __version__.startswith('2.') or __version__.startswith('3.0.'):
        raise NotImplementedError(f'Not supported for PySpark __version__')

    # just here to assert JVM is accessible
    _get_jvm(spark)

    if isinstance(spark, SparkSession):
        spark = spark.sparkContext
    if poetry_python is None:
        poetry_python = sys.executable
    if pip_args is None:
        pip_args = []

    def check_and_log_poetry(proc: subprocess.CompletedProcess) -> List[str]:
        stdout = proc.stdout.decode('utf-8').splitlines(keepends=False)
        for line in stdout:
            logger.info(f"poetry: {line}")

        stderr = proc.stderr.decode('utf-8').splitlines(keepends=False)
        for line in stderr:
            logger.error(f"poetry: {line}")

        if proc.returncode != 0:
            raise RuntimeError(f'Poetry process terminated with exit code {proc.returncode}')

        return stdout

    def build_wheel(project: Path) -> Path:
        logger.info(f"Running poetry using {poetry_python}")

        # make sure the virtual env for this project exists, otherwise we won't get to see the build whl file in stdout
        proc = subprocess.run([
            poetry_python, '-m', 'poetry',
            'env', 'use',
            '--directory', str(project.absolute()),
            sys.executable
        ], capture_output=True)
        check_and_log_poetry(proc)

        # build the whl file
        proc = subprocess.run([
            poetry_python, '-m', 'poetry',
            'build',
            '--verbose',
            '--no-interaction',
            '--format', 'wheel',
            '--directory', str(project.absolute())
        ], capture_output=True)
        stdout = check_and_log_poetry(proc)

        # first matching line is taken to extract whl file name
        whl_pattern = "^  - Built (.*.whl)$"
        for line in stdout:
            if match := re.match(whl_pattern, line):
                return project.joinpath('dist', match.group(1))

        raise RuntimeError(f'Could not find wheel file name in poetry output, was looking for "{whl_pattern}"')

    wheels = [build_wheel(Path(path)) for path in project]

    # install wheels via pip
    spark.install_pip_package(*[str(whl.absolute()) for whl in wheels] + pip_args)


SparkSession.install_poetry_project = install_poetry_project
SparkContext.install_poetry_project = install_poetry_project

if has_connect:
    ConnectSparkSession.install_poetry_project = install_poetry_project
