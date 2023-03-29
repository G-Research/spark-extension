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

from typing import Any, Union, List, Optional, Mapping

from py4j.java_gateway import JVMView, JavaObject
from pyspark.sql import DataFrame
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.context import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.storagelevel import StorageLevel


def _to_seq(jvm: JVMView, list: List[Any]) -> JavaObject:
    array = jvm.java.util.ArrayList(list)
    return jvm.scala.collection.JavaConverters.asScalaIteratorConverter(array.iterator()).asScala().toSeq()


def _to_map(jvm: JVMView, map: Mapping[Any, Any]) -> JavaObject:
    return jvm.scala.collection.JavaConverters.mapAsScalaMap(map)


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

    sc = SparkContext._active_spark_context
    if sc is None or sc._jvm is None:
        raise RuntimeError("This method must be called inside an active Spark session")

    func = sc._jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").dotNetTicksToTimestamp
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

    sc = SparkContext._active_spark_context
    if sc is None or sc._jvm is None:
        raise RuntimeError("This method must be called inside an active Spark session")

    func = sc._jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").dotNetTicksToUnixEpoch
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

    sc = SparkContext._active_spark_context
    if sc is None or sc._jvm is None:
        raise RuntimeError("This method must be called inside an active Spark session")

    func = sc._jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").dotNetTicksToUnixEpochNanos
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

    sc = SparkContext._active_spark_context
    if sc is None or sc._jvm is None:
        raise RuntimeError("This method must be called inside an active Spark session")

    func = sc._jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").timestampToDotNetTicks
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

    sc = SparkContext._active_spark_context
    if sc is None or sc._jvm is None:
        raise RuntimeError("This method must be called inside an active Spark session")

    func = sc._jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").unixEpochToDotNetTicks
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

    sc = SparkContext._active_spark_context
    if sc is None or sc._jvm is None:
        raise RuntimeError("This method must be called inside an active Spark session")

    func = sc._jvm.uk.co.gresearch.spark.__getattr__("package$").__getattr__("MODULE$").unixEpochNanosToDotNetTicks
    return Column(func(_to_java_column(unix_column)))


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

    jvm = self._sc._jvm
    col = jvm.org.apache.spark.sql.functions.col
    value_column = col(value_column)
    aggregate_columns = [col(column) for column in aggregate_columns]

    hist = jvm.uk.co.gresearch.spark.Histogram
    jdf = hist.of(self._jdf, _to_seq(jvm, thresholds), value_column, _to_seq(jvm, aggregate_columns))
    return DataFrame(jdf, self.session_or_ctx())


DataFrame.histogram = histogram


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
    jvm = self._sc._jvm
    handle = jvm.uk.co.gresearch.spark.UnpersistHandle()
    return UnpersistHandle(handle)


SparkSession.unpersist_handle = unpersist_handle


def with_row_numbers(self: DataFrame,
                     storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK,
                     unpersist_handle: Optional[UnpersistHandle] = None,
                     row_number_column_name: str = "row_number",
                     order: Union[str, Column, List[Union[str, Column]]] = [],
                     ascending: Union[bool, List[bool]] = True) -> DataFrame:
    jvm = self._sc._jvm
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


def session_or_ctx(self: DataFrame) -> Union[SparkSession, SQLContext]:
    return self.sparkSession if hasattr(self, 'sparkSession') else self.sql_ctx


DataFrame.with_row_numbers = with_row_numbers
DataFrame.session_or_ctx = session_or_ctx
