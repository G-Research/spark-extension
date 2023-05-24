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

from contextlib import contextmanager
from typing import Any, Union, List, Optional, Mapping

from py4j.java_gateway import JVMView, JavaObject
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.storagelevel import StorageLevel


def _to_seq(jvm: JVMView, list: List[Any]) -> JavaObject:
    array = jvm.java.util.ArrayList(list)
    return jvm.scala.collection.JavaConverters.asScalaIteratorConverter(array.iterator()).asScala().toSeq()


def _to_map(jvm: JVMView, map: Mapping[Any, Any]) -> JavaObject:
    return jvm.scala.collection.JavaConverters.mapAsScalaMap(map)


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


def set_description(description: str, if_not_set: bool = False):
    context = SparkContext._active_spark_context
    jvm = context._jvm
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
    jvm = context._jvm
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
