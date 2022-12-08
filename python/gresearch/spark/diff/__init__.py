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
import dataclasses
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Mapping, Any, Callable

from py4j.java_gateway import JavaObject, JVMView
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from gresearch.spark import _to_seq, _to_map
from gresearch.spark.diff.comparator import DiffComparator, DiffComparators, DefaultDiffComparator


class DiffMode(Enum):
    ColumnByColumn = "ColumnByColumn"
    SideBySide = "SideBySide"
    LeftSide = "LeftSide"
    RightSide = "RightSide"

    # the actual default enum value is defined in Java
    Default = "Default"

    def _to_java(self, jvm: JVMView) -> JavaObject:
        return jvm.uk.co.gresearch.spark.diff.DiffMode.withNameOption(self.name).get()


@dataclass(frozen=True)
class DiffOptions:
    """
    Configuration class for diffing Datasets.

    :param diff_column: name of the diff column
    :type diff_column: str
    :param left_column_prefix: prefix of columns from the left Dataset
    :type left_column_prefix: str
    :param right_column_prefix: prefix of columns from the right Dataset
    :type right_column_prefix: str
    :param insert_diff_value: value in diff column for inserted rows
    :type insert_diff_value: str
    :param change_diff_value: value in diff column for changed rows
    :type change_diff_value: str
    :param delete_diff_value: value in diff column for deleted rows
    :type delete_diff_value: str
    :param nochange_diff_value: value in diff column for un-changed rows
    :type nochange_diff_value: str
    :param change_column: name of change column
    :type change_column: str
    :param diff_mode: diff mode
    :type diff_mode: DiffMode
    :param sparse_mode: sparse mode
    :type sparse_mode: bool
    """
    diff_column: str = 'diff'
    left_column_prefix: str = 'left'
    right_column_prefix: str = 'right'
    insert_diff_value: str = 'I'
    change_diff_value: str = 'C'
    delete_diff_value: str = 'D'
    nochange_diff_value: str = 'N'
    change_column: Optional[str] = None
    diff_mode: DiffMode = DiffMode.Default
    sparse_mode: bool = False
    default_comparator: DiffComparator = DefaultDiffComparator()
    data_type_comparators: Dict[DataType, DiffComparator] = dataclasses.field(default_factory=lambda: dict())
    column_name_comparators: Dict[str, DiffComparator] = dataclasses.field(default_factory=lambda: dict())

    def with_diff_column(self, diff_column: str) -> 'DiffOptions':
        """
        Fluent method to change the diff column name.
        Returns a new immutable DiffOptions instance with the new diff column name.

        :param diff_column: new diff column name
        :type diff_column: str
        :return: new immutable DiffOptions instance
        :rtype: DiffOptions
        """
        return dataclasses.replace(self, diff_column=diff_column)

    def with_left_column_prefix(self, left_column_prefix: str) -> 'DiffOptions':
        """
        Fluent method to change the prefix of columns from the left Dataset.
        Returns a new immutable DiffOptions instance with the new column prefix.

        :param left_column_prefix: new column prefix
        :type left_column_prefix: str
        :return: new immutable DiffOptions instance
        :rtype: DiffOptions
        """
        return dataclasses.replace(self, left_column_prefix=left_column_prefix)

    def with_right_column_prefix(self, right_column_prefix: str) -> 'DiffOptions':
        """
        Fluent method to change the prefix of columns from the right Dataset.
        Returns a new immutable DiffOptions instance with the new column prefix.

        :param right_column_prefix: new column prefix
        :type right_column_prefix: str
        :return: new immutable DiffOptions instance
        :rtype: DiffOptions
        """
        return dataclasses.replace(self, right_column_prefix=right_column_prefix)

    def with_insert_diff_value(self, insert_diff_value: str) -> 'DiffOptions':
        """
        Fluent method to change the value of inserted rows in the diff column.
        Returns a new immutable DiffOptions instance with the new diff value.

        :param insert_diff_value: new diff value
        :type insert_diff_value: str
        :return: new immutable DiffOptions instance
        :rtype: DiffOptions
        """
        return dataclasses.replace(self, insert_diff_value=insert_diff_value)

    def with_change_diff_value(self, change_diff_value: str) -> 'DiffOptions':
        """
        Fluent method to change the value of changed rows in the diff column.
        Returns a new immutable DiffOptions instance with the new diff value.

        :param change_diff_value: new diff column name
        :type change_diff_value: str
        :return: new immutable DiffOptions instance
        :rtype: DiffOptions
        """
        return dataclasses.replace(self, change_diff_value=change_diff_value)

    def with_delete_diff_value(self, delete_diff_value: str) -> 'DiffOptions':
        """
        Fluent method to change the value of deleted rows in the diff column.
        Returns a new immutable DiffOptions instance with the new diff value.

        :param delete_diff_value: new diff column name
        :type delete_diff_value: str
        :return: new immutable DiffOptions instance
        :rtype: DiffOptions
        """
        return dataclasses.replace(self, delete_diff_value=delete_diff_value)

    def with_nochange_diff_value(self, nochange_diff_value: str) -> 'DiffOptions':
        """
        Fluent method to change the value of un-changed rows in the diff column.
        Returns a new immutable DiffOptions instance with the new diff value.

        :param nochange_diff_value: new diff column name
        :type nochange_diff_value: str
        :return: new immutable DiffOptions instance
        :rtype: DiffOptions
        """
        return dataclasses.replace(self, nochange_diff_value=nochange_diff_value)

    def with_change_column(self, change_column: str) -> 'DiffOptions':
        """
        Fluent method to change the change column name.
        Returns a new immutable DiffOptions instance with the new change column name.

        :param change_column: new change column name
        :type change_column: str
        :return: new immutable DiffOptions instance
        :rtype: DiffOptions
        """
        return dataclasses.replace(self, change_column=change_column)

    def without_change_column(self) -> 'DiffOptions':
        """
        Fluent method to remove change column.
        Returns a new immutable DiffOptions instance without a change column.

        :return: new immutable DiffOptions instance
        :rtype: DiffOptions
        """
        return dataclasses.replace(self, change_column=None)

    def with_diff_mode(self, diff_mode: DiffMode) -> 'DiffOptions':
        """
        Fluent method to change the diff mode.
        Returns a new immutable DiffOptions instance with the new diff mode.

        :param diff_mode: new diff mode
        :type diff_mode: DiffMode
        :return: new immutable DiffOptions instance
        :rtype: DiffOptions
        """
        return dataclasses.replace(self, diff_mode=diff_mode)

    def with_sparse_mode(self, sparse_mode: bool) -> 'DiffOptions':
        """
        Fluent method to change the sparse mode.
        Returns a new immutable DiffOptions instance with the new sparse mode.

        :param sparse: new sparse mode
        :type sparse: bool
        :return: new immutable DiffOptions instance
        :rtype: DiffOptions
        """
        return dataclasses.replace(self, sparse_mode=sparse_mode)

    def with_default_comparator(self, comparator: DiffComparator) -> 'DiffOptions':
        return dataclasses.replace(self, default_comparator=comparator)

    def with_data_type_comparator(self, comparator: DiffComparator, *data_type: DataType) -> 'DiffOptions':
        existing_data_types = {dt.simpleString() for dt in data_type if dt in self.data_type_comparators.keys()}
        if existing_data_types:
            existing_data_types = sorted(list(existing_data_types))
            raise ValueError(f'A comparator for data type{"s" if len(existing_data_types) > 1 else ""} '
                             f'{", ".join(existing_data_types)} exists already.')

        data_type_comparators = self.data_type_comparators.copy()
        data_type_comparators.update({dt: comparator for dt in data_type})
        return dataclasses.replace(self, data_type_comparators=data_type_comparators)

    def with_column_name_comparator(self, comparator: DiffComparator, *column_name: str) -> 'DiffOptions':
        existing_column_names = {cn for cn in column_name if cn in self.column_name_comparators.keys()}
        if existing_column_names:
            existing_column_names = sorted(list(existing_column_names))
            raise ValueError(f'A comparator for column name{"s" if len(existing_column_names) > 1 else ""} '
                             f'{", ".join(existing_column_names)} exists already.')

        column_name_comparators = self.column_name_comparators.copy()
        column_name_comparators.update({dt: comparator for dt in column_name})
        return dataclasses.replace(self, column_name_comparators=column_name_comparators)

    def _to_java(self, jvm: JVMView) -> JavaObject:
        return jvm.uk.co.gresearch.spark.diff.DiffOptions(
            self.diff_column,
            self.left_column_prefix,
            self.right_column_prefix,
            self.insert_diff_value,
            self.change_diff_value,
            self.delete_diff_value,
            self.nochange_diff_value,
            jvm.scala.Option.apply(self.change_column),
            self.diff_mode._to_java(jvm),
            self.sparse_mode,
            self.default_comparator._to_java(jvm),
            self._to_java_map(jvm, self.data_type_comparators, key_to_java=self._to_java_data_type),
            self._to_java_map(jvm, self.column_name_comparators)
        )

    def _to_java_map(self, jvm: JVMView, map: Mapping[Any, DiffComparator], key_to_java: Callable[[JVMView, Any], Any] = lambda j, x: x) -> JavaObject:
        return _to_map(jvm, {key_to_java(jvm, key): cmp._to_java(jvm) for key, cmp in map.items()})

    def _to_java_data_type(self, jvm: JVMView, dt: DataType) -> JavaObject:
        return jvm.org.apache.spark.sql.types.DataType.fromJson(dt.json())


class Differ:
    """
    Differ class to diff two Datasets. See Differ.of(…) for details.

    :param options: options for the diffing process
    :type options: DiffOptions
    """
    def __init__(self, options: DiffOptions = None):
        self._options = options or DiffOptions()

    def _to_java(self, jvm: JVMView) -> JavaObject:
        jdo = self._options._to_java(jvm)
        return jvm.uk.co.gresearch.spark.diff.Differ(jdo)

    def diff(self, left: DataFrame, right: DataFrame, *id_columns: str) -> DataFrame:
        """
        Returns a new DataFrame that contains the differences between the two DataFrames.

        Both DataFrames must contain the same set of column names and data types.
        The order of columns in the two DataFrames is not important as columns are compared based on the
        name, not the the position.

        Optional id columns are used to uniquely identify rows to compare. If values in any non-id
        column are differing between the two DataFrames, then that row is marked as `"C"`hange
        and `"N"`o-change otherwise. Rows of the right DataFrame, that do not exist in the left DataFrame
        (w.r.t. the values in the id columns) are marked as `"I"`nsert. And rows of the left DataFrame,
        that do not exist in the right DataFrame are marked as `"D"`elete.

        If no id columns are given, all columns are considered id columns. Then, no `"C"`hange rows
        will appear, as all changes will exists as respective `"D"`elete and `"I"`nsert.

        The returned DataFrame has the `diff` column as the first column. This holds the `"N"`, `"C"`,
        `"I"` or `"D"` strings. The id columns follow, then the non-id columns (all remaining columns).

        .. code-block:: python

          df1 = spark.createDataFrame([(1, "one"), (2, "two"), (3, "three")], ["id", "value"])
          df2 = spark.createDataFrame([(1, "one"), (2, "Two"), (4, "four")], ["id", "value"])
       
          differ.diff(df1, df2).show()
       
          // output:
          // +----+---+-----+
          // |diff| id|value|
          // +----+---+-----+
          // |   N|  1|  one|
          // |   D|  2|  two|
          // |   I|  2|  Two|
          // |   D|  3|three|
          // |   I|  4| four|
          // +----+---+-----+
       
          differ.diff(df1, df2, "id").show()
       
          // output:
          // +----+---+----------+-----------+
          // |diff| id|left_value|right_value|
          // +----+---+----------+-----------+
          // |   N|  1|       one|        one|
          // |   C|  2|       two|        Two|
          // |   D|  3|     three|       null|
          // |   I|  4|      null|       four|
          // +----+---+----------+-----------+

        The id columns are in order as given to the method. If no id columns are given then all
        columns of this DataFrame are id columns and appear in the same order. The remaining non-id
        columns are in the order of this DataFrame.

        :param left: left DataFrame
        :type left: DataFrame
        :param right: right DataFrame
        :type right: DataFrame
        :param id_columns: optional id column names
        :type id_columns: str
        :return: the diff DataFrame
        :rtype DataFrame
        """
        jvm = left._sc._jvm
        jdiffer = self._to_java(jvm)
        jdf = jdiffer.diff(left._jdf, right._jdf, _to_seq(jvm, list(id_columns)))
        return DataFrame(jdf, left.session_or_ctx())

    def diffwith(self, left: DataFrame, right: DataFrame, *id_columns: str) -> DataFrame:
        """
        Returns a new DataFrame that contains the differences between the two DataFrames
        as tuples of type `(String, Row, Row)`.

        See `diff(left: DataFrame, right: DataFrame, *id_columns: str)`.

        :param left: left DataFrame
        :type left: DataFrame
        :param right: right DataFrame
        :type right: DataFrame
        :param id_columns: optional id column names
        :type id_columns: str
        :return: the diff DataFrame
        :rtype DataFrame
        """
        jvm = left._sc._jvm
        jdiffer = self._to_java(jvm)
        jdf = jdiffer.diffWith(left._jdf, right._jdf, _to_seq(jvm, list(id_columns)))
        df = DataFrame(jdf, left.sql_ctx)
        return df \
            .withColumnRenamed('_1', self._options.diff_column) \
            .withColumnRenamed('_2', self._options.left_column_prefix) \
            .withColumnRenamed('_3', self._options.right_column_prefix)


def diff(self: DataFrame, other: DataFrame, *id_columns: str) -> DataFrame:
    """
    Returns a new DataFrame that contains the differences between this and the other DataFrame.
    Both DataFrames must contain the same set of column names and data types.
    The order of columns in the two DataFrames is not important as one column is compared to the
    column with the same name of the other DataFrame, not the column with the same position.

    Optional id columns are used to uniquely identify rows to compare. If values in any non-id
    column are differing between this and the other DataFrame, then that row is marked as `"C"`hange
    and `"N"`o-change otherwise. Rows of the other DataFrame, that do not exist in this DataFrame
    (w.r.t. the values in the id columns) are marked as `"I"`nsert. And rows of this DataFrame, that
    do not exist in the other DataFrame are marked as `"D"`elete.

    If no id columns are given, all columns are considered id columns. Then, no `"C"`hange rows
    will appear, as all changes will exists as respective `"D"`elete and `"I"`nsert.

    The returned DataFrame has the `diff` column as the first column. This holds the `"N"`, `"C"`,
    `"I"` or `"D"` strings. The id columns follow, then the non-id columns (all remaining columns).

    .. code-block:: python

      df1 = spark.createDataFrame([(1, "one"), (2, "two"), (3, "three")], ["id", "value"])
      df2 = spark.createDataFrame([(1, "one"), (2, "Two"), (4, "four")], ["id", "value"])

      df1.diff(df2).show()

      // output:
      // +----+---+-----+
      // |diff| id|value|
      // +----+---+-----+
      // |   N|  1|  one|
      // |   D|  2|  two|
      // |   I|  2|  Two|
      // |   D|  3|three|
      // |   I|  4| four|
      // +----+---+-----+

      df1.diff(df2, "id").show()

      // output:
      // +----+---+----------+-----------+
      // |diff| id|left_value|right_value|
      // +----+---+----------+-----------+
      // |   N|  1|       one|        one|
      // |   C|  2|       two|        Two|
      // |   D|  3|     three|       null|
      // |   I|  4|      null|       four|
      // +----+---+----------+-----------+

    The id columns are in order as given to the method. If no id columns are given then all
    columns of this DataFrame are id columns and appear in the same order. The remaining non-id
    columns are in the order of this DataFrame.

    :param other: right DataFrame
    :type other: DataFrame
    :param id_columns: optional id column names
    :type id_columns: str
    :return: the diff DataFrame
    :rtype DataFrame
    """
    return Differ().diff(self, other, *id_columns)


def diffwith(self: DataFrame, other: DataFrame, *id_columns: str) -> DataFrame:
    """
    Returns a new DataFrame that contains the differences between the two DataFrames
    as tuples of type `(String, Row, Row)`.

    See `diff(left: DataFrame, right: DataFrame, *id_columns: str)`.

    :param left: left DataFrame
    :type left: DataFrame
    :param right: right DataFrame
    :type right: DataFrame
    :param id_columns: optional id column names
    :type id_columns: str
    :return: the diff DataFrame
    :rtype DataFrame
    """
    return Differ().diffwith(self, other, *id_columns)


def diff_with_options(self: DataFrame, other: DataFrame, options: DiffOptions, *id_columns: str) -> DataFrame:
    """
    Returns a new DataFrame that contains the differences between this and the other DataFrame.

    See `diff(other: DataFrame, *id_columns: str)`.

    The schema of the returned DataFrame can be configured by the given `DiffOptions`.

    :param other: right DataFrame
    :type other: DataFrame
    :param id_columns: optional id column names
    :type id_columns: str
    :param options: diff options
    :type options: DiffOptions
    :return: the diff DataFrame
    :rtype DataFrame
    """
    return Differ(options).diff(self, other, *id_columns)


def diffwith_with_options(self: DataFrame, other: DataFrame, options: DiffOptions, *id_columns: str) -> DataFrame:
    """
    Returns a new DataFrame that contains the differences between the two DataFrames
    as tuples of type `(String, Row, Row)`.

    See `diff(left: DataFrame, right: DataFrame, *id_columns: str)`.

    The schema of the returned DataFrame can be configured by the given `DiffOptions`.

    :param other: right DataFrame
    :type other: DataFrame
    :param id_columns: optional id column names
    :type id_columns: str
    :param options: diff options
    :type options: DiffOptions
    :return: the diff DataFrame
    :rtype DataFrame
    """
    return Differ(options).diffwith(self, other, *id_columns)


DataFrame.diff = diff
DataFrame.diffwith = diffwith

DataFrame.diff_with_options = diff_with_options
DataFrame.diffwith_with_options = diffwith_with_options
