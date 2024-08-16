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
from functools import reduce
from typing import Optional, Dict, Mapping, Any, Callable, List, Tuple, Union, Iterable, overload

from py4j.java_gateway import JavaObject, JVMView
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, lit, when, concat, coalesce, array, struct
from pyspark.sql.types import DataType, StructField, ArrayType

from gresearch.spark import _get_jvm, _to_seq, _to_map, backticks, distinct_prefix_for, \
    handle_configured_case_sensitivity, list_contains_case_sensitivity, list_filter_case_sensitivity, list_diff_case_sensitivity
from gresearch.spark.diff.comparator import DiffComparator, DiffComparators, DefaultDiffComparator

try:
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    has_connect = True
except ImportError:
    has_connect = False


class DiffMode(Enum):
    ColumnByColumn = "ColumnByColumn"
    SideBySide = "SideBySide"
    LeftSide = "LeftSide"
    RightSide = "RightSide"

    # should be in sync with default defined in Java
    Default = ColumnByColumn

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

    def comparator_for(self, column: StructField) -> DiffComparator:
        cmp = self.column_name_comparators.get(column.name)
        if cmp is None:
            cmp = self.data_type_comparators.get(column.dataType)
        if cmp is None:
            cmp = self.default_comparator
        return cmp


class Differ:
    """
    Differ class to diff two Datasets. See Differ.of(…) for details.

    :param options: options for the diffing process
    :type options: DiffOptions
    """
    def __init__(self, options: DiffOptions = None):
        self._options = options or DiffOptions()

    @overload
    def diff(self, left: DataFrame, right: DataFrame, *id_columns: str) -> DataFrame: ...

    @overload
    def diff(self, left: DataFrame, right: DataFrame, id_columns: Iterable[str], ignore_columns: Iterable[str]) -> DataFrame: ...

    def diff(self, left: DataFrame, right: DataFrame, *id_or_ignore_columns: Union[str, Iterable[str]]) -> DataFrame:
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

        Values in optional ignore columns are not compared but included in the output DataFrame.

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
        :param id_or_ignore_columns: either id column names or two lists of column names,
               first the id column names, second the ignore column names
        :type id_or_ignore_columns: str
        :return: the diff DataFrame
        :rtype DataFrame
        """
        if len(id_or_ignore_columns) == 2 and all([isinstance(lst, Iterable) and not isinstance(lst, str) for lst in id_or_ignore_columns]):
            id_columns, ignore_columns = id_or_ignore_columns
        else:
            id_columns, ignore_columns = (id_or_ignore_columns, [])

        return self._do_diff(left, right, id_columns, ignore_columns)

    @staticmethod
    def _columns_of_side(df: DataFrame, id_columns: List[str], side_prefix: str) -> List[Column]:
        prefix = side_prefix + '_'
        return [col(c) if c in id_columns else col(c).alias(c.replace(prefix, ""))
                for c in df.columns if c in id_columns or c.startswith(side_prefix)]

    @overload
    def diffwith(self, left: DataFrame, right: DataFrame, *id_columns: str) -> DataFrame: ...

    @overload
    def diffwith(self, left: DataFrame, right: DataFrame, id_columns: Iterable[str], ignore_columns: Iterable[str]) -> DataFrame: ...

    def diffwith(self, left: DataFrame, right: DataFrame, *id_or_ignore_columns: Union[str, Iterable[str]]) -> DataFrame:
        """
        Returns a new DataFrame that contains the differences between the two DataFrames
        as tuples of type `(String, Row, Row)`.

        See `diff(left: DataFrame, right: DataFrame, *id_columns: str)`.

        :param left: left DataFrame
        :type left: DataFrame
        :param right: right DataFrame
        :type right: DataFrame
        :param id_or_ignore_columns: either id column names or two lists of column names,
               first the id column names, second the ignore column names
        :type id_or_ignore_columns: str
        :return: the diff DataFrame
        :rtype DataFrame
        """
        if len(id_or_ignore_columns) == 2 and all([isinstance(lst, Iterable) for lst in id_or_ignore_columns]):
            id_columns, ignore_columns = id_or_ignore_columns
        else:
            id_columns, ignore_columns = (id_or_ignore_columns, [])

        diff = self._do_diff(left, right, id_columns, ignore_columns)
        left_columns = self._columns_of_side(diff, id_columns, self._options.left_column_prefix)
        right_columns = self._columns_of_side(diff, id_columns, self._options.right_column_prefix)
        diff_column = col(self._options.diff_column)

        left_struct = when(diff_column == self._options.insert_diff_value, lit(None)) \
            .otherwise(struct(*left_columns)) \
            .alias(self._options.left_column_prefix)
        right_struct = when(diff_column == self._options.delete_diff_value, lit(None)) \
            .otherwise(struct(*right_columns)) \
            .alias(self._options.right_column_prefix)
        return diff.select(diff_column, left_struct, right_struct)

    def _check_schema(self, left: DataFrame, right: DataFrame, id_columns: List[str], ignore_columns: List[str], case_sensitive: bool):
        def require(result: bool, message: str) -> None:
            if not result:
                raise ValueError(message)

        require(
            len(left.columns) == len(set(left.columns)) and len(right.columns) == len(set(right.columns)),
            f"The datasets have duplicate columns.\n" +
            f"Left column names: {', '.join(left.columns)}\n" +
            f"Right column names: {', '.join(right.columns)}")

        left_non_ignored = list_diff_case_sensitivity(left.columns, ignore_columns, case_sensitive)
        right_non_ignored = list_diff_case_sensitivity(right.columns, ignore_columns, case_sensitive)

        except_ignored_columns_msg = ' except ignored columns' if ignore_columns else ''

        require(
            len(left_non_ignored) == len(right_non_ignored),
            "The number of columns doesn't match.\n" +
            f"Left column names{except_ignored_columns_msg} ({len(left_non_ignored)}): {', '.join(left_non_ignored)}\n" +
            f"Right column names{except_ignored_columns_msg} ({len(right_non_ignored)}): {', '.join(right_non_ignored)}"
        )

        require(len(left_non_ignored) > 0, f"The schema{except_ignored_columns_msg} must not be empty")

        # column types must match but we ignore the nullability of columns
        left_fields = {handle_configured_case_sensitivity(field.name, case_sensitive): field.dataType
                       for field in left.schema.fields
                       if not list_contains_case_sensitivity(ignore_columns, field.name, case_sensitive)}
        right_fields = {handle_configured_case_sensitivity(field.name, case_sensitive): field.dataType
                        for field in right.schema.fields
                        if not list_contains_case_sensitivity(ignore_columns, field.name, case_sensitive)}
        left_extra_schema = set(left_fields.items()) - set(right_fields.items())
        right_extra_schema = set(right_fields.items()) - set(left_fields.items())
        require(
            len(left_extra_schema) == 0 and len(right_extra_schema) == 0,
            "The datasets do not have the same schema.\n" +
            f"Left extra columns: {', '.join([f'{f} ({t.typeName()})' for f, t in sorted(list(left_extra_schema))])}\n" +
            f"Right extra columns: {', '.join([f'{f} ({t.typeName()})' for f, t in sorted(list(right_extra_schema))])}")

        columns = left_non_ignored
        pk_columns = id_columns or columns
        non_pk_columns = list_diff_case_sensitivity(columns, pk_columns, case_sensitive)
        missing_id_columns = list_diff_case_sensitivity(pk_columns, columns, case_sensitive)
        require(
            len(missing_id_columns) == 0,
            f"Some id columns do not exist: {', '.join(missing_id_columns)} missing among {', '.join(columns)}"
        )

        missing_ignore_columns = list_diff_case_sensitivity(ignore_columns, left.columns + right.columns, case_sensitive)
        require(
            len(missing_ignore_columns) == 0,
            f"Some ignore columns do not exist: {', '.join(missing_ignore_columns)} " +
            f"missing among {', '.join(sorted(list(set(left_non_ignored + right_non_ignored))))}"
        )

        require(
            not list_contains_case_sensitivity(pk_columns, self._options.diff_column, case_sensitive),
            f"The id columns must not contain the diff column name '{self._options.diff_column}': {', '.join(pk_columns)}"
        )
        require(
            self._options.change_column is None or not list_contains_case_sensitivity(pk_columns, self._options.change_column, case_sensitive),
            f"The id columns must not contain the change column name '{self._options.change_column}': {', '.join(pk_columns)}"
        )
        diff_value_columns = self._get_diff_value_columns(pk_columns, non_pk_columns, left, right, ignore_columns, case_sensitive)
        diff_value_columns = {n for n, t in diff_value_columns}

        if self._options.diff_mode in [DiffMode.LeftSide, DiffMode.RightSide]:
            require(
                not list_contains_case_sensitivity(diff_value_columns, self._options.diff_column, case_sensitive),
                f"The {'left' if self._options.diff_mode == DiffMode.LeftSide else 'right'} " +
                f"non-id columns must not contain the diff column name '{self._options.diff_column}': " +
                f"{', '.join(list_diff_case_sensitivity((left if self._options.diff_mode == DiffMode.LeftSide else right).columns, id_columns, case_sensitive))}"
            )

            require(
                self._options.change_column is None or not list_contains_case_sensitivity(diff_value_columns, self._options.change_column, case_sensitive),
                f"The {'left' if self._options.diff_mode == DiffMode.LeftSide else 'right'} " +
                f"non-id columns must not contain the change column name '{self._options.change_column}': " +
                f"{', '.join(list_diff_case_sensitivity((left if self._options.diff_mode == DiffMode.LeftSide else right).columns, id_columns, case_sensitive))}"
            )
        else:
            require(
                not list_contains_case_sensitivity(diff_value_columns, self._options.diff_column, case_sensitive),
                f"The column prefixes '{self._options.left_column_prefix}' and '{self._options.right_column_prefix}', " +
                f"together with these non-id columns must not produce the diff column name '{self._options.diff_column}': " +
                f"{', '.join(non_pk_columns)}"
            )

            require(
                self._options.change_column is None or not list_contains_case_sensitivity(diff_value_columns, self._options.change_column, case_sensitive),
                f"The column prefixes '{self._options.left_column_prefix}' and '{self._options.right_column_prefix}', " +
                f"together with these non-id columns must not produce the change column name '{self._options.change_column}': " +
                f"{', '.join(non_pk_columns)}"
            )

            require(
                all(not list_contains_case_sensitivity(pk_columns, c, case_sensitive) for c in diff_value_columns),
                f"The column prefixes '{self._options.left_column_prefix}' and '{self._options.right_column_prefix}', " +
                f"together with these non-id columns must not produce any id column name '{', '.join(pk_columns)}': " +
                f"{', '.join(non_pk_columns)}"
            )

    def _get_change_column(self,
                           exists_column_name: str,
                           value_columns_with_comparator: List[Tuple[str, DiffComparator]],
                           left: DataFrame,
                           right: DataFrame) -> Optional[Column]:
        if self._options.change_column is None:
            return None
        if not self._options.change_column:
            return array().cast(ArrayType(StringType, containsNull = false)).alias(self._options.change_column)
        return when(left[exists_column_name].isNull() | right[exists_column_name].isNull(), lit(None)) \
            .otherwise(
                concat(*[when(cmp.equiv(left[c], right[c]), array()).otherwise(array(lit(c)))
                         for (c, cmp) in value_columns_with_comparator])) \
            .alias(self._options.change_column)

    def _do_diff(self, left: DataFrame, right: DataFrame, id_columns: List[str], ignore_columns: List[str]) -> DataFrame:
        case_sensitive = left.session().conf.get("spark.sql.caseSensitive") == "true"
        self._check_schema(left, right, id_columns, ignore_columns, case_sensitive)

        columns = list_diff_case_sensitivity(left.columns, ignore_columns, case_sensitive)
        pk_columns = id_columns or columns
        value_columns = list_diff_case_sensitivity(columns, pk_columns, case_sensitive)
        value_struct_fields = {f.name: f for f in left.schema.fields}
        value_columns_with_comparator = [(c, self._options.comparator_for(value_struct_fields[c])) for c in value_columns]

        exists_column_name = distinct_prefix_for(left.columns) + "exists"
        left_with_exists = left.withColumn(exists_column_name, lit(1))
        right_with_exists = right.withColumn(exists_column_name, lit(1))
        join_condition = reduce(lambda l, r: l & r,
                                [left_with_exists[c].eqNullSafe(right_with_exists[c])
                                 for c in pk_columns])
        un_changed = reduce(lambda l, r: l & r,
                            [cmp.equiv(left_with_exists[c], right_with_exists[c])
                             for (c, cmp) in value_columns_with_comparator],
                            lit(True))
        change_condition = ~un_changed

        diff_action_column = \
            when(left_with_exists[exists_column_name].isNull(), lit(self._options.insert_diff_value)) \
            .when(right_with_exists[exists_column_name].isNull(), lit(self._options.delete_diff_value)) \
            .when(change_condition, lit(self._options.change_diff_value)) \
            .otherwise(lit(self._options.nochange_diff_value)) \
            .alias(self._options.diff_column)

        diff_columns = [c[1] for c in self._get_diff_columns(pk_columns, value_columns, left, right, ignore_columns, case_sensitive)]
        # turn this column into a list of one or none column so we can easily concat it below with diffActionColumn and diffColumns
        change_column = self._get_change_column(exists_column_name, value_columns_with_comparator, left_with_exists, right_with_exists)
        change_columns = [change_column] if change_column is not None else []

        return left_with_exists \
            .join(right_with_exists, join_condition, "fullouter") \
            .select(*([diff_action_column] + change_columns + diff_columns))

    def _get_diff_id_columns(self, pk_columns: List[str],
                                left: DataFrame,
                                right: DataFrame) -> List[Tuple[str, Column]]:
        return [(c, coalesce(left[c], right[c]).alias(c)) for c in pk_columns]

    def _get_diff_value_columns(self, pk_columns: List[str],
                          value_columns: List[str],
                          left: DataFrame,
                          right: DataFrame,
                          ignore_columns: List[str],
                          case_sensitive: bool) -> List[Tuple[str, Column]]:
        left_value_columns = list_filter_case_sensitivity(left.columns, value_columns, case_sensitive)
        right_value_columns = list_filter_case_sensitivity(right.columns, value_columns, case_sensitive)

        left_non_pk_columns = list_diff_case_sensitivity(left.columns, pk_columns, case_sensitive)
        right_non_pk_columns = list_diff_case_sensitivity(right.columns, pk_columns, case_sensitive)

        left_ignored_columns = list_filter_case_sensitivity(left.columns, ignore_columns, case_sensitive)
        right_ignored_columns = list_filter_case_sensitivity(right.columns, ignore_columns, case_sensitive)
        left_values = {handle_configured_case_sensitivity(c, case_sensitive): (c, when(~(left[c].eqNullSafe(right[c])), left[c]) if self._options.sparse_mode else left[c]) for c in left_non_pk_columns}
        right_values = {handle_configured_case_sensitivity(c, case_sensitive): (c, when(~(left[c].eqNullSafe(right[c])), right[c]) if self._options.sparse_mode else right[c]) for c in right_non_pk_columns}

        def alias(prefix: Optional[str], values: Dict[str, Tuple[str, Column]]) -> Callable[[str], Tuple[str, Column]]:
            def func(name: str) -> (str, Column):
                name, column = values[handle_configured_case_sensitivity(name, case_sensitive)]
                alias = name if prefix is None else f'{prefix}_{name}'
                return alias, column.alias(alias)

            return func

        def alias_left(name: str) -> (str, Column):
            return alias(self._options.left_column_prefix, left_values)(name)

        def alias_right(name: str) -> (str, Column):
            return alias(self._options.right_column_prefix, right_values)(name)

        prefixed_left_ignored_columns = [alias_left(c) for c in left_ignored_columns]
        prefixed_right_ignored_columns = [alias_right(c) for c in right_ignored_columns]

        if self._options.diff_mode == DiffMode.ColumnByColumn:
            non_id_columns = \
                [c for vc in value_columns for c in [alias_left(vc), alias_right(vc)]] + \
                [c for ic in ignore_columns for c in (
                   ([alias_left(ic)] if list_contains_case_sensitivity(left_ignored_columns, ic, case_sensitive) else []) +
                   ([alias_right(ic)] if list_contains_case_sensitivity(right_ignored_columns, ic, case_sensitive) else [])
                )]
        elif self._options.diff_mode == DiffMode.SideBySide:
            non_id_columns = \
                [alias_left(c) for c in left_value_columns] + prefixed_left_ignored_columns + \
                [alias_right(c) for c in right_value_columns] + prefixed_right_ignored_columns
        elif self._options.diff_mode == DiffMode.LeftSide:
            non_id_columns = \
                [alias(None, left_values)(c) for c in value_columns] +\
                [alias(None, left_values)(c) for c in left_ignored_columns]
        elif self._options.diff_mode == DiffMode.RightSide:
            non_id_columns = \
                [alias(None, right_values)(c) for c in value_columns] + \
                [alias(None, right_values)(c) for c in right_ignored_columns]
        else:
            raise RuntimeError(f'Unsupported diff mode: {self._options.diff_mode}')

        return non_id_columns

    def _get_diff_columns(self, pk_columns: List[str],
                          value_columns: List[str],
                          left: DataFrame,
                          right: DataFrame,
                          ignore_columns: List[str],
                          case_sensitive: bool) -> List[Tuple[str, Column]]:
        return self._get_diff_id_columns(pk_columns, left, right) + \
               self._get_diff_value_columns(pk_columns, value_columns, left, right, ignore_columns, case_sensitive)


@overload
def diff(self: DataFrame, other: DataFrame, *id_columns: str) -> DataFrame: ...


@overload
def diff(self: DataFrame, other: DataFrame, id_columns: Iterable[str], ignore_columns: Iterable[str]) -> DataFrame: ...


def diff(self: DataFrame, other: DataFrame, *id_or_ignore_columns: Union[str, Iterable[str]]) -> DataFrame:
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

    Values in optional ignore columns are not compared but included in the output DataFrame.

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
    :param id_or_ignore_columns: either id column names or two lists of column names,
           first the id column names, second the ignore column names
    :type id_or_ignore_columns: str
    :return: the diff DataFrame
    :rtype DataFrame
    """
    return Differ().diff(self, other, *id_or_ignore_columns)


@overload
def diffwith(self: DataFrame, other: DataFrame, *id_columns: str) -> DataFrame: ...


@overload
def diffwith(self: DataFrame, other: DataFrame, id_columns: Iterable[str], ignore_columns: Iterable[str]) -> DataFrame: ...


def diffwith(self: DataFrame, other: DataFrame, *id_or_ignore_columns: Union[str, Iterable[str]]) -> DataFrame:
    """
    Returns a new DataFrame that contains the differences between the two DataFrames
    as tuples of type `(String, Row, Row)`.

    See `diff(left: DataFrame, right: DataFrame, *id_columns: str)`.

    :param left: left DataFrame
    :type left: DataFrame
    :param right: right DataFrame
    :type right: DataFrame
    :param id_or_ignore_columns: either id column names or two lists of column names,
           first the id column names, second the ignore column names
    :type id_or_ignore_columns: str
    :return: the diff DataFrame
    :rtype DataFrame
    """
    return Differ().diffwith(self, other, *id_or_ignore_columns)


@overload
def diff_with_options(self: DataFrame, other: DataFrame, options: DiffOptions, *id_columns: str) -> DataFrame: ...


@overload
def diff_with_options(self: DataFrame, other: DataFrame, options: DiffOptions, id_columns: Iterable[str], ignore_columns: Iterable[str]) -> DataFrame: ...


def diff_with_options(self: DataFrame, other: DataFrame, options: DiffOptions, *id_or_ignore_columns: Union[str, Iterable[str]]) -> DataFrame:
    """
    Returns a new DataFrame that contains the differences between this and the other DataFrame.

    See `diff(other: DataFrame, *id_columns: str)`.

    The schema of the returned DataFrame can be configured by the given `DiffOptions`.

    :param other: right DataFrame
    :type other: DataFrame
    :param id_or_ignore_columns: either id column names or two lists of column names,
           first the id column names, second the ignore column names
    :type id_or_ignore_columns: str
    :param options: diff options
    :type options: DiffOptions
    :return: the diff DataFrame
    :rtype DataFrame
    """
    return Differ(options).diff(self, other, *id_or_ignore_columns)


@overload
def diffwith_with_options(self: DataFrame, other: DataFrame, options: DiffOptions, *id_columns: str) -> DataFrame: ...


@overload
def diffwith_with_options(self: DataFrame, other: DataFrame, options: DiffOptions, id_columns: Iterable[str], ignore_columns: Iterable[str]) -> DataFrame: ...


def diffwith_with_options(self: DataFrame, other: DataFrame, options: DiffOptions, *id_or_ignore_columns: Union[str, Iterable[str]]) -> DataFrame:
    """
    Returns a new DataFrame that contains the differences between the two DataFrames
    as tuples of type `(String, Row, Row)`.

    See `diff(left: DataFrame, right: DataFrame, *id_columns: str)`.

    The schema of the returned DataFrame can be configured by the given `DiffOptions`.

    :param other: right DataFrame
    :type other: DataFrame
    :param id_or_ignore_columns: either id column names or two lists of column names,
           first the id column names, second the ignore column names
    :type id_or_ignore_columns: str
    :param options: diff options
    :type options: DiffOptions
    :return: the diff DataFrame
    :rtype DataFrame
    """
    return Differ(options).diffwith(self, other, *id_or_ignore_columns)


DataFrame.diff = diff
DataFrame.diffwith = diffwith
DataFrame.diff_with_options = diff_with_options
DataFrame.diffwith_with_options = diffwith_with_options

if has_connect:
    ConnectDataFrame.diff = diff
    ConnectDataFrame.diffwith = diffwith
    ConnectDataFrame.diff_with_options = diff_with_options
    ConnectDataFrame.diffwith_with_options = diffwith_with_options
