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
import contextlib
import re

from py4j.java_gateway import JavaObject
from pyspark.sql import Row
from pyspark.sql.functions import col, when, abs
from pyspark.sql.types import IntegerType, LongType, StringType, DateType, StructField, StructType, FloatType, DoubleType
from unittest import skipIf

from gresearch.spark.diff import Differ, DiffOptions, DiffMode, DiffComparators
from spark_common import SparkTest


class DiffTest(SparkTest):

    expected_diff = None

    @classmethod
    def setUpClass(cls):
        super(DiffTest, cls).setUpClass()

        value_row = Row('id', 'val', 'label')
        cls.left_df = cls.spark.createDataFrame([
            value_row(1, 1.0, 'one'),
            value_row(2, 2.0, 'two'),
            value_row(3, 3.0, 'three'),
            value_row(4, None, None),
            value_row(5, 5.0, 'five'),
            value_row(7, 7.0, 'seven'),
        ])

        cls.right_df = cls.spark.createDataFrame([
            value_row(1, 1.1, 'one'),
            value_row(2, 2.0, 'Two'),
            value_row(3, 3.0, 'three'),
            value_row(4, 4.0, 'four'),
            value_row(5, None, None),
            value_row(6, 6.0, 'six'),
        ])

        diff_row = Row('diff', 'id', 'left_val', 'right_val', 'left_label', 'right_label')
        cls.expected_diff = [
            diff_row('C', 1, 1.0, 1.1, 'one', 'one'),
            diff_row('C', 2, 2.0, 2.0, 'two', 'Two'),
            diff_row('N', 3, 3.0, 3.0, 'three', 'three'),
            diff_row('C', 4, None, 4.0, None, 'four'),
            diff_row('C', 5, 5.0, None, 'five', None),
            diff_row('I', 6, None, 6.0, None, 'six'),
            diff_row('D', 7, 7.0, None, 'seven', None),
        ]
        diff_change_row = Row('diff', 'change', 'id', 'left_val', 'right_val', 'left_label', 'right_label')
        cls.expected_diff_change = [
            diff_change_row('C', ['val'], 1, 1.0, 1.1, 'one', 'one'),
            diff_change_row('C', ['label'], 2, 2.0, 2.0, 'two', 'Two'),
            diff_change_row('N', [], 3, 3.0, 3.0, 'three', 'three'),
            diff_change_row('C', ['val', 'label'], 4, None, 4.0, None, 'four'),
            diff_change_row('C', ['val', 'label'], 5, 5.0, None, 'five', None),
            diff_change_row('I', None, 6, None, 6.0, None, 'six'),
            diff_change_row('D', None, 7, 7.0, None, 'seven', None),
        ]
        cls.expected_diff_reversed = [
            diff_row('C', 1, 1.1, 1.0, 'one', 'one'),
            diff_row('C', 2, 2.0, 2.0, 'Two', 'two'),
            diff_row('N', 3, 3.0, 3.0, 'three', 'three'),
            diff_row('C', 4, 4.0, None, 'four', None),
            diff_row('C', 5, None, 5.0, None, 'five'),
            diff_row('D', 6, 6.0, None, 'six', None),
            diff_row('I', 7, None, 7.0, None, 'seven'),
        ]
        cls.expected_diff_ignored = [
            diff_row('C', 1, 1.0, 1.1, 'one', 'one'),
            diff_row('N', 2, 2.0, 2.0, 'two', 'Two'),
            diff_row('N', 3, 3.0, 3.0, 'three', 'three'),
            diff_row('C', 4, None, 4.0, None, 'four'),
            diff_row('C', 5, 5.0, None, 'five', None),
            diff_row('I', 6, None, 6.0, None, 'six'),
            diff_row('D', 7, 7.0, None, 'seven', None),
        ]

        diffwith_row = Row('diff', 'left', 'right')
        cls.expected_diffwith = [
            diffwith_row('C', value_row(1, 1.0, 'one'), value_row(1, 1.1, 'one')),
            diffwith_row('C', value_row(2, 2.0, 'two'), value_row(2, 2.0, 'Two')),
            diffwith_row('N', value_row(3, 3.0, 'three'), value_row(3, 3.0, 'three')),
            diffwith_row('C', value_row(4, None, None), value_row(4, 4.0, 'four')),
            diffwith_row('C', value_row(5, 5.0, 'five'), value_row(5, None, None)),
            diffwith_row('I', None, value_row(6, 6.0, 'six')),
            diffwith_row('D', value_row(7, 7.0, 'seven'), None),
        ]
        cls.expected_diffwith_ignored = [
            diffwith_row('C', value_row(1, 1.0, 'one'), value_row(1, 1.1, 'one')),
            diffwith_row('N', value_row(2, 2.0, 'two'), value_row(2, 2.0, 'Two')),
            diffwith_row('N', value_row(3, 3.0, 'three'), value_row(3, 3.0, 'three')),
            diffwith_row('C', value_row(4, None, None), value_row(4, 4.0, 'four')),
            diffwith_row('C', value_row(5, 5.0, 'five'), value_row(5, None, None)),
            diffwith_row('I', None, value_row(6, 6.0, 'six')),
            diffwith_row('D', value_row(7, 7.0, 'seven'), None),
        ]

        diff_with_options_row = Row('d', 'id', 'l_val', 'r_val', 'l_label', 'r_label')
        cls.expected_diff_with_options = [
            diff_with_options_row('c', 1, 1.0, 1.1, 'one', 'one'),
            diff_with_options_row('c', 2, 2.0, 2.0, 'two', 'Two'),
            diff_with_options_row('n', 3, 3.0, 3.0, 'three', 'three'),
            diff_with_options_row('c', 4, None, 4.0, None, 'four'),
            diff_with_options_row('c', 5, 5.0, None, 'five', None),
            diff_with_options_row('i', 6, None, 6.0, None, 'six'),
            diff_with_options_row('r', 7, 7.0, None, 'seven', None),
        ]
        cls.expected_diff_with_options_ignored = [
            diff_with_options_row('c', 1, 1.0, 1.1, 'one', 'one'),
            diff_with_options_row('n', 2, 2.0, 2.0, 'two', 'Two'),
            diff_with_options_row('n', 3, 3.0, 3.0, 'three', 'three'),
            diff_with_options_row('c', 4, None, 4.0, None, 'four'),
            diff_with_options_row('c', 5, 5.0, None, 'five', None),
            diff_with_options_row('i', 6, None, 6.0, None, 'six'),
            diff_with_options_row('r', 7, 7.0, None, 'seven', None),
        ]

        diff_with_changes_row = Row('diff', 'changes', 'id', 'left_val', 'right_val', 'left_label', 'right_label')
        cls.expected_diff_with_changes = [
            diff_with_changes_row('C', ['val'], 1, 1.0, 1.1, 'one', 'one'),
            diff_with_changes_row('C', ['label'], 2, 2.0, 2.0, 'two', 'Two'),
            diff_with_changes_row('N', [], 3, 3.0, 3.0, 'three', 'three'),
            diff_with_changes_row('C', ['val', 'label'], 4, None, 4.0, None, 'four'),
            diff_with_changes_row('C', ['val', 'label'], 5, 5.0, None, 'five', None),
            diff_with_changes_row('I', None, 6, None, 6.0, None, 'six'),
            diff_with_changes_row('D', None, 7, 7.0, None, 'seven', None),
        ]

        cls.expected_diff_in_column_by_column_mode = cls.expected_diff

        diff_in_side_by_side_mode_row = Row('diff', 'id', 'left_val', 'left_label', 'right_val', 'right_label')
        cls.expected_diff_in_side_by_side_mode = [
            diff_in_side_by_side_mode_row('C', 1, 1.0, 'one', 1.1, 'one'),
            diff_in_side_by_side_mode_row('C', 2, 2.0, 'two', 2.0, 'Two'),
            diff_in_side_by_side_mode_row('N', 3, 3.0, 'three', 3.0, 'three'),
            diff_in_side_by_side_mode_row('C', 4, None, None, 4.0, 'four'),
            diff_in_side_by_side_mode_row('C', 5, 5.0, 'five', None, None),
            diff_in_side_by_side_mode_row('I', 6, None, None, 6.0, 'six'),
            diff_in_side_by_side_mode_row('D', 7, 7.0, 'seven', None, None),
        ]

        diff_in_left_side_mode_row = Row('diff', 'id', 'left_val', 'left_label')
        cls.expected_diff_in_left_side_mode = [
            diff_in_left_side_mode_row('C', 1, 1.0, 'one'),
            diff_in_left_side_mode_row('C', 2, 2.0, 'two'),
            diff_in_left_side_mode_row('N', 3, 3.0, 'three'),
            diff_in_left_side_mode_row('C', 4, None, None),
            diff_in_left_side_mode_row('C', 5, 5.0, 'five'),
            diff_in_left_side_mode_row('I', 6, None, None),
            diff_in_left_side_mode_row('D', 7, 7.0, 'seven'),
        ]

        diff_in_right_side_mode_row = Row('diff', 'id', 'right_val', 'right_label')
        cls.expected_diff_in_right_side_mode = [
            diff_in_right_side_mode_row('C', 1, 1.1, 'one'),
            diff_in_right_side_mode_row('C', 2, 2.0, 'Two'),
            diff_in_right_side_mode_row('N', 3, 3.0, 'three'),
            diff_in_right_side_mode_row('C', 4, 4.0, 'four'),
            diff_in_right_side_mode_row('C', 5, None, None),
            diff_in_right_side_mode_row('I', 6, 6.0, 'six'),
            diff_in_right_side_mode_row('D', 7, None, None),
        ]

        diff_in_sparse_mode_row = Row('diff', 'id', 'left_val', 'right_val', 'left_label', 'right_label')
        cls.expected_diff_in_sparse_mode = [
            diff_in_sparse_mode_row('C', 1, 1.0, 1.1, None, None),
            diff_in_sparse_mode_row('C', 2, None, None, 'two', 'Two'),
            diff_in_sparse_mode_row('N', 3, None, None, None, None),
            diff_in_sparse_mode_row('C', 4, None, 4.0, None, 'four'),
            diff_in_sparse_mode_row('C', 5, 5.0, None, 'five', None),
            diff_in_sparse_mode_row('I', 6, None, 6.0, None, 'six'),
            diff_in_sparse_mode_row('D', 7, 7.0, None, 'seven', None),
        ]

    def test_check_schema(self):
        @contextlib.contextmanager
        def test_requirement(error_message: str):
            with self.assertRaises(ValueError) as e:
                yield
            self.assertEqual((error_message, ), e.exception.args)

        with self.subTest("duplicate columns"):
            with test_requirement("The datasets have duplicate columns.\n"
                                  "Left column names: id, id\nRight column names: id, id"):
                self.left_df.select("id", "id").diff(self.right_df.select("id", "id"), "id")

        with self.subTest("case-sensitive id column"):
            with test_requirement("Some id columns do not exist: ID missing among id, val, label"):
                with self.sql_conf({"spark.sql.caseSensitive": "true"}):
                    self.left_df.diff(self.right_df, "ID")

        left = self.left_df.withColumnRenamed("val", "diff")
        right = self.right_df.withColumnRenamed("val", "diff")

        with self.subTest("id column 'diff'"):
            with test_requirement("The id columns must not contain the diff column name 'diff': id, diff, label"):
                left.diff(right)
            with test_requirement("The id columns must not contain the diff column name 'diff': diff"):
                left.diff(right, "diff")
            with test_requirement("The id columns must not contain the diff column name 'diff': diff, id"):
                left.diff(right, "diff", "id")

            with self.sql_conf({"spark.sql.caseSensitive": "false"}):
                with test_requirement("The id columns must not contain the diff column name 'diff': Diff, id"):
                    left.withColumnRenamed("diff", "Diff") \
                        .diff(right.withColumnRenamed("diff", "Diff"), "Diff", "id")

            with self.sql_conf({"spark.sql.caseSensitive": "true"}):
                left.withColumnRenamed("diff", "Diff") \
                    .diff(right.withColumnRenamed("diff", "Diff"), "Diff", "id")

        with self.subTest("non-id column 'diff"):
            actual = left.diff(right, "id").orderBy("id")
            expected_columns = ["diff", "id", "left_diff", "right_diff", "left_label", "right_label"]
            self.assertEqual(actual.columns, expected_columns)
            self.assertEqual(actual.collect(), self.expected_diff)

        with self.subTest("non-id column produces diff column name"):
            options = DiffOptions() \
                .with_diff_column("a_val") \
                .with_left_column_prefix("a") \
                .with_right_column_prefix("b")

            with test_requirement("The column prefixes 'a' and 'b', together with these non-id columns " +
                                  "must not produce the diff column name 'a_val': val, label"):
                self.left_df.diff_with_options(self.right_df, options, "id")
            with test_requirement("The column prefixes 'a' and 'b', together with these non-id columns " +
                                  "must not produce the diff column name 'b_val': val, label"):
                self.left_df.diff_with_options(self.right_df, options.with_diff_column("b_val"), "id")

        with self.subTest("non-id column would produce diff column name unless in left-side mode"):
            options = DiffOptions() \
                .with_diff_column("a_val") \
                .with_left_column_prefix("a") \
                .with_right_column_prefix("b") \
                .with_diff_mode(DiffMode.LeftSide)
            self.left_df.diff_with_options(self.right_df, options, "id")

        with self.subTest("non-id column would produce diff column name unless in right-side mode"):
            options = DiffOptions() \
                .with_diff_column("b_val") \
                .with_left_column_prefix("a") \
                .with_right_column_prefix("b") \
                .with_diff_mode(DiffMode.RightSide)
            self.left_df.diff_with_options(self.right_df, options, "id")

        with self.sql_conf({"spark.sql.caseSensitive": "false"}):
            with self.subTest("case-insensitive non-id column produces diff column name"):
                options = DiffOptions() \
                    .with_diff_column("a_val") \
                    .with_left_column_prefix("A") \
                    .with_right_column_prefix("b")
                with test_requirement("The column prefixes 'A' and 'b', together with these non-id columns " +
                                      "must not produce the diff column name 'a_val': val, label"):
                    self.left_df.diff_with_options(self.right_df, options, "id")

            with self.subTest("case-insensitive non-id column would produce diff column name unless in left-side mode"):
                options = DiffOptions() \
                    .with_diff_column("a_val") \
                    .with_left_column_prefix("A") \
                    .with_right_column_prefix("B") \
                    .with_diff_mode(DiffMode.LeftSide)
                self.left_df.diff_with_options(self.right_df, options, "id")

            with self.subTest("case-insensitive non-id column would produce diff column name unless in right-side mode"):
                options = DiffOptions() \
                    .with_diff_column("b_val") \
                    .with_left_column_prefix("A") \
                    .with_right_column_prefix("B") \
                    .with_diff_mode(DiffMode.RightSide)
                self.left_df.diff_with_options(self.right_df, options, "id")

        with self.sql_conf({"spark.sql.caseSensitive": "true"}):
            with self.subTest("case-sensitive non-id column produces non-conflicting diff column name"):
                options = DiffOptions() \
                    .with_diff_column("a_val") \
                    .with_left_column_prefix("A") \
                    .with_right_column_prefix("B") \

                actual = self.left_df.diff_with_options(self.right_df, options, "id").orderBy("id")
                expected_columns = ["a_val", "id", "A_val", "B_val", "A_label", "B_label"]
                self.assertEqual(actual.columns, expected_columns)
                self.assertEqual(actual.collect(), self.expected_diff)

        left = self.left_df.withColumnRenamed("val", "change")
        right = self.right_df.withColumnRenamed("val", "change")

        with self.subTest("id column 'change'"):
            options = DiffOptions() \
                .with_change_column("change")
            with test_requirement("The id columns must not contain the change column name 'change': id, change, label"):
                left.diff_with_options(right, options)
            with test_requirement("The id columns must not contain the change column name 'change': change"):
                left.diff_with_options(right, options, "change")
            with test_requirement("The id columns must not contain the change column name 'change': change, id"):
                left.diff_with_options(right, options, "change", "id")

            with self.sql_conf({"spark.sql.caseSensitive": "false"}):
                with test_requirement("The id columns must not contain the change column name 'change': Change, id"):
                    left.withColumnRenamed("change", "Change") \
                        .diff_with_options(right.withColumnRenamed("change", "Change"), options, "Change", "id")

            with self.sql_conf({"spark.sql.caseSensitive": "true"}):
                left.withColumnRenamed("change", "Change") \
                    .diff_with_options(right.withColumnRenamed("change", "Change"), options, "Change", "id")

        with self.subTest("non-id column 'change"):
            actual = left.diff_with_options(right, options, "id").orderBy("id")
            expected_columns = ["diff", "change", "id", "left_change", "right_change", "left_label", "right_label"]
            diff_change_row = Row(*expected_columns)
            expected_diff = [
                diff_change_row('C', ['change'], 1, 1.0, 1.1, 'one', 'one'),
                diff_change_row('C', ['label'], 2, 2.0, 2.0, 'two', 'Two'),
                diff_change_row('N', [], 3, 3.0, 3.0, 'three', 'three'),
                diff_change_row('C', ['change', 'label'], 4, None, 4.0, None, 'four'),
                diff_change_row('C', ['change', 'label'], 5, 5.0, None, 'five', None),
                diff_change_row('I', None, 6, None, 6.0, None, 'six'),
                diff_change_row('D', None, 7, 7.0, None, 'seven', None),
            ]
            self.assertEqual(actual.columns, expected_columns)
            self.assertEqual(actual.collect(), expected_diff)

        with self.subTest("non-id column produces change column name"):
            options = DiffOptions() \
                .with_change_column("a_val") \
                .with_left_column_prefix("a") \
                .with_right_column_prefix("b")
            with test_requirement("The column prefixes 'a' and 'b', together with these non-id columns " +
                                  "must not produce the change column name 'a_val': val, label"):
                self.left_df.diff_with_options(self.right_df, options, "id")

        with self.sql_conf({"spark.sql.caseSensitive": "false"}):
            with self.subTest("case-insensitive non-id column produces change column name"):
                options = DiffOptions() \
                    .with_change_column("a_val") \
                    .with_left_column_prefix("A") \
                    .with_right_column_prefix("B")
                with test_requirement("The column prefixes 'A' and 'B', together with these non-id columns " +
                                      "must not produce the change column name 'a_val': val, label"):
                    self.left_df.diff_with_options(self.right_df, options, "id")

        with self.sql_conf({"spark.sql.caseSensitive": "true"}):
            with self.subTest("case-sensitive non-id column produces non-conflicting change column name"):
                options = DiffOptions() \
                    .with_change_column("a_val") \
                    .with_left_column_prefix("A") \
                    .with_right_column_prefix("B")
                actual = self.left_df.diff_with_options(self.right_df, options, "id").orderBy("id")
                expected_columns = ["diff", "a_val", "id", "A_val", "B_val", "A_label", "B_label"]
                self.assertEqual(actual.columns, expected_columns)
                self.assertEqual(actual.collect(), self.expected_diff_change)

        left = self.left_df.select(col("id").alias("first_id"), col("val").alias("id"), "label")
        right = self.right_df.select(col("id").alias("first_id"), col("val").alias("id"), "label")
        with self.subTest("non-id column produces id column name"):
            options = DiffOptions() \
                .with_left_column_prefix("first") \
                .with_right_column_prefix("second")
            with test_requirement("The column prefixes 'first' and 'second', together with these non-id columns " +
                                  "must not produce any id column name 'first_id': id, label"):
                left.diff_with_options(right, options, "first_id")

        with self.sql_conf({"spark.sql.caseSensitive": "false"}):
            with self.subTest("case-insensitive non-id column produces id column name"):
                options = DiffOptions() \
                    .with_left_column_prefix("FIRST") \
                    .with_right_column_prefix("SECOND")
                with test_requirement("The column prefixes 'FIRST' and 'SECOND', together with these non-id columns " +
                                      "must not produce any id column name 'first_id': id, label"):
                    left.diff_with_options(right, options, "first_id")

        with self.sql_conf({"spark.sql.caseSensitive": "true"}):
            with self.subTest("case-sensitive non-id column produces non-conflicting id column name"):
                options = DiffOptions() \
                    .with_left_column_prefix("FIRST") \
                    .with_right_column_prefix("SECOND")
                actual = left.diff_with_options(right, options, "first_id").orderBy("first_id")
                expected_columns = ["diff", "first_id", "FIRST_id", "SECOND_id", "FIRST_label", "SECOND_label"]
                self.assertEqual(actual.columns, expected_columns)
                self.assertEqual(actual.collect(), self.expected_diff)

        with self.subTest("empty schema"):
            with test_requirement("The schema must not be empty"):
                self.left_df.select().diff(self.right_df.select())

        with self.subTest("empty schema after ignored columns"):
            with test_requirement("The schema except ignored columns must not be empty"):
                self.left_df.select("id", "val").diff(self.right_df.select("id", "label"), [], ["id", "val", "label"])

        with self.subTest("different types"):
            with test_requirement("The datasets do not have the same schema.\n" +
                                  "Left extra columns: val (double)\n" +
                                  "Right extra columns: val (string)"):
                self.left_df.select("id", "val").diff(self.right_df.select("id", col("label").alias("val")))

        with self.subTest("ignore columns with different types"):
            actual = self.left_df.select("id", "val").diff(self.right_df.select("id", col("label").alias("val")), [], ["val"])
            expected_schema = [
                ("diff", StringType()),
                ("id", LongType()),
                ("left_val", DoubleType()),
                ("right_val", StringType()),
            ]
            self.assertEqual([(f.name, f.dataType) for f in actual.schema], expected_schema)

        with self.subTest("diff with different column names"):
            with test_requirement("The datasets do not have the same schema.\n" +
                                  "Left extra columns: val (double)\n" +
                                  "Right extra columns: label (string)"):
                self.left_df.select("id", "val").diff(self.right_df.select("id", "label"))

        left = self.left_df.select("id", "val", "label")
        right = self.right_df.select(col("id").alias("ID"), col("val").alias("VaL"), "label")
        with self.sql_conf({"spark.sql.caseSensitive": "false"}):
            with self.subTest("case-insensitive column names"):
                actual = left.diff(right, "id").orderBy("id")
                reverse = right.diff(left, "id").orderBy("id")
                self.assertEqual(actual.columns, ["diff", "id", "left_val", "right_VaL", "left_label", "right_label"])
                self.assertEqual(actual.collect(), self.expected_diff)
                self.assertEqual(reverse.columns, ["diff", "id", "left_VaL", "right_val", "left_label", "right_label"])
                self.assertEqual(reverse.collect(), self.expected_diff_reversed)

        with self.sql_conf({"spark.sql.caseSensitive": "true"}):
            with self.subTest("case-sensitive column names"):
                with test_requirement("The datasets do not have the same schema.\n" +
                                      "Left extra columns: id (long), val (double)\n" +
                                      "Right extra columns: ID (long), VaL (double)"):
                    left.diff(right, "id")

        with self.subTest("non-existing id column"):
            with test_requirement("Some id columns do not exist: does not exists missing among id, val, label"):
                self.left_df.diff(self.right_df, "does not exists")

        with self.subTest("different number of columns"):
            with test_requirement("The number of columns doesn't match.\n" +
                                  "Left column names (2): id, val\n" +
                                  "Right column names (3): id, val, label"):
                self.left_df.select("id", "val").diff(self.right_df, "id")

        with self.subTest("different number of columns after ignoring columns"):
            left = self.left_df.select("id", "val", col("label").alias("meta"))
            right = self.right_df.select("id", col("label").alias("seq"), "val")
            with test_requirement("The number of columns doesn't match.\n" +
                                  "Left column names except ignored columns (2): id, val\n" +
                                  "Right column names except ignored columns (3): id, seq, val"):
                left.diff(right, ["id"], ["meta"])

        with self.subTest("diff column name in value columns in left-side diff mode"):
            options = DiffOptions().with_diff_column("val").with_diff_mode(DiffMode.LeftSide)
            with test_requirement("The left non-id columns must not contain the diff column name 'val': val, label"):
                self.left_df.diff_with_options(self.right_df, options, "id")

        with self.subTest("diff column name in value columns in right-side diff mode"):
            options = DiffOptions().with_diff_column("val").with_diff_mode(DiffMode.RightSide)
            with test_requirement("The right non-id columns must not contain the diff column name 'val': val, label"):
                self.left_df.diff_with_options(self.right_df, options, "id")

        with self.subTest("change column name in value columns in left-side diff mode"):
            options = DiffOptions().with_change_column("val").with_diff_mode(DiffMode.LeftSide)
            with test_requirement("The left non-id columns must not contain the change column name 'val': val, label"):
                self.left_df.diff_with_options(self.right_df, options, "id")

        with self.subTest("change column name in value columns in right-side diff mode"):
            options = DiffOptions().with_change_column("val").with_diff_mode(DiffMode.RightSide)
            with test_requirement("The right non-id columns must not contain the change column name 'val': val, label"):
                self.left_df.diff_with_options(self.right_df, options, "id")

    def test_dataframe_diff(self):
        diff = self.left_df.diff(self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff, diff)

    def test_dataframe_diff_with_ignored(self):
        diff = self.left_df.diff(self.right_df, ['id'], ['label']).orderBy('id').collect()
        self.assertEqual(self.expected_diff_ignored, diff)

    def test_dataframe_diffwith(self):
        diff = self.left_df.diffwith(self.right_df, 'id').orderBy('id').collect()
        self.assertSetEqual(set(self.expected_diffwith), set(diff))
        self.assertEqual(len(self.expected_diffwith), len(diff))

    def test_dataframe_diffwith_with_ignored(self):
        diff = self.left_df.diffwith(self.right_df, ['id'], ['label']).orderBy('id').collect()
        self.assertSetEqual(set(self.expected_diffwith_ignored), set(diff))
        self.assertEqual(len(self.expected_diffwith_ignored), len(diff))

    def test_dataframe_diff_with_default_options(self):
        diff = self.left_df.diff_with_options(self.right_df, DiffOptions(), 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff, diff)

    def test_dataframe_diff_with_options(self):
        options = DiffOptions('d', 'l', 'r', 'i', 'c', 'r', 'n', None)
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_with_options, diff)

    def test_dataframe_diff_with_options_and_ignored(self):
        options = DiffOptions('d', 'l', 'r', 'i', 'c', 'r', 'n', None)
        diff = self.left_df.diff_with_options(self.right_df, options, ['id'], ['label']).orderBy('id').collect()
        self.assertEqual(self.expected_diff_with_options_ignored, diff)

    def test_dataframe_diff_with_changes(self):
        options = DiffOptions().with_change_column('changes')
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_with_changes, diff)

    def test_dataframe_diff_with_diff_mode_column_by_column(self):
        options = DiffOptions().with_diff_mode(DiffMode.ColumnByColumn)
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_in_column_by_column_mode, diff)

    def test_dataframe_diff_with_diff_mode_side_by_side(self):
        options = DiffOptions().with_diff_mode(DiffMode.SideBySide)
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_in_side_by_side_mode, diff)

    def test_dataframe_diff_with_diff_mode_left_side(self):
        options = DiffOptions().with_diff_mode(DiffMode.LeftSide)
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_in_left_side_mode, diff)

    def test_dataframe_diff_with_diff_mode_right_side(self):
        options = DiffOptions().with_diff_mode(DiffMode.RightSide)
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_in_right_side_mode, diff)

    def test_dataframe_diff_with_sparse_mode(self):
        options = DiffOptions().with_sparse_mode(True)
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_in_sparse_mode, diff)

    def test_differ_diff(self):
        diff = Differ().diff(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff, diff)

    def test_differ_diffwith(self):
        diff = Differ().diffwith(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertSetEqual(set(self.expected_diffwith), set(diff))
        self.assertEqual(len(self.expected_diffwith), len(diff))

    def test_differ_diff_with_default_options(self):
        options = DiffOptions()
        diff = Differ(options).diff(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff, diff)

    def test_differ_diff_with_options(self):
        options = DiffOptions('d', 'l', 'r', 'i', 'c', 'r', 'n', None)
        diff = Differ(options).diff(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_with_options, diff)

    def test_differ_diff_with_changes(self):
        options = DiffOptions().with_change_column('changes')
        diff = Differ(options).diff(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_with_changes, diff)

    def test_differ_diff_in_diff_mode_column_by_column(self):
        options = DiffOptions().with_diff_mode(DiffMode.ColumnByColumn)
        diff = Differ(options).diff(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_in_column_by_column_mode, diff)

    def test_differ_diff_in_diff_mode_side_by_side(self):
        options = DiffOptions().with_diff_mode(DiffMode.SideBySide)
        diff = Differ(options).diff(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_in_side_by_side_mode, diff)

    def test_differ_diff_in_diff_mode_left_side(self):
        options = DiffOptions().with_diff_mode(DiffMode.LeftSide)
        diff = Differ(options).diff(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_in_left_side_mode, diff)

    def test_differ_diff_in_diff_mode_right_side(self):
        options = DiffOptions().with_diff_mode(DiffMode.RightSide)
        diff = Differ(options).diff(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_in_right_side_mode, diff)

    def test_differ_diff_with_sparse_mode(self):
        options = DiffOptions().with_sparse_mode(True)
        diff = Differ(options).diff(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_in_sparse_mode, diff)

    @skipIf(SparkTest.is_spark_connect, "Spark Connect does not provide access to the JVM")
    def test_diff_options_default(self):
        jvm = self.spark._jvm
        joptions = jvm.uk.co.gresearch.spark.diff.DiffOptions.default()
        options = DiffOptions()
        for attr in options.__dict__.keys():
            const = re.sub(r'_(.)', lambda match: match.group(1).upper(), attr)
            expected = getattr(joptions, const)()
            actual = getattr(options, attr)

            if type(expected) == JavaObject:
                class_name = re.sub(r'\$.*$', '', expected.getClass().getName())
                if class_name in ['scala.None']:  # how does the Some(?) look like?
                    actual = 'Some({})'.format(actual) if actual is not None else 'None'
                if class_name in ['scala.collection.immutable.Map', 'scala.collection.mutable.Map']:
                    actual = f'Map({", ".join(f"{key} -> {value._to_java(jvm).toString()}" for key, value in actual.items())})'
                expected = expected.toString()

            if attr in ['diff_mode', 'default_comparator']:
                # does the Python default diff mode resolve to the same Java diff mode enum value?
                # does the Python diff comparator resolve to the same Java diff comparator?
                self.assertEqual(expected, actual._to_java(jvm).toString(), '{} == {} ?'.format(attr, const))
            else:
                self.assertEqual(expected, actual, '{} == {} ?'.format(attr, const))

    @skipIf(SparkTest.is_spark_connect, "Spark Connect does not provide access to the JVM")
    def test_diff_mode_consts(self):
        jvm = self.spark._jvm
        jmodes = jvm.uk.co.gresearch.spark.diff.DiffMode
        modes = DiffMode
        for attr in modes.__dict__.keys():
            if attr[0] != '_':
                actual = getattr(modes, attr)
                if isinstance(actual, DiffMode) and actual != DiffMode.Default:
                    expected = getattr(jmodes, attr)()
                    self.assertEqual(expected.toString(), actual.name, actual.name)
        self.assertIsNotNone(DiffMode.Default.name, jmodes.Default().toString())

    def test_diff_options_comparator_for(self):
        cmp1 = DiffComparators.default()
        cmp2 = DiffComparators.epsilon(0.01)
        cmp3 = DiffComparators.string()

        opts = DiffOptions() \
            .with_column_name_comparator(cmp1, "abc", "def") \
            .with_data_type_comparator(cmp2, LongType()) \
            .with_default_comparator(cmp3)

        self.assertEqual(opts.comparator_for(StructField("abc", IntegerType())), cmp1)
        self.assertEqual(opts.comparator_for(StructField("def", LongType())), cmp1)
        self.assertEqual(opts.comparator_for(StructField("ghi", LongType())), cmp2)
        self.assertEqual(opts.comparator_for(StructField("jkl", IntegerType())), cmp3)

    def test_diff_fluent_setters(self):
        cmp1 = DiffComparators.default()
        cmp2 = DiffComparators.epsilon(0.01)
        cmp3 = DiffComparators.string()
        cmp4 = DiffComparators.duration('PT24H')

        default = DiffOptions()
        options = default \
            .with_diff_column('d') \
            .with_left_column_prefix('l') \
            .with_right_column_prefix('r') \
            .with_insert_diff_value('i') \
            .with_change_diff_value('c') \
            .with_delete_diff_value('r') \
            .with_nochange_diff_value('n') \
            .with_change_column('c') \
            .with_diff_mode(DiffMode.SideBySide) \
            .with_sparse_mode(True) \
            .with_default_comparator(cmp1) \
            .with_data_type_comparator(cmp2, IntegerType()) \
            .with_data_type_comparator(cmp3, StringType()) \
            .with_column_name_comparator(cmp4, 'value')

        self.assertEqual(options.diff_column, 'd')
        self.assertEqual(options.left_column_prefix, 'l')
        self.assertEqual(options.right_column_prefix, 'r')
        self.assertEqual(options.insert_diff_value, 'i')
        self.assertEqual(options.change_diff_value, 'c')
        self.assertEqual(options.delete_diff_value, 'r')
        self.assertEqual(options.nochange_diff_value, 'n')
        self.assertEqual(options.change_column, 'c')
        self.assertEqual(options.diff_mode, DiffMode.SideBySide)
        self.assertEqual(options.sparse_mode, True)
        self.assertEqual(options.default_comparator, cmp1)
        self.assertEqual(options.data_type_comparators, {IntegerType(): cmp2, StringType(): cmp3})
        self.assertEqual(options.column_name_comparators, {'value': cmp4})

        self.assertNotEqual(options.diff_column, default.diff_column)
        self.assertNotEqual(options.left_column_prefix, default.left_column_prefix)
        self.assertNotEqual(options.right_column_prefix, default.right_column_prefix)
        self.assertNotEqual(options.insert_diff_value, default.insert_diff_value)
        self.assertNotEqual(options.change_diff_value, default.change_diff_value)
        self.assertNotEqual(options.delete_diff_value, default.delete_diff_value)
        self.assertNotEqual(options.nochange_diff_value, default.nochange_diff_value)
        self.assertNotEqual(options.change_column, default.change_column)
        self.assertNotEqual(options.diff_mode, default.diff_mode)
        self.assertNotEqual(options.sparse_mode, default.sparse_mode)

        without_change = options.without_change_column()
        self.assertEqual(without_change.diff_column, 'd')
        self.assertEqual(without_change.left_column_prefix, 'l')
        self.assertEqual(without_change.right_column_prefix, 'r')
        self.assertEqual(without_change.insert_diff_value, 'i')
        self.assertEqual(without_change.change_diff_value, 'c')
        self.assertEqual(without_change.delete_diff_value, 'r')
        self.assertEqual(without_change.nochange_diff_value, 'n')
        self.assertIsNone(without_change.change_column)
        self.assertEqual(without_change.diff_mode, DiffMode.SideBySide)
        self.assertEqual(without_change.sparse_mode, True)

    def test_diff_with_epsilon_comparator(self):
        # relative inclusive epsilon
        options = DiffOptions() \
            .with_column_name_comparator(DiffComparators.epsilon(0.1).as_relative().as_inclusive(), 'val')
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        expected = self.spark.createDataFrame(self.expected_diff) \
            .withColumn("diff", when(col("id") == 1, "N").otherwise(col("diff"))) \
            .collect()
        self.assertEqual(expected, diff)

        # relative exclusive epsilon
        options = DiffOptions() \
            .with_column_name_comparator(DiffComparators.epsilon(0.0909).as_relative().as_exclusive(), 'val')
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff, diff)

        # absolute inclusive epsilon
        options = DiffOptions() \
            .with_column_name_comparator(DiffComparators.epsilon(0.10000000000000009).as_absolute().as_inclusive(), 'val')
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        self.assertEqual(expected, diff)

        # absolute exclusive epsilon
        options = DiffOptions() \
            .with_column_name_comparator(DiffComparators.epsilon(0.10000000000000009).as_absolute().as_exclusive(), 'val')
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff, diff)

    def test_diff_options_with_duplicate_comparators(self):
        options = DiffOptions() \
            .with_data_type_comparator(DiffComparators.default(), DateType(), IntegerType()) \
            .with_column_name_comparator(DiffComparators.default(), 'col1', 'col2')

        with self.assertRaisesRegex(ValueError, "A comparator for data type date exists already."):
            options.with_data_type_comparator(DiffComparators.default(), DateType())

        with self.assertRaisesRegex(ValueError, "A comparator for data type int exists already."):
            options.with_data_type_comparator(DiffComparators.default(), IntegerType())

        with self.assertRaisesRegex(ValueError, "A comparator for data types date, int exists already."):
            options.with_data_type_comparator(DiffComparators.default(), DateType(), IntegerType())

        with self.assertRaisesRegex(ValueError, "A comparator for column name col1 exists already."):
            options.with_column_name_comparator(DiffComparators.default(), 'col1')

        with self.assertRaisesRegex(ValueError, "A comparator for column name col2 exists already."):
            options.with_column_name_comparator(DiffComparators.default(), 'col2')

        with self.assertRaisesRegex(ValueError, "A comparator for column names col1, col2 exists already."):
            options.with_column_name_comparator(DiffComparators.default(), 'col1', 'col2')


if __name__ == '__main__':
    SparkTest.main(__file__)
