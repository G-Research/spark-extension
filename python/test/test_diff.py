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

import logging

logger = logging.getLogger()
logger.level = logging.INFO

import unittest
import re

from pyspark.sql import Row
from py4j.java_gateway import JavaObject

from spark_common import SparkTest
from gresearch.spark.diff import Diff, DiffOptions


class DiffTest(SparkTest):

    @classmethod
    def setUpClass(cls):
        super(DiffTest, cls).setUpClass()

        cls.left_df = cls.spark.createDataFrame([
            (1, 1.0, 'one'),
            (2, 2.0, 'two'),
            (3, 3.0, 'three'),
            (4, None, None),
            (5, 5.0, 'five'),
            (7, 7.0, 'seven'),
        ], ['id', 'val', 'label'])

        cls.right_df = cls.spark.createDataFrame([
            (1, 1.1, 'one'),
            (2, 2.0, 'Two'),
            (3, 3.0, 'three'),
            (4, 4.0, 'four'),
            (5, None, None),
            (6, 6.0, 'six'),
        ], ['id', 'val', 'label'])

        diff_row = Row('diff', 'id', 'left_val', 'right_val', 'left_label', 'right_label')
        cls.expected_diff = [
            diff_row('C', 1, 1.0, 1.1, 'one', 'one'),
            diff_row('C', 2, 2.0, 2.0, 'two', 'Two'),
            diff_row('N', 3, 3.0, 3.0, 'three', 'three'),
            diff_row('C', 4, None, 4., None, 'four'),
            diff_row('C', 5, 5.0, None, 'five', None),
            diff_row('I', 6, None, 6.0, None, 'six'),
            diff_row('D', 7, 7.0, None, 'seven', None),
        ]

        diff_with_options_row = Row('d', 'id', 'l_val', 'r_val', 'l_label', 'r_label')
        cls.expected_diff_with_options = [
            diff_with_options_row('c', 1, 1.0, 1.1, 'one', 'one'),
            diff_with_options_row('c', 2, 2.0, 2.0, 'two', 'Two'),
            diff_with_options_row('n', 3, 3.0, 3.0, 'three', 'three'),
            diff_with_options_row('c', 4, None, 4., None, 'four'),
            diff_with_options_row('c', 5, 5.0, None, 'five', None),
            diff_with_options_row('i', 6, None, 6.0, None, 'six'),
            diff_with_options_row('r', 7, 7.0, None, 'seven', None),
        ]

        diff_with_changes_row = Row('diff', 'changes', 'id', 'left_val', 'right_val', 'left_label', 'right_label')
        cls.expected_diff_with_changes = [
            diff_with_changes_row('C', ['val'], 1, 1.0, 1.1, 'one', 'one'),
            diff_with_changes_row('C', ['label'], 2, 2.0, 2.0, 'two', 'Two'),
            diff_with_changes_row('N', [], 3, 3.0, 3.0, 'three', 'three'),
            diff_with_changes_row('C', ['val', 'label'], 4, None, 4., None, 'four'),
            diff_with_changes_row('C', ['val', 'label'], 5, 5.0, None, 'five', None),
            diff_with_changes_row('I', None, 6, None, 6.0, None, 'six'),
            diff_with_changes_row('D', None, 7, 7.0, None, 'seven', None),
        ]

    def test_dataframe_diff(self):
        diff = self.left_df.diff(self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff, diff)

    def test_dataframe_diff_with_default_options(self):
        diff = self.left_df.diff_with_options(self.right_df, DiffOptions(), 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff, diff)

    def test_dataframe_diff_with_options(self):
        options = DiffOptions('d', 'l', 'r', 'i', 'c', 'r', 'n', None)
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_with_options, diff)

    def test_dataframe_diff_with_changes(self):
        options = DiffOptions().with_change_column('changes')
        diff = self.left_df.diff_with_options(self.right_df, options, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_with_changes, diff)

    def test_diff_of(self):
        diff = Diff().of(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff, diff)

    def test_diff_of_with_default_options(self):
        options = DiffOptions()
        diff = Diff(options).of(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff, diff)

    def test_diff_of_with_options(self):
        options = DiffOptions('d', 'l', 'r', 'i', 'c', 'r', 'n', None)
        diff = Diff(options).of(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_with_options, diff)

    def test_diff_of_with_changes(self):
        options = DiffOptions().with_change_column('changes')
        diff = Diff(options).of(self.left_df, self.right_df, 'id').orderBy('id').collect()
        self.assertEqual(self.expected_diff_with_changes, diff)

    def test_diff_options_default(self):
        jvm = self.spark._jvm
        joptions = jvm.uk.co.gresearch.spark.diff.DiffOptions.default()
        options = DiffOptions()
        for attr in options.__dict__.keys():
            const = re.sub(r'_(.)', lambda match: match.group(1).upper(), attr)
            expected = getattr(joptions, const)()
            actual = getattr(options, attr)

            if type(expected) == JavaObject:
                # assuming an Option
                expected = expected.toString()
                actual = 'Some({})'.format(actual) if actual else 'None'

            self.assertEqual(expected, actual, '{} == {} ?'.format(attr, const))

    def test_diff_fluent_setters(self):
        default = DiffOptions()
        options = default \
            .with_diff_column('d') \
            .with_left_column_prefix('l') \
            .with_right_column_prefix('r') \
            .with_insert_diff_value('i') \
            .with_change_diff_value('c') \
            .with_delete_diff_value('r') \
            .with_nochange_diff_value('n') \
            .with_change_column('c')

        self.assertEqual(options.diff_column, 'd')
        self.assertEqual(options.left_column_prefix, 'l')
        self.assertEqual(options.right_column_prefix, 'r')
        self.assertEqual(options.insert_diff_value, 'i')
        self.assertEqual(options.change_diff_value, 'c')
        self.assertEqual(options.delete_diff_value, 'r')
        self.assertEqual(options.nochange_diff_value, 'n')
        self.assertEqual(options.change_column, 'c')

        self.assertNotEqual(options.diff_column, default.diff_column)
        self.assertNotEqual(options.left_column_prefix, default.left_column_prefix)
        self.assertNotEqual(options.right_column_prefix, default.right_column_prefix)
        self.assertNotEqual(options.insert_diff_value, default.insert_diff_value)
        self.assertNotEqual(options.change_diff_value, default.change_diff_value)
        self.assertNotEqual(options.delete_diff_value, default.delete_diff_value)
        self.assertNotEqual(options.nochange_diff_value, default.nochange_diff_value)
        self.assertNotEqual(options.change_column, default.change_column)

        without_change = options.without_change_column()
        self.assertEqual(without_change.diff_column, 'd')
        self.assertEqual(without_change.left_column_prefix, 'l')
        self.assertEqual(without_change.right_column_prefix, 'r')
        self.assertEqual(without_change.insert_diff_value, 'i')
        self.assertEqual(without_change.change_diff_value, 'c')
        self.assertEqual(without_change.delete_diff_value, 'r')
        self.assertEqual(without_change.nochange_diff_value, 'n')
        self.assertIsNone(without_change.change_column)


if __name__ == '__main__':
    unittest.main()
