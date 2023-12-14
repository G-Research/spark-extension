#  Copyright 2022 G-Research
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

from pyspark.storagelevel import StorageLevel

from spark_common import SparkTest
import gresearch.spark


class RowNumberTest(SparkTest):

    @classmethod
    def setUpClass(cls):
        super(RowNumberTest, cls).setUpClass()

        cls.df1 = cls.spark.createDataFrame([
            (1, 'one'),
            (2, 'two'),
            (3, 'three'),
            (4, 'four'),
        ], ['id', 'value'])
        cls.expected1 = [
            {'id': 1, 'value': 'one', 'row_number': 1},
            {'id': 2, 'value': 'two', 'row_number': 2},
            {'id': 3, 'value': 'three', 'row_number': 3},
            {'id': 4, 'value': 'four', 'row_number': 4},
        ]
        cls.expected1Desc = [
            {'id': 1, 'value': 'one', 'row_number': 4},
            {'id': 2, 'value': 'two', 'row_number': 3},
            {'id': 3, 'value': 'three', 'row_number': 2},
            {'id': 4, 'value': 'four', 'row_number': 1},
        ]

        cls.df2 = cls.spark.createDataFrame([
            (1, 'one'),
            (2, 'TWO'),
            (2, 'two'),
            (3, 'three'),
        ], ['id', 'value'])
        cls.expected2 = [
            {'id': 1, 'value': 'one', 'row_number': 1},
            {'id': 2, 'value': 'TWO', 'row_number': 2},
            {'id': 2, 'value': 'two', 'row_number': 3},
            {'id': 3, 'value': 'three', 'row_number': 4},
        ]
        cls.expected2Desc = [
            {'id': 1, 'value': 'one', 'row_number': 4},
            {'id': 2, 'value': 'TWO', 'row_number': 3},
            {'id': 2, 'value': 'two', 'row_number': 2},
            {'id': 3, 'value': 'three', 'row_number': 1},
        ]

    def test_row_numbers(self):
        rows = self.df1.with_row_numbers().orderBy('id', 'value').collect()
        self.assertEqual(self.expected1, [row.asDict() for row in rows])

    def test_row_numbers_order_one_column(self):
        for order in ['id', ['id'], self.df1.id, [self.df1.id]]:
            with self.subTest(order=order):
                rows = self.df1.with_row_numbers(order=order).orderBy('id', 'value').collect()
                self.assertEqual(self.expected1, [row.asDict() for row in rows])

    def test_row_numbers_order_two_columns(self):
        for order in [['id', 'value'], [self.df2.id, self.df2.value]]:
            with self.subTest(order=order):
                rows = self.df2.with_row_numbers(order=order).orderBy('id', 'value').collect()
                self.assertEqual(self.expected2, [row.asDict() for row in rows])

    def test_row_numbers_order_not_asc_one_column(self):
        for order in ['id', ['id'], self.df1.id, [self.df1.id]]:
            with self.subTest(order=order):
                rows = self.df1.with_row_numbers(order=order, ascending=False).orderBy('id', 'value').collect()
                self.assertEqual(self.expected1Desc, [row.asDict() for row in rows])

    def test_row_numbers_order_not_asc_two_columns(self):
        for order in [['id', 'value'], [self.df2.id, self.df2.value]]:
            with self.subTest(order=order):
                rows = self.df2.with_row_numbers(order=order, ascending=False).orderBy('id', 'value').collect()
                self.assertEqual(self.expected2Desc, [row.asDict() for row in rows])

    def test_row_numbers_order_desc_one_column(self):
        for order in [self.df1.id.desc(), [self.df1.id.desc()]]:
            with self.subTest(order=order):
                rows = self.df1.with_row_numbers(order=order).orderBy('id', 'value').collect()
                self.assertEqual(self.expected1Desc, [row.asDict() for row in rows])

    def test_row_numbers_order_desc_two_columns(self):
        for order in [[self.df2.id.desc(), self.df2.value.desc()]]:
            with self.subTest(order=order):
                rows = self.df2.with_row_numbers(order=order).orderBy('id', 'value').collect()
                self.assertEqual(self.expected2Desc, [row.asDict() for row in rows])

    def test_row_numbers_unpersist(self):
        for storage_level in [StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_ONLY, StorageLevel.DISK_ONLY]:
            with self.subTest(storage_level=storage_level):
                # make sure the cache is clear
                jcm = self.spark._jsparkSession.sharedState().cacheManager()
                jcm.clearCache()
                self.assertTrue(jcm.isEmpty())

                unpersist = self.spark.unpersist_handle()
                self.df1.with_row_numbers(storage_level=storage_level, unpersist_handle=unpersist) \
                    .orderBy('id', 'value').collect()

                # the cache should not be empty now
                self.assertFalse(jcm.isEmpty())
                unpersist(blocking=True)

                # this should have removed the only DataFrame from the cache
                self.assertTrue(jcm.isEmpty())

                # calling unpersist again does not hurt, this time without blocking
                unpersist()

    def test_row_numbers_row_number_col_name(self):
        rows = self.df1.with_row_numbers(row_number_column_name='row').orderBy('id', 'value').collect()
        self.assertEqual([{'row' if k == 'row_number' else k: v for k, v in row.items()}
                          for row in self.expected1],
                         [row.asDict() for row in rows])


if __name__ == '__main__':
    SparkTest.main(__file__)
