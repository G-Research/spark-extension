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

from spark_common import SparkTest
import gresearch.spark


class HistogramTest(SparkTest):

    @classmethod
    def setUpClass(cls):
        super(HistogramTest, cls).setUpClass()

        cls.df = cls.spark.createDataFrame([
            (1, 1),
            (1, 2),
            (1, 10),
            (2, -3),
            (2, 5),
            (3, 8),
        ], ['id', 'value'])

    def test_histogram_with_ints(self):
        hist = self.df.histogram([-5, 0, 5], 'value', 'id').orderBy('id').collect()
        self.assertEqual([
            {'id': 1, '≤-5': 0, '≤0': 0, '≤5': 2, '>5': 1},
            {'id': 2, '≤-5': 0, '≤0': 1, '≤5': 1, '>5': 0},
            {'id': 3, '≤-5': 0, '≤0': 0, '≤5': 0, '>5': 1},
        ], [row.asDict() for row in hist])

    def test_histogram_with_floats(self):
        hist = self.df.histogram([-5.0, 0.0, 5.0], 'value', 'id').orderBy('id').collect()
        self.assertEqual([
            {'id': 1, '≤-5.0': 0, '≤0.0': 0, '≤5.0': 2, '>5.0': 1},
            {'id': 2, '≤-5.0': 0, '≤0.0': 1, '≤5.0': 1, '>5.0': 0},
            {'id': 3, '≤-5.0': 0, '≤0.0': 0, '≤5.0': 0, '>5.0': 1},
        ], [row.asDict() for row in hist])


if __name__ == '__main__':
    SparkTest.main(__file__)
