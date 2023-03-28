#  Copyright 2023 G-Research
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
import datetime

from gresearch.spark import dotnet_ticks_to_timestamp, dotnet_ticks_to_unix_epoch
from spark_common import SparkTest
from decimal import Decimal


class PackageTest(SparkTest):

    @classmethod
    def setUpClass(cls):
        super(PackageTest, cls).setUpClass()

        cls.df = cls.spark.createDataFrame([
            (1, 599266080000000000),
            (2, 621355968000000000),
            (3, 638155413748959308),
            (4, 638155413748959309),
            (5, 638155413748959310),
            (6, 946723967999999999)
        ], ['id', 'ticks'])

    def test_dotnet_ticks_to_timestamp(self):
        self.maxDiff = None
        for column in ["ticks", self.df.ticks]:
            with self.subTest(column=column):
                timestamps = self.df.withColumn("timestamp", dotnet_ticks_to_timestamp(column)).orderBy('id').collect()
                self.assertEqual([
                    {'id': 1, 'ticks': 599266080000000000, 'timestamp': datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc).astimezone().replace(tzinfo=None)},
                    {'id': 2, 'ticks': 621355968000000000, 'timestamp': datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc).astimezone().replace(tzinfo=None)},
                    {'id': 3, 'ticks': 638155413748959308, 'timestamp': datetime.datetime(2023, 3, 27, 19, 16, 14, 895930, datetime.timezone.utc).astimezone().replace(tzinfo=None)},
                    {'id': 4, 'ticks': 638155413748959309, 'timestamp': datetime.datetime(2023, 3, 27, 19, 16, 14, 895930, datetime.timezone.utc).astimezone().replace(tzinfo=None)},
                    {'id': 5, 'ticks': 638155413748959310, 'timestamp': datetime.datetime(2023, 3, 27, 19, 16, 14, 895931, datetime.timezone.utc).astimezone().replace(tzinfo=None)},
                    {'id': 6, 'ticks': 946723967999999999, 'timestamp': datetime.datetime(3001, 1, 19, 7, 59, 59, 999999, datetime.timezone.utc).astimezone().replace(tzinfo=None)},
                ], [row.asDict() for row in timestamps])

    def test_dotnet_ticks_to_unix_epoch(self):
        self.maxDiff = None
        for column in ["ticks", self.df.ticks]:
            with self.subTest(column=column):
                timestamps = self.df.withColumn("timestamp", dotnet_ticks_to_unix_epoch(column)).orderBy('id').collect()
                self.assertEqual([
                    {'id': 1, 'ticks': 599266080000000000, 'timestamp': Decimal('-2208988800.000000000')},
                    {'id': 2, 'ticks': 621355968000000000, 'timestamp': Decimal('0E-9')},
                    {'id': 3, 'ticks': 638155413748959308, 'timestamp': Decimal('1679944574.895930800')},
                    {'id': 4, 'ticks': 638155413748959309, 'timestamp': Decimal('1679944574.895930900')},
                    {'id': 5, 'ticks': 638155413748959310, 'timestamp': Decimal('1679944574.895931000')},
                    {'id': 6, 'ticks': 946723967999999999, 'timestamp': Decimal('32536799999.999999900')},
                ], [row.asDict() for row in timestamps])


if __name__ == '__main__':
    SparkTest.main()
