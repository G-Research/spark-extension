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
import time

from pyspark.sql import Row
import pyspark.sql.functions as func
from py4j.java_gateway import JavaObject

from spark_common import SparkTest
import gresearch.spark
from gresearch.spark import Observation


class ObservationTest(SparkTest):

    @classmethod
    def setUpClass(cls):
        super(ObservationTest, cls).setUpClass()

        cls.df = cls.spark.createDataFrame([
            (1, 1.0, 'one'),
            (2, 2.0, 'two'),
            (3, 3.0, 'three'),
        ], ['id', 'val', 'label'])

    def test_observe(self):
        # we do not bother accessing the listeners to obtain the metric
        # this just tests that pyspark DataFrame.observe exists and works transparently
        observed = self.df.observe("metric", func.count(func.lit(1)), func.sum(func.col("val")))
        actual = observed.orderBy('id').collect()
        self.assertEqual([
            {'id': 1, 'val': 1.0, 'label': 'one'},
            {'id': 2, 'val': 2.0, 'label': 'two'},
            {'id': 3, 'val': 3.0, 'label': 'three'},
        ], [row.asDict() for row in actual])

    def test_observation(self):
        #self.assertTrue(self.df._sc._gateway.start_callback_server())
        #listener = TaskEndListener()
        #printer = PrintQueryExecutionListener()
        #self.df.sql_ctx._jsc.sc().addSparkListener(listener)
        #self.spark._jvm.org.apache.spark.sql.SQLContext(self.df.sql_ctx._jsc.sc()).sparkSession().listenerManager().register(printer)

        observation = Observation(
            'metric',
            func.count(func.lit(1)).alias('count'),
            func.sum(func.col("id")).alias('id_sum'),
            func.sum(func.col("val")).alias('val_sum')
        )
        observed = self.df.orderBy('id').observe(observation)
        self.assertFalse(observation.waitCompleted(1000))

        actual = observed.orderBy('id').collect()
        self.assertEqual([
            {'id': 1, 'val': 1.0, 'label': 'one'},
            {'id': 2, 'val': 2.0, 'label': 'two'},
            {'id': 3, 'val': 3.0, 'label': 'three'},
        ], [row.asDict() for row in actual])

        self.assertTrue(observation.waitCompleted(1000))
        self.assertEqual(observation.waitAndGet, Row(count=3, id_sum=6, val_sum=6.0))
        self.assertEqual(observation.get, Row(count=3, id_sum=6, val_sum=6.0))


if __name__ == '__main__':
    unittest.main()
