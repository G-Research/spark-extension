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
import os
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path

from pyspark import SparkConf
from pyspark.sql import SparkSession

logger = logging.getLogger()
logger.level = logging.INFO


@contextmanager
def spark_session():
    session = SparkTest.get_spark_session()
    try:
        yield session
    finally:
        session.stop()


class SparkTest(unittest.TestCase):

    @staticmethod
    def main(file: str):
        if len(sys.argv) == 2:
            # location to store test results provided, this requires package unittest-xml-reporting
            import xmlrunner

            unittest.main(
                module=f'test.{Path(file).name[:-3]}',
                testRunner=xmlrunner.XMLTestRunner(output=sys.argv[1]),
                argv=sys.argv[:1],
                # these make sure that some options that are not applicable
                # remain hidden from the help menu.
                failfast=False, buffer=False, catchbreak=False
            )
        else:
            unittest.main()

    @staticmethod
    def get_pom_path() -> str:
        paths = ['.', '..', os.path.join('..', '..')]
        for path in paths:
            if os.path.exists(os.path.join(path, 'pom.xml')):
                return path
        raise RuntimeError('Could not find path to pom.xml, looked here: {}'.format(', '.join(paths)))

    @staticmethod
    def get_spark_config(path) -> SparkConf:
        master = 'local[2]'
        conf = SparkConf().setAppName('unit test').setMaster(master)
        return conf.setAll([
            ('spark.ui.showConsoleProgress', 'false'),
            ('spark.test.home', os.environ.get('SPARK_HOME')),
            ('spark.locality.wait', '0'),
            ('spark.driver.extraClassPath', '{}'.format(':'.join([
                os.path.join(os.getcwd(), path, 'target', 'classes'),
                os.path.join(os.getcwd(), path, 'target', 'test-classes'),
            ]))),
        ])

    @classmethod
    def get_spark_session(cls) -> SparkSession:
        builder = SparkSession.builder

        if 'TEST_SPARK_CONNECT_SERVER' in os.environ:
            builder.remote(os.environ['TEST_SPARK_CONNECT_SERVER'])
        elif 'PYSPARK_GATEWAY_PORT' in os.environ:
            logging.info('Running inside existing Spark environment')
        else:
            logging.info('Setting up Spark environment')
            # setting conf spark.pyspark.python does not work
            os.environ['PYSPARK_PYTHON'] = sys.executable
            path = cls.get_pom_path()
            conf = cls.get_spark_config(path)
            builder.config(conf=conf)

        return builder.getOrCreate()

    spark: SparkSession = None
    is_spark_connect: bool = 'TEST_SPARK_CONNECT_SERVER' in os.environ

    @classmethod
    def setUpClass(cls):
        super(SparkTest, cls).setUpClass()
        logging.info('launching Spark session')
        cls.spark = cls.get_spark_session()

    @classmethod
    def tearDownClass(cls):
        logging.info('stopping Spark session')
        cls.spark.stop()
        super(SparkTest, cls).tearDownClass()

    @contextmanager
    def sql_conf(self, pairs):
        """
        Copied from pyspark/testing/sqlutils available from PySpark 3.5.0 and higher.
        https://github.com/apache/spark/blob/v3.5.0/python/pyspark/testing/sqlutils.py#L171
        http://www.apache.org/licenses/LICENSE-2.0

        A convenient context manager to test some configuration specific logic. This sets
        `value` to the configuration `key` and then restores it back when it exits.
        """
        assert isinstance(pairs, dict), "pairs should be a dictionary."
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        keys = pairs.keys()
        new_values = pairs.values()
        old_values = [self.spark.conf.get(key, None) for key in keys]
        for key, new_value in zip(keys, new_values):
            self.spark.conf.set(key, new_value)
        try:
            yield
        finally:
            for key, old_value in zip(keys, old_values):
                if old_value is None:
                    self.spark.conf.unset(key)
                else:
                    self.spark.conf.set(key, old_value)
