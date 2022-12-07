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
import logging
import os
import subprocess
import sys
import unittest

from pyspark import SparkConf
from pyspark.sql import SparkSession

logger = logging.getLogger()
logger.level = logging.INFO


@contextlib.contextmanager
def spark_session():
    session = SparkTest.get_spark_session()
    try:
        yield session
    finally:
        session.stop()


class SparkTest(unittest.TestCase):

    @staticmethod
    def main():
        if len(sys.argv) == 2:
            # location to store test results provided, this requires package unittest-xml-reporting
            import xmlrunner

            unittest.main(
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
    def get_dependencies_from_mvn(path) -> str:
        logging.info('running mvn to get JVM dependencies')
        dependencies_process = subprocess.run(
            ['/bin/bash', '-c', 'mvn dependency:build-classpath 2>/dev/null | grep -A 1 "Dependencies classpath:$" | tail -n 1'],
            cwd=path, stdout=subprocess.PIPE
        )
        if dependencies_process.returncode != 0:
            raise RuntimeError("failed to run mvn to get classpath for JVM")
        return str(dependencies_process.stdout.strip())

    @staticmethod
    def get_spark_config(path, dependencies) -> SparkConf:
        master = 'local[2]'
        conf = SparkConf().setAppName('unit test').setMaster(master)
        return conf.setAll([
            ('spark.ui.showConsoleProgress', 'false'),
            ('spark.test.home', os.environ.get('SPARK_HOME')),
            ('spark.locality.wait', '0'),
            ('spark.driver.extraClassPath', '{}'.format(':'.join([
                os.path.join(os.getcwd(), path, 'target', 'classes'),
                os.path.join(os.getcwd(), path, 'target', 'test-classes'),
                dependencies
            ]))),
        ])

    @classmethod
    def get_spark_session(cls) -> SparkSession:
        builder = SparkSession.builder

        if 'PYSPARK_GATEWAY_PORT' in os.environ:
            logging.info('Running inside existing Spark environment')
        else:
            logging.info('Setting up Spark environment')
            path = cls.get_pom_path()
            dependencies = cls.get_dependencies_from_mvn(path)
            logging.info('found {} JVM dependencies'.format(len(dependencies.split(':'))))
            conf = cls.get_spark_config(path, dependencies)
            builder.config(conf=conf)

        return builder.getOrCreate()

    spark: SparkSession = None

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
