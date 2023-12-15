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

from pyspark import TaskContext, SparkContext
from typing import Optional

from spark_common import SparkTest
from gresearch.spark import job_description, append_job_description


class JobDescriptionTest(SparkTest):

    def _assert_job_description(self, expected: Optional[str]):
        def get_job_description_func(part):
            def func(row):
                return row.id, part, TaskContext.get().getLocalProperty("spark.job.description")
            return func

        descriptions = self.spark.range(3, numPartitions=3).rdd \
            .mapPartitionsWithIndex(lambda part, it: map(get_job_description_func(part), it)) \
            .collect()
        self.assertEqual(
            [(0, 0, expected), (1, 1, expected), (2, 2, expected)],
            descriptions
        )

    def setUp(self) -> None:
        SparkContext._active_spark_context.setJobDescription(None)

    def test_with_job_description(self):
        self._assert_job_description(None)
        with job_description("job description"):
            self._assert_job_description("job description")
            with job_description("inner job description"):
                self._assert_job_description("inner job description")
            self._assert_job_description("job description")
            with job_description("inner job description", True):
                self._assert_job_description("job description")
            self._assert_job_description("job description")
        self._assert_job_description(None)
        with job_description("other job description", True):
            self._assert_job_description("other job description")
        self._assert_job_description(None)

    def test_append_job_description(self):
        self._assert_job_description(None)
        with append_job_description("job"):
            self._assert_job_description("job")
            with append_job_description("description"):
                self._assert_job_description("job - description")
            self._assert_job_description("job")
            with append_job_description("description 2", " "):
                self._assert_job_description("job description 2")
            self._assert_job_description("job")
        self._assert_job_description(None)


if __name__ == '__main__':
    SparkTest.main(__file__)
