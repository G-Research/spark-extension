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

from typing import List, Any, Union, Optional

from py4j.java_gateway import JVMView, JavaObject
from pyspark.sql import DataFrame, Row


def _to_seq(jvm: JVMView, list: List[Any]) -> JavaObject:
    array = jvm.java.util.ArrayList(list)
    return jvm.scala.collection.JavaConverters.asScalaIteratorConverter(array.iterator()).asScala().toSeq()


def _get_scala_object(jvm: JVMView, name: str) -> JavaObject:
    clazz = jvm.java.lang.Class.forName('{}$'.format(name))
    ff = clazz.getDeclaredField("MODULE$")
    return ff.get(None)


def histogram(self: DataFrame,
              thresholds: List[Union[int, float]],
              value_column: str,
              *aggregate_columns: str) -> DataFrame:

    if len(thresholds) == 0:
        t = 'Int'
    else:
        t = type(thresholds[0])
        if t == int:
            t = 'Int'
        elif t == float:
            t = 'Double'
        else:
            raise ValueError('thresholds must be int or floats: {}'.format(t))

    jvm = self._sc._jvm
    col = jvm.org.apache.spark.sql.functions.col
    value_column = col(value_column)
    aggregate_columns = [col(column) for column in aggregate_columns]

    ordering = _get_scala_object(jvm, 'scala.math.Ordering${}'.format(t))
    hist = _get_scala_object(jvm, 'uk.co.gresearch.spark.Histogram')
    jdf = hist.of(self._jdf, _to_seq(jvm, thresholds), value_column, _to_seq(jvm, aggregate_columns), ordering)
    return DataFrame(jdf, self.sql_ctx)


DataFrame.histogram = histogram


from pyspark.sql import Column, dataframe, column
basestring = unicode = str


def observe(self: DataFrame, name: str, *exprs: Column) -> DataFrame:
    assert isinstance(name, basestring), "name should be a string"
    assert exprs, "exprs should not be empty"
    assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
    jdf = self._jdf.observe(name,
                            exprs[0]._jc,
                            dataframe._to_seq(self.sql_ctx._sc, [c._jc for c in exprs[1:]]))
    return DataFrame(jdf, self.sql_ctx)


#DataFrame.observe = observe


class Observation:
    def __init__(self, name: str, *exprs: Column):
        assert isinstance(name, basestring), "name should be a string"
        assert exprs, "exprs should not be empty"
        assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
        self._name = name
        self._exprs = list(exprs)
        self._jvm = None
        self._jo = None

    def observe(self, df: DataFrame) -> DataFrame:
        if not self._jo:
            self._jvm = df._sc._jvm
            self._jo = self._jvm.uk.co.gresearch.spark.Observation(
                self._name,
                self._exprs[0]._jc,
                column._to_seq(df._sc, [c._jc for c in self._exprs[1:]])
            )
        return DataFrame(self._jo.observe(df._jdf), df.sql_ctx)

    @property
    def get(self) -> Row:
        if self._jo is None:
            raise RuntimeError('call observe first')
        jrow = self._jo.get()
        return self._to_row(jrow)

    @property
    def waitAndGet(self) -> Row:
        if self._jo is None:
            raise RuntimeError('call observe first')
        jrow = self._jo.waitAndGet()
        return self._to_row(jrow)

    def waitCompleted(self, millis: Optional[int] = None) -> bool:
        if self._jo is None:
            raise RuntimeError('call observe first')
        unit = self._jvm.java.util.concurrent.TimeUnit.MILLISECONDS
        return self._jo.waitCompleted(millis, unit)

    def _to_row(self, jrow):
        field_names = jrow.schema().fieldNames()
        valuesScalaMap = jrow.getValuesMap(self._jvm.PythonUtils.toSeq(list(field_names)))
        valuesJavaMap = self._jvm.scala.collection.JavaConversions.mapAsJavaMap(valuesScalaMap)
        return Row(**valuesJavaMap)


def observation(self: DataFrame, observation: Union[str, Observation], *exprs: Column) -> DataFrame:
    if isinstance(observation, Observation):
        assert not exprs, "exprs should not be given when Observation given"
        return observation.observe(self)
    else:
        assert isinstance(observation, basestring), "name should be a string"
        assert exprs, "exprs should not be empty"
        assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
        jdf = self._jdf.observe(observation,
                                exprs[0]._jc,
                                dataframe._to_seq(self.sql_ctx._sc, [c._jc for c in exprs[1:]]))
        return DataFrame(jdf, self.sql_ctx)


DataFrame.observe = observation
