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

import abc
import dataclasses
from dataclasses import dataclass

from py4j.java_gateway import JVMView, JavaObject

from pyspark.sql.types import DataType


class DiffComparator(abc.ABC):
    @abc.abstractmethod
    def _to_java(self, jvm: JVMView) -> JavaObject:
        pass


class DiffComparators:
    @staticmethod
    def default() -> 'DefaultDiffComparator':
        return DefaultDiffComparator()

    @staticmethod
    def nullSafeEqual() -> 'NullSafeEqualDiffComparator':
        return NullSafeEqualDiffComparator()

    @staticmethod
    def epsilon(epsilon: float) -> 'EpsilonDiffComparator':
        return EpsilonDiffComparator(epsilon)

    @staticmethod
    def string(whitespace_agnostic: bool = True) -> 'StringDiffComparator':
        return StringDiffComparator(whitespace_agnostic)

    @staticmethod
    def duration(duration: str) -> 'DurationDiffComparator':
        return DurationDiffComparator(duration)

    @staticmethod
    def map(key_type: DataType, value_type: DataType, key_order_sensitive: bool = False) -> 'MapDiffComparator':
        return MapDiffComparator(key_type, value_type, key_order_sensitive)


class DefaultDiffComparator(DiffComparator):
    def _to_java(self, jvm: JVMView) -> JavaObject:
        return jvm.uk.co.gresearch.spark.diff.DiffComparators.default()


class NullSafeEqualDiffComparator(DiffComparator):
    def _to_java(self, jvm: JVMView) -> JavaObject:
        return jvm.uk.co.gresearch.spark.diff.DiffComparators.nullSafeEqual()


@dataclass(frozen=True)
class EpsilonDiffComparator(DiffComparator):
    epsilon: float
    relative: bool = True
    inclusive: bool = True

    def as_relative(self) -> 'EpsilonDiffComparator':
        return dataclasses.replace(self, relative=True)

    def as_absolute(self) -> 'EpsilonDiffComparator':
        return dataclasses.replace(self, relative=False)

    def as_inclusive(self) -> 'EpsilonDiffComparator':
        return dataclasses.replace(self, inclusive=True)

    def as_exclusive(self) -> 'EpsilonDiffComparator':
        return dataclasses.replace(self, inclusive=False)

    def _to_java(self, jvm: JVMView) -> JavaObject:
        return jvm.uk.co.gresearch.spark.diff.comparator.EpsilonDiffComparator(self.epsilon, self.relative, self.inclusive)


@dataclass(frozen=True)
class StringDiffComparator(DiffComparator):
    whitespace_agnostic: bool

    def _to_java(self, jvm: JVMView) -> JavaObject:
        return jvm.uk.co.gresearch.spark.diff.DiffComparators.string(self.whitespace_agnostic)


@dataclass(frozen=True)
class DurationDiffComparator(DiffComparator):
    duration: str
    inclusive: bool = True

    def as_inclusive(self) -> 'DurationDiffComparator':
        return dataclasses.replace(self, inclusive=True)

    def as_exclusive(self) -> 'DurationDiffComparator':
        return dataclasses.replace(self, inclusive=False)

    def _to_java(self, jvm: JVMView) -> JavaObject:
        jduration = jvm.java.time.Duration.parse(self.duration)
        return jvm.uk.co.gresearch.spark.diff.comparator.DurationDiffComparator(jduration, self.inclusive)


@dataclass(frozen=True)
class MapDiffComparator(DiffComparator):
    key_type: DataType
    value_type: DataType
    key_order_sensitive: bool

    def _to_java(self, jvm: JVMView) -> JavaObject:
        from pyspark.sql import SparkSession

        jfromjson = jvm.org.apache.spark.sql.types.__getattr__("DataType$").__getattr__("MODULE$").fromJson
        jkeytype = jfromjson(self.key_type.json())
        jvaluetype = jfromjson(self.value_type.json())
        return jvm.uk.co.gresearch.spark.diff.DiffComparators.map(jkeytype, jvaluetype, self.key_order_sensitive)
