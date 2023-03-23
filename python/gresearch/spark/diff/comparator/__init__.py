import abc
import dataclasses
from dataclasses import dataclass

from py4j.java_gateway import JVMView, JavaObject


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
