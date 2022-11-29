import abc
import dataclasses
from dataclasses import dataclass

from py4j.java_gateway import JVMView, JavaObject

from gresearch.spark import _get_scala_object


class DiffComparator(abc.ABC):
    @abc.abstractmethod
    def _to_java(self, jvm: JVMView) -> JavaObject:
        pass

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
    def duration(duration: str) -> 'DurationDiffComparator':
        return DurationDiffComparator(duration)


class DefaultDiffComparator(DiffComparator):
    def _to_java(self, jvm: JVMView) -> JavaObject:
        return _get_scala_object(jvm, "uk.co.gresearch.spark.diff.comparator.DefaultDiffComparator")


class NullSafeEqualDiffComparator(DiffComparator):
    def _to_java(self, jvm: JVMView) -> JavaObject:
        return _get_scala_object(jvm, "uk.co.gresearch.spark.diff.comparator.NullSafeEqualDiffComparator")


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