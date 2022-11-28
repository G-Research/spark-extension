package uk.co.gresearch.spark.diff

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, Encoder}
import uk.co.gresearch.spark.diff.comparator._

import java.time.Duration

trait DiffComparator {
  def equiv(left: Column, right: Column): Column
}

object DiffComparator {
  def default(): DiffComparator = DefaultDiffComparator

  def nullSafeEqual(): DiffComparator = NullSafeEqualDiffComparator

  def equiv[T : Encoder](equiv: math.Equiv[T]): EquivDiffComparator[T] = EquivDiffComparator(equiv)
  def equiv[T](equiv: math.Equiv[T], inputType: DataType): EquivDiffComparator[T] = EquivDiffComparator(equiv, inputType)
  def equiv(equiv: math.Equiv[Any]): EquivDiffComparator[Any] = EquivDiffComparator(equiv)

  def epsilon(epsilon: Double, relative: Boolean = true, inclusive: Boolean = true): EpsilonDiffComparator =
    EpsilonDiffComparator(epsilon, relative = relative, inclusive = inclusive)

  def duration(duration: Duration, inclusive: Boolean = true): DurationDiffComparator =
    DurationDiffComparator(duration, inclusive = inclusive)
}
