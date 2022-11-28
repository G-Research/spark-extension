package uk.co.gresearch.spark.diff

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, Decimal}
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

  def epsilon(epsilon: Double): EpsilonDiffComparator = EpsilonDiffComparator(epsilon)

  def duration(duration: Duration): DurationDiffComparator = DurationDiffComparator(duration)
}
