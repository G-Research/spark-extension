package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{abs, greatest}
import uk.co.gresearch.spark.diff.DiffComparator

case class EpsilonDiffComparator(epsilon: Double, relative: Boolean = true, inclusive: Boolean = true)
  extends DiffComparator {
  override def equiv(left: Column, right: Column): Column = {
    val threshold = if (relative)
      greatest(abs(left), abs(right)) * epsilon
    else
      epsilon

    val inEpsilon = if (inclusive)
      (diff: Column) => diff <= threshold
    else
      (diff: Column) => diff < threshold

    left.isNull && right.isNull || left.isNotNull && right.isNotNull && inEpsilon(abs(left - right))
  }

  def asAbsolute(): EpsilonDiffComparator = if (relative) copy(relative = false) else this
  def asRelative(): EpsilonDiffComparator = if (relative) this else copy(relative = true)

  def asInclusive(): EpsilonDiffComparator = if (inclusive) this else copy(inclusive = true)
  def asExclusive(): EpsilonDiffComparator = if (inclusive) copy(inclusive = false) else this
}
