package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.abs
import uk.co.gresearch.spark
import uk.co.gresearch.spark.SparkVersion
import uk.co.gresearch.spark.diff.comparator.DurationDiffComparator.isNotSupportedBySpark

import java.time.Duration

/**
 * Compares two timestamps and considers them equal when they are less than
 * (or equal to when inclusive = true) a given duration apart.
 *
 * @param duration equality threshold
 * @param inclusive duration is considered equal when true
 */
case class DurationDiffComparator(duration: Duration, inclusive: Boolean = true) extends DiffComparator {
  if (isNotSupportedBySpark) {
    throw new UnsupportedOperationException(s"java.time.Duration is not supported by Spark ${spark.SparkCompatVersionString}")
  }

  override def equiv(left: Column, right: Column): Column = {
    val inDuration = if (inclusive)
      (diff: Column) => diff <= duration
    else
      (diff: Column) => diff < duration

    left.isNull && right.isNull ||
      left.isNotNull && right.isNotNull && inDuration(abs(left - right))
  }

  def asInclusive(): DurationDiffComparator = if (inclusive) this else copy(inclusive = true)
  def asExclusive(): DurationDiffComparator = if (inclusive) copy(inclusive = false) else this
}

object DurationDiffComparator extends SparkVersion {
  val isSupportedBySpark: Boolean = SparkMajorVersion == 3 && SparkMinorVersion >= 3 || SparkMajorVersion > 3
  val isNotSupportedBySpark: Boolean = ! isSupportedBySpark
}
