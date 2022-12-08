package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.abs
import uk.co.gresearch.spark.diff.comparator.DurationDiffComparator.isNotSupportedBySpark
import uk.co.gresearch.spark.{SparkCompatMajorVersion, SparkCompatMinorVersion}

import java.time.Duration

case class DurationDiffComparator(duration: Duration, inclusive: Boolean = true) extends DiffComparator {
  if (isNotSupportedBySpark) {
    throw new UnsupportedOperationException(s"java.time.Duration is not supported by Spark $SparkCompatMajorVersion.$SparkCompatMinorVersion")
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

object DurationDiffComparator {
  val isSupportedBySpark: Boolean = SparkCompatMajorVersion == 3 && SparkCompatMinorVersion >= 3 || SparkCompatMajorVersion > 3
  val isNotSupportedBySpark: Boolean = ! isSupportedBySpark
}
