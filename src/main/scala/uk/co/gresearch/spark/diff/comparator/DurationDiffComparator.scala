package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.abs
import uk.co.gresearch.spark.diff.DiffComparator

import java.time.Duration

case class DurationDiffComparator(duration: Duration, inclusive: Boolean = true) extends DiffComparator {
  override def equiv(left: Column, right: Column): Column = {
    val inDuration = if (inclusive)
      (diff: Column) => diff <= duration
    else
      (diff: Column) => diff < duration

    left.isNull && right.isNull ||
      left.isNotNull && right.isNotNull && inDuration(abs(left - right))
  }
}
