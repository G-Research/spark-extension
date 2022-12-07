package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.Column

case object DefaultDiffComparator extends DiffComparator {
  override def equiv(left: Column, right: Column): Column = NullSafeEqualDiffComparator.equiv(left, right)
}
