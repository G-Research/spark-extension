package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.Column

case object NullSafeEqualDiffComparator extends DiffComparator {
  override def equiv(left: Column, right: Column): Column = left <=> right
}
