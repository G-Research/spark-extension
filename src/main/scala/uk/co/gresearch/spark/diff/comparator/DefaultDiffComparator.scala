package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.Column
import uk.co.gresearch.spark.diff.DiffComparator

case object DefaultDiffComparator extends DiffComparator {
  override def compare(left: Column, right: Column): Column = left <=> right
}
