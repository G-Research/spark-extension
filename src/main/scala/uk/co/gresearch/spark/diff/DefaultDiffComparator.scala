package uk.co.gresearch.spark.diff
import org.apache.spark.sql.Column

case object DefaultDiffComparator extends DiffComparator {
  override def compare(left: Column, right: Column): Column = left <=> right
}
