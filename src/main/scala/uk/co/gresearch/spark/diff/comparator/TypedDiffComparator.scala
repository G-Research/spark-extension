package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.types.DataType

trait TypedDiffComparator extends DiffComparator {
  def inputType: DataType
}
