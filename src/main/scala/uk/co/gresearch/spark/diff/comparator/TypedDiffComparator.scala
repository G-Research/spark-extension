package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, StringType}

trait TypedDiffComparator extends DiffComparator {
  def inputType: DataType
}

trait StringDiffComparator extends TypedDiffComparator {
  override def inputType: DataType = StringType
}

case object StringDiffComparator extends StringDiffComparator {
  override def equiv(left: Column, right: Column): Column = DefaultDiffComparator.equiv(left, right)
}
