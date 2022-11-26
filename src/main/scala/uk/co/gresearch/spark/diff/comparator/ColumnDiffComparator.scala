package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{Column, TypedColumn}
import uk.co.gresearch.spark.diff.DiffComparator

case class ColumnDiffComparator(comparator: (Column, Column) => Column) extends DiffComparator {
  def compare(left: Column, right: Column): Column = {
    val column = comparator(left, right)
    if (column.expr.dataType != BooleanType) {
      throw new IllegalArgumentException(s"Comparator functions must return a boolean Column, not ${column.expr.dataType}")
    }
    column
  }
}

case class ColumnTypedDiffComparator[T](comparator: (Column, Column) => TypedColumn[T, Boolean]) extends DiffComparator {
  def compare(left: Column, right: Column): Column = comparator(left, right)
}
