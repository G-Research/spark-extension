package uk.co.gresearch.spark.diff

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

case class EquivTypedDiffComparator[T](equiv: math.Equiv[T], inputType: DataType) extends DiffComparator {
  def compare(left: Column, right: Column): Column = {
    val comparator = Equiv(left.expr, right.expr, equiv, inputType)
    new Column(comparator.asInstanceOf[Expression])
  }
}
