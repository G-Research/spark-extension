package uk.co.gresearch.spark.diff

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

case class EquivAnyDiffComparator(equiv: math.Equiv[Any]) extends DiffComparator {
  def compare(left: Column, right: Column): Column = {
    val comparator = EquivAny(left.expr, right.expr, equiv)
    new Column(comparator.asInstanceOf[Expression])
  }
}

private case class EquivAny(left: Expression, right: Expression, equiv: math.Equiv[Any])
  extends EquivExpression[Any] {

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): EquivAny =
    copy(left = newLeft, right = newRight)
}
