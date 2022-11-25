package uk.co.gresearch.spark.diff

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, BinaryOperator, ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.sql.{Column, Encoder}

case class OrderingDiffComparator[T : Encoder](ordering: Ordering[T]) extends DiffComparator {
  def compare(left: Column, right: Column): Column = {
    val comparator = Comparator(left.expr, right.expr, Some(ordering))
    new Column(comparator.asInstanceOf[Expression])
  }
}

private case class Comparator[T : Encoder](left: Expression, right: Expression, customOrdering: Option[Ordering[T]])
  extends BinaryOperator with ExpectsInputTypes with CodegenFallback {

  override def nullable: Boolean = false

  def inputType: DataType = encoderFor[T].schema.fields(0).dataType

  override def symbol: String = "≡"

  override def dataType: DataType = BooleanType

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input).asInstanceOf[T]
    val input2 = right.eval(input).asInstanceOf[T]
    if (input1 == null && input2 == null) {
      true
    } else if (input1 == null || input2 == null) {
      false
    } else if (customOrdering.isDefined) {
      customOrdering.get.equiv(input1, input2)
    } else {
      // TODO: use case?
      TypeUtils.getInterpretedOrdering(left.dataType).equiv(input1, input2)
    }
  }

  /**
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val orderingObj = customOrdering.getOrElse(TypeUtils.getInterpretedOrdering(left.dataType))
    val ordering = ctx.addReferenceObj("ordering", orderingObj)
    ev.copy(code = eval1.code + eval2.code + code"""
        boolean ${ev.value} = (${eval1.isNull} && ${eval2.isNull}) ||
           (!${eval1.isNull} && !${eval2.isNull} && $ordering.equiv(${eval1.value}, ${eval2.value}));""", isNull = FalseLiteral)
  }
  **/

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Comparator[T] =
    copy(left=newLeft, right=newRight)

}

case class DoubleEpsilonOrdering(epsilon: Double) extends Ordering[Double] {
  override def compare(x: Double, y: Double): Int = {
    val delta = math.min(math.abs(x), math.abs(y)) * epsilon
    if (math.abs(x - y) <= delta) {
      0
    } else if (x < y) {
      -1
    } else {
      1
    }
  }
}
