package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, BinaryOperator, Expression}
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.sql.{Column, Encoder}
import uk.co.gresearch.spark.diff.DiffComparator

case class EquivDiffComparator[T : Encoder](equiv: math.Equiv[T]) extends DiffComparator {
  def compare(left: Column, right: Column): Column = {
    val comparator = Equiv(left.expr, right.expr, equiv)
    new Column(comparator.asInstanceOf[Expression])
  }
}

private trait EquivExpression[T] extends BinaryExpression {
  val equiv: math.Equiv[T]

  override def nullable: Boolean = false

  override def dataType: DataType = BooleanType

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input).asInstanceOf[T]
    val input2 = right.eval(input).asInstanceOf[T]
    if (input1 == null && input2 == null) {
      true
    } else if (input1 == null || input2 == null) {
      false
    } else {
      equiv.equiv(input1, input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val equivRef = ctx.addReferenceObj("equiv", equiv, math.Equiv.getClass.getName.stripSuffix("$"))
    ev.copy(code = eval1.code + eval2.code + code"""
        boolean ${ev.value} = (${eval1.isNull} && ${eval2.isNull}) ||
           (!${eval1.isNull} && !${eval2.isNull} && $equivRef.equiv(${eval1.value}, ${eval2.value}));""", isNull = FalseLiteral)
  }
}

private trait EquivOperator[T] extends BinaryOperator with EquivExpression[T] {
  val equivInputType: DataType

  override def inputType: DataType = equivInputType

  override def symbol: String = "≡"
}

private case class Equiv[T](left: Expression, right: Expression, equiv: math.Equiv[T], equivInputType: DataType)
  extends EquivOperator[T] {
  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Equiv[T] =
    copy(left=newLeft, right=newRight)
}

private object Equiv {
  def apply[T : Encoder](left: Expression, right: Expression, equiv: math.Equiv[T]): Equiv[T] =
    Equiv(left, right, equiv, encoderFor[T].schema.fields(0).dataType)
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
