/*
 * Copyright 2022 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, BinaryOperator, Expression}
import org.apache.spark.sql.extension.{ColumnExtension, ExpressionExtension}
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.sql.{Column, Encoder}

trait EquivDiffComparator[T] extends DiffComparator {
  val equiv: math.Equiv[T]
}

private trait ExpressionEquivDiffComparator[T] extends EquivDiffComparator[T] {
  def equiv(left: Expression, right: Expression): EquivExpression[T]
  def equiv(left: Column, right: Column): Column = equiv(left.expr, right.expr).column
}

trait TypedEquivDiffComparator[T] extends EquivDiffComparator[T] with TypedDiffComparator

private[comparator] trait TypedEquivDiffComparatorWithInput[T]
    extends ExpressionEquivDiffComparator[T]
    with TypedEquivDiffComparator[T] {
  def equiv(left: Expression, right: Expression): Equiv[T] = Equiv(left, right, equiv, inputType)
}

private[comparator] case class InputTypedEquivDiffComparator[T](equiv: math.Equiv[T], inputType: DataType)
    extends TypedEquivDiffComparatorWithInput[T]

object EquivDiffComparator {
  def apply[T: Encoder](equiv: math.Equiv[T]): TypedEquivDiffComparator[T] = EncoderEquivDiffComparator(equiv)
  def apply[T](equiv: math.Equiv[T], inputType: DataType): TypedEquivDiffComparator[T] =
    InputTypedEquivDiffComparator(equiv, inputType)
  def apply(equiv: math.Equiv[Any]): EquivDiffComparator[Any] = EquivAnyDiffComparator(equiv)

  private case class EncoderEquivDiffComparator[T: Encoder](equiv: math.Equiv[T])
      extends ExpressionEquivDiffComparator[T]
      with TypedEquivDiffComparator[T] {
    override def inputType: DataType = encoderFor[T].schema.fields(0).dataType
    def equiv(left: Expression, right: Expression): Equiv[T] = Equiv(left, right, equiv, inputType)
  }

  private case class EquivAnyDiffComparator(equiv: math.Equiv[Any]) extends ExpressionEquivDiffComparator[Any] {
    def equiv(left: Expression, right: Expression): EquivExpression[Any] = EquivAny(left, right, equiv)
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
    ev.copy(
      code = eval1.code + eval2.code + code"""
        boolean ${ev.value} = (${eval1.isNull} && ${eval2.isNull}) ||
           (!${eval1.isNull} && !${eval2.isNull} && $equivRef.equiv(${eval1.value}, ${eval2.value}));""",
      isNull = FalseLiteral
    )
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
    copy(left = newLeft, right = newRight)
}

private case class EquivAny(left: Expression, right: Expression, equiv: math.Equiv[Any]) extends EquivExpression[Any] {

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): EquivAny =
    copy(left = newLeft, right = newRight)
}
