/*
 * Copyright 2020 G-Research
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

package uk.co.gresearch.spark.diff

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{BinaryComparison, BinaryExpression, Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, DataType, NullType, BooleanType}
import uk.co.gresearch.spark.diff
import uk.co.gresearch.spark.diff.DiffMode.{Default, DiffMode}

/**
 * The diff mode determines the output columns of the diffing transformation.
 */
object DiffMode extends Enumeration {
  type DiffMode = Value

  /**
   * The diff mode determines the output columns of the diffing transformation.
   *
   * - ColumnByColumn: The diff contains value columns from the left and right dataset,
   *                   arranged column by column:
   *   diff,( changes,) id-1, id-2, …, left-value-1, right-value-1, left-value-2, right-value-2, …
   *
   * - SideBySide: The diff contains value columns from the left and right dataset,
   *               arranged side by side:
   *   diff,( changes,) id-1, id-2, …, left-value-1, left-value-2, …, right-value-1, right-value-2, …
   * - LeftSide / RightSide: The diff contains value columns from the left / right dataset only.
   */
  val ColumnByColumn, SideBySide, LeftSide, RightSide = Value

  /**
   * The diff mode determines the output columns of the diffing transformation.
   * The default diff mode is ColumnByColumn.
   *
   * Default is not a enum value here (hence the def) so that we do not have to include it in every
   * match clause. We will see the respective enum value that Default points to instead.
   */
  def Default: diff.DiffMode.Value = ColumnByColumn

  // we want to return Default's enum value for 'Default' here but cannot override super.withName.
  def withNameOption(name: String): Option[Value] = {
    if ("Default".equals(name)) {
      Some(DiffMode.Default)
    } else {
      try {
        Some(super.withName(name))
      } catch {
        case _: NoSuchElementException => None
      }
    }
  }

}

/**
 * Configuration class for diffing Datasets.
 *
 * @param diffColumn name of the diff column
 * @param leftColumnPrefix prefix of columns from the left Dataset
 * @param rightColumnPrefix prefix of columns from the right Dataset
 * @param insertDiffValue value in diff column for inserted rows
 * @param changeDiffValue value in diff column for changed rows
 * @param deleteDiffValue value in diff column for deleted rows
 * @param nochangeDiffValue value in diff column for un-changed rows
 * @param changeColumn name of change column
 * @param diffMode diff output format
 * @param sparseMode un-changed values are null on both sides
 */
case class DiffOptions(diffColumn: String,
                       leftColumnPrefix: String,
                       rightColumnPrefix: String,
                       insertDiffValue: String,
                       changeDiffValue: String,
                       deleteDiffValue: String,
                       nochangeDiffValue: String,
                       changeColumn: Option[String] = None,
                       diffMode: DiffMode = Default,
                       sparseMode: Boolean = false,
                       comparator: (Column, Column) => Column = _ <=> _) {
  require(leftColumnPrefix.nonEmpty, "Left column prefix must not be empty")
  require(rightColumnPrefix.nonEmpty, "Right column prefix must not be empty")
  require(handleConfiguredCaseSensitivity(leftColumnPrefix) != handleConfiguredCaseSensitivity(rightColumnPrefix),
    s"Left and right column prefix must be distinct: $leftColumnPrefix")

  // apply comparator on two values to assert the result column expression's type
  require(comparator(lit(1), lit(2)).expr.isInstanceOf[BinaryExpression],
    "Comparator has to be a binary expression / boolean condition")

  val diffValues = Seq(insertDiffValue, changeDiffValue, deleteDiffValue, nochangeDiffValue)
  require(diffValues.distinct.length == diffValues.length,
    s"Diff values must be distinct: $diffValues")

  require(!changeColumn.map(handleConfiguredCaseSensitivity).contains(handleConfiguredCaseSensitivity(diffColumn)),
    s"Change column name must be different to diff column: $diffColumn")

  def this(diffColumn: String,
           leftColumnPrefix: String,
           rightColumnPrefix: String,
           insertDiffValue: String,
           changeDiffValue: String,
           deleteDiffValue: String,
           nochangeDiffValue: String) =
    this(
      diffColumn, leftColumnPrefix, rightColumnPrefix,
      insertDiffValue, changeDiffValue, deleteDiffValue, nochangeDiffValue,
      changeColumn = None  // this argument is used to call into case class constructor, not this apply method
    )

  /**
   * Fluent method to change the diff column name.
   * Returns a new immutable DiffOptions instance with the new diff column name.
   * @param diffColumn new diff column name
   * @return new immutable DiffOptions instance
   */
  def withDiffColumn(diffColumn: String): DiffOptions =
    this.copy(diffColumn = diffColumn)

  /**
   * Fluent method to change the prefix of columns from the left Dataset.
   * Returns a new immutable DiffOptions instance with the new column prefix.
   * @param leftColumnPrefix new column prefix
   * @return new immutable DiffOptions instance
   */
  def withLeftColumnPrefix(leftColumnPrefix: String): DiffOptions =
    this.copy(leftColumnPrefix = leftColumnPrefix)

  /**
   * Fluent method to change the prefix of columns from the right Dataset.
   * Returns a new immutable DiffOptions instance with the new column prefix.
   * @param rightColumnPrefix new column prefix
   * @return new immutable DiffOptions instance
   */
  def withRightColumnPrefix(rightColumnPrefix: String): DiffOptions =
    this.copy(rightColumnPrefix = rightColumnPrefix)

  /**
   * Fluent method to change the value of inserted rows in the diff column.
   * Returns a new immutable DiffOptions instance with the new diff value.
   * @param insertDiffValue new diff value
   * @return new immutable DiffOptions instance
   */
  def withInsertDiffValue(insertDiffValue: String): DiffOptions =
    this.copy(insertDiffValue = insertDiffValue)

  /**
   * Fluent method to change the value of changed rows in the diff column.
   * Returns a new immutable DiffOptions instance with the new diff value.
   * @param changeDiffValue new diff value
   * @return new immutable DiffOptions instance
   */
  def withChangeDiffValue(changeDiffValue: String): DiffOptions =
    this.copy(changeDiffValue = changeDiffValue)

  /**
   * Fluent method to change the value of deleted rows in the diff column.
   * Returns a new immutable DiffOptions instance with the new diff value.
   * @param deleteDiffValue new diff value
   * @return new immutable DiffOptions instance
   */
  def withDeleteDiffValue(deleteDiffValue: String): DiffOptions =
    this.copy(deleteDiffValue = deleteDiffValue)

  /**
   * Fluent method to change the value of un-changed rows in the diff column.
   * Returns a new immutable DiffOptions instance with the new diff value.
   * @param nochangeDiffValue new diff value
   * @return new immutable DiffOptions instance
   */
  def withNochangeDiffValue(nochangeDiffValue: String): DiffOptions =
    this.copy(nochangeDiffValue = nochangeDiffValue)

  /**
   * Fluent method to change the change column name.
   * Returns a new immutable DiffOptions instance with the new change column name.
   * @param changeColumn new change column name
   * @return new immutable DiffOptions instance
   */
  def withChangeColumn(changeColumn: String): DiffOptions =
    this.copy(changeColumn = Some(changeColumn))

  /**
   * Fluent method to remove change column.
   * Returns a new immutable DiffOptions instance without a change column.
   * @return new immutable DiffOptions instance
   */
  def withoutChangeColumn(): DiffOptions =
    this.copy(changeColumn = None)

  /**
   * Fluent method to change the diff mode.
   * Returns a new immutable DiffOptions instance with the new diff mode.
   * @return new immutable DiffOptions instance
   */
  def withDiffMode(diffMode: DiffMode): DiffOptions =
    this.copy(diffMode = diffMode)

  /**
   * Fluent method to change the sparse mode.
   * Returns a new immutable DiffOptions instance with the new sparse mode.
   * @return new immutable DiffOptions instance
   */
  def withSparseMode(sparseMode: Boolean): DiffOptions =
    this.copy(sparseMode = sparseMode)

  /**
   * Fluent method to change the comparator for the diff operation.
   * Returns a new immutable DiffOptions instance with the new comparator.
   * @param comparator new comparator
   * @return new immutable DiffOptions instance
   */
  def withComparator(comparator: (Column, Column) => Column): DiffOptions =
    this.copy(comparator = comparator)
}

object DiffOptions {
  /**
   * Default diffing options.
   */
  val default: DiffOptions = DiffOptions("diff", "left", "right", "I", "C", "D", "N", None, Default, false)
}

case class Comparators(func: Column => Option[Ordering[Any]]) {
  def compare(left: Column, right: Column): Column = {
    val customOrdering: Option[Ordering[Any]] = func(left)
    val comparator = Comparator(left.expr, right.expr, customOrdering)
    new Column(comparator.asInstanceOf[Expression])
  }
}

case class Comparator(left: Expression, right: Expression, customOrdering: Option[Ordering[Any]]) extends BinaryExpression {
  // override def symbol: String = "<" + customOrdering.map(_.toString).getOrElse("=") + ">"

  override def dataType: DataType = BooleanType

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    val input2 = right.eval(input)
    if (input1 == null && input2 == null) {
      true
    } else if (input1 == null || input2 == null) {
      false
    } else if (customOrdering.isDefined) {
      customOrdering.get.equiv(input1, input2)
    } else {
      TypeUtils.getInterpretedOrdering(left.dataType).equiv(input1, input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val orderingObj = customOrdering.getOrElse(TypeUtils.getInterpretedOrdering(left.dataType))
    val ordering = ctx.addReferenceObj("ordering", orderingObj)
    ev.copy(code = eval1.code + eval2.code + code"""
        boolean ${ev.value} = (${eval1.isNull} && ${eval2.isNull}) ||
           (!${eval1.isNull} && !${eval2.isNull} && $ordering.equiv(${eval1.value}, ${eval2.value}));""", isNull = FalseLiteral)
  }
}
