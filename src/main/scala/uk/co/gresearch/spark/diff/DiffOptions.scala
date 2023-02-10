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

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types.{DataType, StructField}
import uk.co.gresearch.spark.diff
import uk.co.gresearch.spark.diff.DiffMode.{Default, DiffMode}
import uk.co.gresearch.spark.diff.comparator.{DefaultDiffComparator, DiffComparator, EquivDiffComparator, TypedDiffComparator}

import scala.annotation.varargs
import scala.collection.Map

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
 * @param defaultComparator default custom comparator
 * @param dataTypeComparators custom comparator for some data type
 * @param columnNameComparators custom comparator for some column name
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
                       defaultComparator: DiffComparator = DefaultDiffComparator,
                       dataTypeComparators: Map[DataType, DiffComparator] = Map.empty,
                       columnNameComparators: Map[String, DiffComparator] = Map.empty) {
  // Constructor for Java to construct default options
  def this() = this("diff", "left", "right", "I", "C", "D", "N")
  def this(diffColumn: String,
           leftColumnPrefix: String,
           rightColumnPrefix: String,
           insertDiffValue: String,
           changeDiffValue: String,
           deleteDiffValue: String,
           nochangeDiffValue: String,
           changeColumn: Option[String],
           diffMode: DiffMode,
           sparseMode: Boolean) = {
    this(
      diffColumn,
      leftColumnPrefix,
      rightColumnPrefix,
      insertDiffValue,
      changeDiffValue,
      deleteDiffValue,
      nochangeDiffValue,
      changeColumn,
      diffMode,
      sparseMode,
      DefaultDiffComparator,
      Map.empty,
      Map.empty)
  }

  require(leftColumnPrefix.nonEmpty, "Left column prefix must not be empty")
  require(rightColumnPrefix.nonEmpty, "Right column prefix must not be empty")
  require(handleConfiguredCaseSensitivity(leftColumnPrefix) != handleConfiguredCaseSensitivity(rightColumnPrefix),
    s"Left and right column prefix must be distinct: $leftColumnPrefix")

  val diffValues = Seq(insertDiffValue, changeDiffValue, deleteDiffValue, nochangeDiffValue)
  require(diffValues.distinct.length == diffValues.length,
    s"Diff values must be distinct: $diffValues")

  require(!changeColumn.map(handleConfiguredCaseSensitivity).contains(handleConfiguredCaseSensitivity(diffColumn)),
    s"Change column name must be different to diff column: $diffColumn")

  /**
   * Fluent method to change the diff column name.
   * Returns a new immutable DiffOptions instance with the new diff column name.
   * @param diffColumn new diff column name
   * @return new immutable DiffOptions instance
   */
  def withDiffColumn(diffColumn: String): DiffOptions = {
    this.copy(diffColumn = diffColumn)
  }

  /**
   * Fluent method to change the prefix of columns from the left Dataset.
   * Returns a new immutable DiffOptions instance with the new column prefix.
   * @param leftColumnPrefix new column prefix
   * @return new immutable DiffOptions instance
   */
  def withLeftColumnPrefix(leftColumnPrefix: String): DiffOptions = {
    this.copy(leftColumnPrefix = leftColumnPrefix)
  }

  /**
   * Fluent method to change the prefix of columns from the right Dataset.
   * Returns a new immutable DiffOptions instance with the new column prefix.
   * @param rightColumnPrefix new column prefix
   * @return new immutable DiffOptions instance
   */
  def withRightColumnPrefix(rightColumnPrefix: String): DiffOptions = {
    this.copy(rightColumnPrefix = rightColumnPrefix)
  }

  /**
   * Fluent method to change the value of inserted rows in the diff column.
   * Returns a new immutable DiffOptions instance with the new diff value.
   * @param insertDiffValue new diff value
   * @return new immutable DiffOptions instance
   */
  def withInsertDiffValue(insertDiffValue: String): DiffOptions = {
    this.copy(insertDiffValue = insertDiffValue)
  }

  /**
   * Fluent method to change the value of changed rows in the diff column.
   * Returns a new immutable DiffOptions instance with the new diff value.
   * @param changeDiffValue new diff value
   * @return new immutable DiffOptions instance
   */
  def withChangeDiffValue(changeDiffValue: String): DiffOptions = {
    this.copy(changeDiffValue = changeDiffValue)
  }

  /**
   * Fluent method to change the value of deleted rows in the diff column.
   * Returns a new immutable DiffOptions instance with the new diff value.
   * @param deleteDiffValue new diff value
   * @return new immutable DiffOptions instance
   */
  def withDeleteDiffValue(deleteDiffValue: String): DiffOptions = {
    this.copy(deleteDiffValue = deleteDiffValue)
  }

  /**
   * Fluent method to change the value of un-changed rows in the diff column.
   * Returns a new immutable DiffOptions instance with the new diff value.
   * @param nochangeDiffValue new diff value
   * @return new immutable DiffOptions instance
   */
  def withNochangeDiffValue(nochangeDiffValue: String): DiffOptions = {
    this.copy(nochangeDiffValue = nochangeDiffValue)
  }

  /**
   * Fluent method to change the change column name.
   * Returns a new immutable DiffOptions instance with the new change column name.
   * @param changeColumn new change column name
   * @return new immutable DiffOptions instance
   */
  def withChangeColumn(changeColumn: String): DiffOptions = {
    this.copy(changeColumn = Some(changeColumn))
  }

  /**
   * Fluent method to remove change column.
   * Returns a new immutable DiffOptions instance without a change column.
   * @return new immutable DiffOptions instance
   */
  def withoutChangeColumn(): DiffOptions = {
    this.copy(changeColumn = None)
  }

  /**
   * Fluent method to change the diff mode.
   * Returns a new immutable DiffOptions instance with the new diff mode.
   * @return new immutable DiffOptions instance
   */
  def withDiffMode(diffMode: DiffMode): DiffOptions = {
    this.copy(diffMode = diffMode)
  }

  /**
   * Fluent method to change the sparse mode.
   * Returns a new immutable DiffOptions instance with the new sparse mode.
   * @return new immutable DiffOptions instance
   */
  def withSparseMode(sparseMode: Boolean): DiffOptions = {
    this.copy(sparseMode = sparseMode)
  }

  /**
   * Fluent method to add a default comparator.
   * Returns a new immutable DiffOptions instance with the new default comparator.
   * @return new immutable DiffOptions instance
   */
  def withDefaultComparator(diffComparator: DiffComparator): DiffOptions = {
    this.copy(defaultComparator = diffComparator)
  }

  /**
   * Fluent method to add a typed equivalent operator as a default comparator.
   * The encoder defines the input type of the comparator.
   * Returns a new immutable DiffOptions instance with the new default comparator.
   * @return new immutable DiffOptions instance
   */
  def withDefaultComparator[T : Encoder](equiv: math.Equiv[T]): DiffOptions = {
    withDefaultComparator(EquivDiffComparator(equiv))
  }

  /**
   * Fluent method to add a typed equivalent operator as a default comparator.
   * Returns a new immutable DiffOptions instance with the new default comparator.
   * @return new immutable DiffOptions instance
   */
  def withDefaultComparator[T](equiv: math.Equiv[T], inputDataType: DataType): DiffOptions = {
    withDefaultComparator(EquivDiffComparator(equiv, inputDataType))
  }

  /**
   * Fluent method to add an equivalent operator as a default comparator.
   * Returns a new immutable DiffOptions instance with the new default comparator.
   * @return new immutable DiffOptions instance
   */
  def withDefaultComparator(equiv: math.Equiv[Any]): DiffOptions = {
    withDefaultComparator(EquivDiffComparator(equiv))
  }

  /**
   * Fluent method to add a comparator for its input data type.
   * Returns a new immutable DiffOptions instance with the new comparator.
   * @return new immutable DiffOptions instance
   */
  def withComparator(diffComparator: TypedDiffComparator): DiffOptions = {
    if (dataTypeComparators.contains(diffComparator.inputType)) {
      throw new IllegalArgumentException(
        s"A comparator for data type ${diffComparator.inputType} exists already.")
    }
    this.copy(dataTypeComparators = dataTypeComparators ++ Map(diffComparator.inputType -> diffComparator))
  }

  /**
   * Fluent method to add a comparator for one or more data types.
   * Returns a new immutable DiffOptions instance with the new comparator.
   * @return new immutable DiffOptions instance
   */
  @varargs
  def withComparator(diffComparator: DiffComparator, dataType: DataType, dataTypes: DataType*): DiffOptions = {
    val allDataTypes = dataType +: dataTypes

    diffComparator match {
      case typed: TypedDiffComparator if allDataTypes.exists(_ != typed.inputType) =>
        throw new IllegalArgumentException(s"Comparator with input type ${typed.inputType.simpleString} " +
          s"cannot be used for data type ${allDataTypes.filter(_ != typed.inputType).map(_.simpleString).sorted.mkString(", ")}")
      case _ =>
    }

    val existingDataTypes = allDataTypes.filter(dataTypeComparators.contains)
    if (existingDataTypes.nonEmpty) {
      throw new IllegalArgumentException(
        s"A comparator for data type${if (existingDataTypes.length > 1) "s" else ""} " +
          s"${existingDataTypes.map(_.simpleString).sorted.mkString(", ")} exists already.")
    }
    this.copy(dataTypeComparators = dataTypeComparators ++ allDataTypes.map(dt => dt -> diffComparator))
  }

  /**
   * Fluent method to add a comparator for one or more column names.
   * Returns a new immutable DiffOptions instance with the new comparator.
   * @return new immutable DiffOptions instance
   */
  @varargs
  def withComparator(diffComparator: DiffComparator, columnName: String, columnNames: String*): DiffOptions = {
    val allColumnNames = columnName +: columnNames
    val existingColumnNames = allColumnNames.filter(columnNameComparators.contains)
    if (existingColumnNames.nonEmpty) {
      throw new IllegalArgumentException(
        s"A comparator for column name${if (existingColumnNames.length > 1) "s" else ""} " +
          s"${existingColumnNames.sorted.mkString(", ")} exists already.")
    }
    this.copy(columnNameComparators = columnNameComparators ++ allColumnNames.map(name => name -> diffComparator))
  }

  /**
   * Fluent method to add a typed equivalent operator as a comparator for its input data type.
   * The encoder defines the input type of the comparator.
   * Returns a new immutable DiffOptions instance with the new comparator.
   * @return new immutable DiffOptions instance
   */
  def withComparator[T : Encoder](equiv: math.Equiv[T]): DiffOptions =
    withComparator(EquivDiffComparator(equiv))

  /**
   * Fluent method to add a typed equivalent operator as a comparator for one or more column names.
   * The encoder defines the input type of the comparator.
   * Returns a new immutable DiffOptions instance with the new comparator.
   * @return new immutable DiffOptions instance
   */
  def withComparator[T : Encoder](equiv: math.Equiv[T], columnName: String, columnNames: String*): DiffOptions =
    withComparator(EquivDiffComparator(equiv), columnName, columnNames: _*)

  /**
   * Fluent method to add an equivalent operator as a comparator for one or more column names.
   * Returns a new immutable DiffOptions instance with the new comparator.
   * @note Java-specific method
   * @return new immutable DiffOptions instance
   */
  @varargs
  def withComparator[T](equiv: math.Equiv[T], encoder: Encoder[T], columnName: String, columnNames: String*): DiffOptions =
    withComparator(EquivDiffComparator(equiv)(encoder), columnName, columnNames: _*)

  /**
   * Fluent method to add an equivalent operator as a comparator for one or more data types.
   * Returns a new immutable DiffOptions instance with the new comparator.
   * @return new immutable DiffOptions instance
   */
  // There is probably no use case of calling this with multiple datatype while T not being Any
  // But this is the only way to define withComparator[T](equiv: math.Equiv[T], dataType: DataType)
  // without being ambiguous with withComparator(equiv: math.Equiv[Any], dataType: DataType, dataTypes: DataType*)
  @varargs
  def withComparator[T](equiv: math.Equiv[T], dataType: DataType, dataTypes: DataType*): DiffOptions =
    (dataType +: dataTypes).foldLeft(this)( (options, dataType) =>
      options.withComparator(EquivDiffComparator(equiv, dataType))
    )

  /**
   * Fluent method to add an equivalent operator as a comparator for one or more column names.
   * Returns a new immutable DiffOptions instance with the new comparator.
   * @return new immutable DiffOptions instance
   */
  @varargs
  def withComparator(equiv: math.Equiv[Any], columnName: String, columnNames: String*): DiffOptions =
    withComparator(EquivDiffComparator(equiv), columnName, columnNames: _*)

  private[diff] def comparatorFor(column: StructField): DiffComparator =
    columnNameComparators.get(column.name)
      .orElse(dataTypeComparators.get(column.dataType))
      .getOrElse(defaultComparator)
}

object DiffOptions {
  /**
   * Default diffing options.
   */
  val default: DiffOptions = new DiffOptions()
}
