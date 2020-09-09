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

import uk.co.gresearch.spark.diff
import uk.co.gresearch.spark.diff.DiffMode.{Default, DiffMode}

object DiffMode extends Enumeration {
  type DiffMode = Value

  /**
   * The diff contains value columns from the left and right dataset, arranged column by column:
   * diff,( changes,) id-1, id-2, …, left-value-1, right-value-1, left-value-2, right-value-2, …
   */
  val ColumnByColumn: diff.DiffMode.Value = Value

  /**
   * The default diff mode is ColumnByColumn.
   */
  val Default: diff.DiffMode.Value = ColumnByColumn
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
 */
case class DiffOptions(diffColumn: String,
                       leftColumnPrefix: String,
                       rightColumnPrefix: String,
                       insertDiffValue: String,
                       changeDiffValue: String,
                       deleteDiffValue: String,
                       nochangeDiffValue: String,
                       changeColumn: Option[String] = None,
                       diffMode: DiffMode = Default) {

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

}

object DiffOptions {
  /**
   * Default diffing options.
   */
  val default: DiffOptions = DiffOptions("diff", "left", "right", "I", "C", "D", "N", None, Default)
}
