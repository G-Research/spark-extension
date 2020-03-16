/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.diff

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
 */
case class DiffOptions(diffColumn: String,
                       leftColumnPrefix: String,
                       rightColumnPrefix: String,
                       insertDiffValue: String,
                       changeDiffValue: String,
                       deleteDiffValue: String,
                       nochangeDiffValue: String) {

  require(leftColumnPrefix.nonEmpty, "Left column prefix must not be empty")
  require(rightColumnPrefix.nonEmpty, "Right column prefix must not be empty")
  require(leftColumnPrefix != rightColumnPrefix,
    s"Left and right column prefix must be distinct: $leftColumnPrefix")

  val diffValues = Seq(insertDiffValue, changeDiffValue, deleteDiffValue, nochangeDiffValue)
  require(diffValues.distinct.length == diffValues.length,
    s"Diff values must be distinct: $diffValues")

  /**
   * Fluent method to change the diff column name.
   * Returns a new immutable DiffOptions instance with the new column name.
   * @param diffColumn new column name
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
}

object DiffOptions {
  /**
   * Default diffing options.
   */
  val default: DiffOptions = DiffOptions("diff", "left", "right", "I", "C", "D", "N")
}
