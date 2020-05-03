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

import java.util.Locale

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField}

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
 * @param nullOutValidData if true, all cells which are equal (or in a given tolerance range) are replaced with null to increase visibility of erroneous data
 * @param ignoreColumns an optional list of columns which are dropped from both sides before comparing
 * @param datatypeComparators an optional map of datatypes with the applicable comparator, overwriting the default '<=>' comparator
 * @param columnComparators an optional map of column names with the applicable comparator, overwriting the datatype comparators
 */
case class DiffOptions(
  diffColumn: String,
  leftColumnPrefix: String,
  rightColumnPrefix: String,
  insertDiffValue: String,
  changeDiffValue: String,
  deleteDiffValue: String,
  nochangeDiffValue: String,
  nullOutValidData: Boolean = false,
  ignoreColumns: Seq[String] = Seq(),
  datatypeComparators: Map[DataType, DiffComparator] = Map(),
  columnComparators: Map[String, DiffComparator] = Map()
) {

  require(leftColumnPrefix.nonEmpty, "Left column prefix must not be empty")
  require(rightColumnPrefix.nonEmpty, "Right column prefix must not be empty")

  require(leftColumnPrefix != rightColumnPrefix, s"Left and right column prefix must be distinct: $leftColumnPrefix")

  /**
   * Returns the applicable comparator for the given column
   */
  def getDiffComparator(sf: StructField): DiffComparator = columnComparators.get(sf.name) match {
    case Some(comp) => comp
    case None => datatypeComparators.getOrElse(sf.dataType, DiffComparator.EqualNullSafeComparator)
  }

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

  /**
   * Fluent method to change the returned cell data, determined to be equal, to null.
   * Returns a new immutable DiffOptions instance with the new diff value.
   * @param nullOutValidData - by default this is false
   * @return new immutable DiffOptions instance
   */
  def withNullOutValidData(nullOutValidData: Boolean): DiffOptions = this.copy(nullOutValidData = nullOutValidData)

  /**
   * Fluent method to change the list of columns which shall be ignored from comparison (and thereby dropped from the result).
   * Returns a new immutable DiffOptions instance with the new diff value.
   * @param ignoreColumns - by default this is empty
   * @return new immutable DiffOptions instance
   */
  def withIgnoredColumns(ignoreColumns: Seq[String]): DiffOptions = this.copy(ignoreColumns = ignoreColumns)

  /**
   * Fluent method to change the map of [[DiffComparator]] to be used by default for all columns of a given Spark [[DataType]].
   * Thereby, the default comparator (<=>) is replaced by these comparators for all columns matching the specified datatype.
   * Returns a new immutable DiffOptions instance with the new diff value.
   * @param datatypeComparators - by default this is empty
   * @return new immutable DiffOptions instance
   */
  def withDatatypeComparators(datatypeComparators: Map[DataType, DiffComparator]): DiffOptions = this.copy(datatypeComparators = datatypeComparators)

  /**
   * Fluent method to change the map of [[DiffComparator]] to be used for the specified column (column names are the map keys).
   * Thereby, the default comparator (<=>), as well as possible datatype based comparators, are replaced by these comparators for all columns listed here.
   * Returns a new immutable DiffOptions instance with the new diff value.
   * @param columnComparators - by default this is empty
   * @return new immutable DiffOptions instance
   */
  def withColumnComparators(columnComparators: Map[String, DiffComparator]): DiffOptions = this.copy(columnComparators = columnComparators)
}

object DiffOptions {
  /**
   * Default diffing options.
   */
  val default: DiffOptions = DiffOptions("diff", "left", "right", "I", "C", "D", "N")

  // column names case-sensitivity can be configured
  private[diff] def columnName(columnName: String): String =
    if (SQLConf.get.caseSensitiveAnalysis) columnName else columnName.toLowerCase(Locale.ROOT)
}
