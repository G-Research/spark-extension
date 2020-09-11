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

import org.apache.spark.sql.internal.SQLConf
import org.scalatest.FunSuite
import uk.co.gresearch.spark.SparkTestSession

class DiffOptionsSuite extends FunSuite with SparkTestSession {

  test("diff options with empty diff column name") {
    // test the copy method (constructor), not the fluent methods
    val default = DiffOptions.default
    val options = default.copy(diffColumn = "")
    assert(options.diffColumn.isEmpty)
  }

  test("diff options left and right prefixes") {
    // test the copy method (constructor), not the fluent methods
    val default = DiffOptions.default
    doTestRequirement(default.copy(leftColumnPrefix = ""),
      "Left column prefix must not be empty")
    doTestRequirement(default.copy(rightColumnPrefix = ""),
      "Right column prefix must not be empty")

    val prefix = "prefix"
    doTestRequirement(default.copy(leftColumnPrefix = prefix, rightColumnPrefix = prefix),
      s"Left and right column prefix must be distinct: $prefix")
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      doTestRequirement(default.copy(leftColumnPrefix = prefix.toLowerCase, rightColumnPrefix = prefix.toUpperCase),
        s"Left and right column prefix must be distinct: $prefix")
    }
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      default.copy(leftColumnPrefix = prefix.toLowerCase, rightColumnPrefix = prefix.toUpperCase)
    }
  }

  test("diff options diff value") {
    // test the copy method (constructor), not the fluent methods
    val default = DiffOptions.default

    val emptyInsertDiffValueOpts = default.copy(insertDiffValue = "")
    assert(emptyInsertDiffValueOpts.insertDiffValue.isEmpty)
    val emptyChangeDiffValueOpts = default.copy(changeDiffValue = "")
    assert(emptyChangeDiffValueOpts.changeDiffValue.isEmpty)
    val emptyDeleteDiffValueOpts = default.copy(deleteDiffValue = "")
    assert(emptyDeleteDiffValueOpts.deleteDiffValue.isEmpty)
    val emptyNochangeDiffValueOpts = default.copy(nochangeDiffValue = "")
    assert(emptyNochangeDiffValueOpts.nochangeDiffValue.isEmpty)

    Seq("value", "").foreach { value =>
      doTestRequirement(default.copy(insertDiffValue = value, changeDiffValue = value),
        s"Diff values must be distinct: List($value, $value, D, N)")
      doTestRequirement(default.copy(insertDiffValue = value, deleteDiffValue = value),
        s"Diff values must be distinct: List($value, C, $value, N)")
      doTestRequirement(default.copy(insertDiffValue = value, nochangeDiffValue = value),
        s"Diff values must be distinct: List($value, C, D, $value)")
      doTestRequirement(default.copy(changeDiffValue = value, deleteDiffValue = value),
        s"Diff values must be distinct: List(I, $value, $value, N)")
      doTestRequirement(default.copy(changeDiffValue = value, nochangeDiffValue = value),
        s"Diff values must be distinct: List(I, $value, D, $value)")
      doTestRequirement(default.copy(deleteDiffValue = value, nochangeDiffValue = value),
        s"Diff values must be distinct: List(I, C, $value, $value)")
    }
  }

  test("diff options with change column name same as diff column") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      doTestRequirement(
        DiffOptions.default.withDiffColumn("same").withChangeColumn("same"),
        "Change column name must be different to diff column: same"
      )
      doTestRequirement(
        DiffOptions.default.withChangeColumn("same").withDiffColumn("same"),
        "Change column name must be different to diff column: same"
      )

      doTestRequirement(
        DiffOptions.default.withDiffColumn("SAME").withChangeColumn("same"),
        "Change column name must be different to diff column: SAME"
      )
      doTestRequirement(
        DiffOptions.default.withChangeColumn("SAME").withDiffColumn("same"),
        "Change column name must be different to diff column: same"
      )
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      DiffOptions.default.withDiffColumn("SAME").withChangeColumn("same")
      DiffOptions.default.withChangeColumn("SAME").withDiffColumn("same")
    }
  }

  test("fluent methods of diff options") {
    assert(DiffMode.Default != DiffMode.LeftSide, "test assumption on default diff mode must hold, otherwise test is trivial")

    val options = DiffOptions.default
      .withDiffColumn("d")
      .withLeftColumnPrefix("l")
      .withRightColumnPrefix("r")
      .withInsertDiffValue("i")
      .withChangeDiffValue("c")
      .withDeleteDiffValue("d")
      .withNochangeDiffValue("n")
      .withChangeColumn("change")
      .withDiffMode(DiffMode.LeftSide)

    val expected = DiffOptions("d", "l", "r", "i", "c", "d", "n", Some("change"), DiffMode.LeftSide)
    assert(options === expected)
  }

  def doTestRequirement(f: => Any, expected: String): Unit = {
    assert(intercept[IllegalArgumentException](f).getMessage === s"requirement failed: $expected")
  }

}
