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
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.diff.comparator.{DefaultDiffComparator, EquivDiffComparator}

class DiffOptionsSuite extends AnyFunSuite with SparkTestSession {

  import spark.implicits._

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

  test("diff options with comparators") {
    case class Comparator(name: String) extends DiffComparator {
      override def equiv(left: Column, right: Column): Column = DefaultDiffComparator.equiv(left, right)
    }
    val cmp1 = Comparator("cmp1")
    val cmp2 = Comparator("cmp2")
    val cmp3 = Comparator("cmp3")
    val cmp4 = Comparator("cmp4")

    val options = DiffOptions.default
      .withDefaultComparator(cmp1)
      .withComparator(cmp2, IntegerType, LongType)
      .withComparator(cmp3, DoubleType)
      .withComparator(cmp4, "col1", "col2")

    assert(options.comparatorFor(StructField("col1", IntegerType)) === cmp4)
    assert(options.comparatorFor(StructField("col1", LongType)) === cmp4)
    assert(options.comparatorFor(StructField("col2", StringType)) === cmp4)
    assert(options.comparatorFor(StructField("col3", IntegerType)) === cmp2)
    assert(options.comparatorFor(StructField("col3", LongType)) === cmp2)
    assert(options.comparatorFor(StructField("col4", DoubleType)) === cmp3)
    assert(options.comparatorFor(StructField("col5", FloatType)) === cmp1)
  }

  Seq(
    (
      "single type",
      (options: DiffOptions) => options.withComparator(DiffComparator.default(), IntegerType),
      "A comparator for data type int exists already."
    ),
    (
      "multiple types",
      (options: DiffOptions) => options.withComparator(DiffComparator.default(), IntegerType, FloatType),
      "A comparator for data types float, int exists already."
    ),
    (
      "single column",
      (options: DiffOptions) => options.withComparator(DiffComparator.default(), "col1"),
      "A comparator for column name col1 exists already."
    ),
    (
      "multiple columns",
      (options: DiffOptions) => options.withComparator(DiffComparator.default(), "col2", "col1"),
      "A comparator for column names col1, col2 exists already."
    ),
  ).foreach { case (label, call, expected) =>
    test(s"diff options with duplicate comparator - $label") {
      val options = DiffOptions.default
        .withComparator(DiffComparator.default(), IntegerType, FloatType)
        .withComparator(DiffComparator.default(), "col1", "col2")
      val exception = intercept[IllegalArgumentException] { call(options) }
      assert(exception.getMessage === expected)
    }
  }

  test("diff options with typed diff comparator for other data type") {
    val exceptionSingle = intercept[IllegalArgumentException] {
      DiffOptions.default
        .withComparator(EquivDiffComparator((left: Int, right: Int) => left.abs == right.abs), LongType)
    }
    assert(exceptionSingle.getMessage.contains("Comparator with input type int cannot be used for data type bigint"))

    val exceptionMulti = intercept[IllegalArgumentException] {
      DiffOptions.default
        .withComparator(EquivDiffComparator((left: Int, right: Int) => left.abs == right.abs), LongType, FloatType)
    }
    assert(exceptionMulti.getMessage.contains("Comparator with input type int cannot be used for data type bigint, float"))
  }

  test("fluent methods of diff options") {
    assert(DiffMode.Default != DiffMode.LeftSide, "test assumption on default diff mode must hold, otherwise test is trivial")

    val cmp1 = new DiffComparator {
      override def equiv(left: Column, right: Column): Column = lit(true)
    }
    val cmp2 = new DiffComparator {
      override def equiv(left: Column, right: Column): Column = lit(true)
    }
    val cmp3 = new DiffComparator {
      override def equiv(left: Column, right: Column): Column = lit(true)
    }

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
      .withSparseMode(true)
      .withDefaultComparator(cmp1)
      .withComparator(cmp2, IntegerType)
      .withComparator(cmp3, "col1")

    val dexpectedDefCmp = cmp1
    val expectedDtCmps = Map(IntegerType.asInstanceOf[DataType] -> cmp2)
    val expectedColCmps = Map("col1" -> cmp3)
    val expected = DiffOptions("d", "l", "r", "i", "c", "d", "n", Some("change"), DiffMode.LeftSide, sparseMode = true, dexpectedDefCmp, expectedDtCmps, expectedColCmps)
    assert(options === expected)
  }

  def doTestRequirement(f: => Any, expected: String): Unit = {
    assert(intercept[IllegalArgumentException](f).getMessage === s"requirement failed: $expected")
  }

}
