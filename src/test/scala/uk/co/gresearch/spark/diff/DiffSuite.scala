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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.spark.SparkTestSession

case class Empty()
case class Value(id: Int, value: Option[String])
case class Value2(id: Int, seq: Option[Int], value: Option[String])
case class Value3(id: Int, left_value: String, right_value: String, value: String)
case class Value4(id: Int, diff: String)
case class Value5(first_id: Int, id: String)
case class Value6(id: Int, label: String)
case class Value7(id: Int, value: Option[String], label: Option[String])
case class Value8(id: Int, seq: Option[Int], value: Option[String], meta: Option[String])

case class DiffAs(diff: String,
                  id: Int,
                  left_value: Option[String],
                  right_value: Option[String])
case class DiffAs8(diff: String,
                   id: Int,
                   seq: Option[Int],
                   left_value: Option[String],
                   right_value: Option[String],
                   left_meta: Option[String],
                   right_meta: Option[String])

case class DiffAsCustom(action: String,
                        id: Int,
                        before_value: Option[String],
                        after_value: Option[String])
case class DiffAsSubset(diff: String,
                        id: Int,
                        left_value: Option[String])
case class DiffAsExtra(diff: String,
                       id: Int,
                       left_value: Option[String],
                       right_value: Option[String],
                       extra: String)
case class DiffAsOneSide(diff: String,
                         id: Int,
                         value: Option[String])

class DiffSuite extends AnyFunSuite with SparkTestSession {

  import spark.implicits._

  lazy val left: Dataset[Value] = Seq(
    Value(1, Some("one")),
    Value(2, Some("two")),
    Value(3, Some("three"))
  ).toDS()

  lazy val right: Dataset[Value] = Seq(
    Value(1, Some("one")),
    Value(2, Some("Two")),
    Value(4, Some("four"))
  ).toDS()

  lazy val left7: Dataset[Value7] = Seq(
    Value7(1, Some("one"), Some("one label")),
    Value7(2, Some("two"), Some("two labels")),
    Value7(3, Some("three"), Some("three labels")),
    Value7(4, Some("four"), Some("four labels")),
    Value7(5, None, None),
    Value7(6, Some("six"), Some("six labels")),
    Value7(7, Some("seven"), Some("seven labels")),
    Value7(9, None, None)
  ).toDS()

  lazy val right7: Dataset[Value7] = Seq(
    Value7(1, Some("One"), Some("one label")),
    Value7(2, Some("two"), Some("two Labels")),
    Value7(3, Some("Three"), Some("Three Labels")),
    Value7(4, None, None),
    Value7(5, Some("five"), Some("five labels")),
    Value7(6, Some("six"), Some("six labels")),
    Value7(8, Some("eight"), Some("eight labels")),
    Value7(10, None, None)
  ).toDS()

  lazy val left8: Dataset[Value8] = Seq(
    Value8(1, Some(1), Some("one"), Some("user1")),
    Value8(1, Some(2), Some("one"), None),
    Value8(1, Some(3), Some("one"), Some("user1")),
    Value8(2, None, Some("two"), Some("user2")),
    Value8(2, Some(1), Some("two"), None),
    Value8(2, Some(2), Some("two"), None),
    Value8(3, None, None, None)
  ).toDS()

  lazy val right8: Dataset[Value8] = Seq(
    Value8(1, Some(1), Some("one"), Some("user2")),
    Value8(1, Some(2), Some("one"), Some("user2")),
    Value8(1, Some(3), Some("one"), None),
    Value8(2, None, Some("two"), Some("user2")),
    Value8(2, Some(2), Some("Two"), Some("user1")),
    Value8(2, Some(3), Some("two"), Some("user2")),
    Value8(3, None, None, None)
  ).toDS()

  lazy val expectedDiffColumns: Seq[String] = Seq("diff", "id", "left_value", "right_value")

  lazy val expectedDiff: Seq[Row] = Seq(
    Row("N", 1, "one", "one"),
    Row("C", 2, "two", "Two"),
    Row("D", 3, "three", null),
    Row("I", 4, null, "four")
  )

  lazy val expectedReverseDiff: Seq[Row] = Seq(
    Row("N", 1, "one", "one"),
    Row("C", 2, "Two", "two"),
    Row("I", 3, null, "three"),
    Row("D", 4, "four", null)
  )

  lazy val expectedDiffAs: Seq[DiffAs] = expectedDiff.map(r =>
    DiffAs(r.getString(0), r.getInt(1), Option(r.getString(2)), Option(r.getString(3)))
  )

  lazy val expectedDiff7: Seq[Row] = Seq(
    Row("C", 1, "one", "One", "one label", "one label"),
    Row("C", 2, "two", "two", "two labels", "two Labels"),
    Row("C", 3, "three", "Three", "three labels", "Three Labels"),
    Row("C", 4, "four", null, "four labels", null),
    Row("C", 5, null, "five", null, "five labels"),
    Row("N", 6, "six", "six", "six labels", "six labels"),
    Row("D", 7, "seven", null, "seven labels", null),
    Row("I", 8, null, "eight", null, "eight labels"),
    Row("D", 9, null, null, null, null),
    Row("I", 10, null, null, null, null)
  )

  lazy val expectedSideBySideDiff7: Seq[Row] = expectedDiff7.map(row => Row(row.getString(0), row.getInt(1), row.getString(2), row.getString(4), row.getString(3), row.getString(5)))
  lazy val expectedLeftSideDiff7: Seq[Row] = expectedDiff7.map(row => Row(row.getString(0), row.getInt(1), row.getString(2), row.getString(4)))
  lazy val expectedRightSideDiff7: Seq[Row] = expectedDiff7.map(row => Row(row.getString(0), row.getInt(1), row.getString(3), row.getString(5)))

  lazy val expectedSparseDiff7: Seq[Row] = Seq(
    Row("C", 1, "one", "One", null, null),
    Row("C", 2, null, null, "two labels", "two Labels"),
    Row("C", 3, "three", "Three", "three labels", "Three Labels"),
    Row("C", 4, "four", null, "four labels", null),
    Row("C", 5, null, "five", null, "five labels"),
    Row("N", 6, null, null, null, null),
    Row("D", 7, "seven", null, "seven labels", null),
    Row("I", 8, null, "eight", null, "eight labels"),
    Row("D", 9, null, null, null, null),
    Row("I", 10, null, null, null, null)
  )

  lazy val expectedSideBySideSparseDiff7: Seq[Row] = expectedSparseDiff7.map(row => Row(row.getString(0), row.getInt(1), row.getString(2), row.getString(4), row.getString(3), row.getString(5)))
  lazy val expectedLeftSideSparseDiff7: Seq[Row] = expectedSparseDiff7.map(row => Row(row.getString(0), row.getInt(1), row.getString(2), row.getString(4)))
  lazy val expectedRightSideSparseDiff7: Seq[Row] = expectedSparseDiff7.map(row => Row(row.getString(0), row.getInt(1), row.getString(3), row.getString(5)))

  lazy val expectedDiff7WithChanges: Seq[Row] = Seq(
    Row("C", Seq("value"), 1, "one", "One", "one label", "one label"),
    Row("C", Seq("label"), 2, "two", "two", "two labels", "two Labels"),
    Row("C", Seq("value", "label"), 3, "three", "Three", "three labels", "Three Labels"),
    Row("C", Seq("value", "label"), 4, "four", null, "four labels", null),
    Row("C", Seq("value", "label"), 5, null, "five", null, "five labels"),
    Row("N", Seq.empty[String], 6, "six", "six", "six labels", "six labels"),
    Row("D", null, 7, "seven", null, "seven labels", null),
    Row("I", null, 8, null, "eight", null, "eight labels"),
    Row("D", null, 9, null, null, null, null),
    Row("I", null, 10, null, null, null, null)
  )

  lazy val expectedDiff8: Seq[Row] = Seq(
    Row("N", 1, 1, "one", "one", "user1", "user2"),
    Row("N", 1, 2, "one", "one", null, "user2"),
    Row("N", 1, 3, "one", "one", "user1", null),
    Row("N", 2, null, "two", "two", "user2", "user2"),
    Row("D", 2, 1, "two", null, null, null),
    Row("C", 2, 2, "two", "Two", null, "user1"),
    Row("I", 2, 3, null, "two", null, "user2"),
    Row("N", 3, null, null, null, null, null)
  )

  lazy val expectedSideBySideDiff8: Seq[Row] = expectedDiff8.map(r => Row(r.get(0), r.get(1), r.get(2), r.get(3), r.get(5), r.get(4), r.get(6)))
  lazy val expectedLeftSideDiff8: Seq[Row] = expectedDiff8.map(r => Row(r.get(0), r.get(1), r.get(2), r.get(3), r.get(5)))
  lazy val expectedRightSideDiff8: Seq[Row] = expectedDiff8.map(r => Row(r.get(0), r.get(1), r.get(2), r.get(4), r.get(6)))

  lazy val expectedSparseDiff8: Seq[Row] = Seq(
    Row("N", 1, 1, null, null, "user1", "user2"),
    Row("N", 1, 2, null, null, null, "user2"),
    Row("N", 1, 3, null, null, "user1", null),
    Row("N", 2, null, null, null, null, null),
    Row("D", 2, 1, "two", null, null, null),
    Row("C", 2, 2, "two", "Two", null, "user1"),
    Row("I", 2, 3, null, "two", null, "user2"),
    Row("N", 3, null, null, null, null, null)
  )

  lazy val expectedSideBySideSparseDiff8: Seq[Row] = expectedSparseDiff8.map(r => Row(r.get(0), r.get(1), r.get(2), r.get(3), r.get(5), r.get(4), r.get(6)))
  lazy val expectedLeftSideSparseDiff8: Seq[Row] = expectedSparseDiff8.map(r => Row(r.get(0), r.get(1), r.get(2), r.get(3), r.get(5)))
  lazy val expectedRightSideSparseDiff8: Seq[Row] = expectedSparseDiff8.map(r => Row(r.get(0), r.get(1), r.get(2), r.get(4), r.get(6)))

  lazy val expectedDiffAs8: Seq[DiffAs8] = expectedDiff8.map(r =>
    DiffAs8(r.getString(0),
      r.getInt(1), Some(r).filterNot(_.isNullAt(2)).map(_.getInt(2)),
      Option(r.getString(3)), Option(r.getString(4)),
      Option(r.getString(5)), Option(r.getString(6))
    )
  )

  lazy val expectedDiff8WithChanges: Seq[Row] = expectedDiff8.map(r =>
    Row(r.get(0),
      r.get(0) match {
        case "N" => Seq.empty
        case "I" => null
        case "C" => Seq("value")
        case "D" => null
      },
      r.get(1), r.get(2),
      r.getString(3), r.getString(4),
      r.getString(5), r.getString(6)
    )
  )

  test("distinct string for") {
    assert(Diff.distinctStringNameFor(Seq.empty[String]) === "_")
    assert(Diff.distinctStringNameFor(Seq("a")) === "__")
    assert(Diff.distinctStringNameFor(Seq("abc")) === "____")
    assert(Diff.distinctStringNameFor(Seq("a", "bc", "def")) === "____")
  }

  test("diff dataframe with duplicate columns") {
    val df = Seq((1)).toDF("id").select($"id", $"id")

    doTestRequirement(df.diff(df, "id"),
      "The datasets have duplicate columns.\n" +
        "Left column names: id, id\nRight column names: id, id")
  }

  test("diff with no id column") {
    val expected = Seq(
      Row("N", 1, "one"),
      Row("D", 2, "two"),
      Row("I", 2, "Two"),
      Row("D", 3, "three"),
      Row("I", 4, "four")
    )

    val actual = left.diff(right).orderBy("id", "diff")

    assert(actual.columns === Seq("diff", "id", "value"))
    assert(actual.collect() === expected)
  }

  test("diff with one id column") {
    val actual = left.diff(right, "id").orderBy("id")
    val reverse = right.diff(left, "id").orderBy("id")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff)
    assert(reverse.columns === expectedDiffColumns)
    assert(reverse.collect() === expectedReverseDiff)
  }

  test("diff with one ID column case-insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val actual = left.diff(right, "ID").orderBy("ID")
      val reverse = right.diff(left, "ID").orderBy("ID")

      assert(actual.columns === Seq("diff", "ID", "left_value", "right_value"))
      assert(actual.collect() === expectedDiff)
      assert(reverse.columns === Seq("diff", "ID", "left_value", "right_value"))
      assert(reverse.collect() === expectedReverseDiff)
    }
  }

  test("diff with one id column case-sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      doTestRequirement(left.diff(right, "ID"),
        "Some id columns do not exist: ID missing among id, value")

      val actual = left.diff(right, "id").orderBy("id")
      val reverse = right.diff(left, "id").orderBy("id")

      assert(actual.columns === expectedDiffColumns)
      assert(actual.collect() === expectedDiff)
      assert(reverse.columns === expectedDiffColumns)
      assert(reverse.collect() === expectedReverseDiff)
    }
  }

  test("diff with two id columns") {
    val left = Seq(
      Value2(1, Some(1), Some("one")),
      Value2(2, Some(1), Some("two.one")),
      Value2(2, Some(2), Some("two.two")),
      Value2(3, Some(1), Some("three"))
    ).toDS()

    val right = Seq(
      Value2(1, Some(1), Some("one")),
      Value2(2, Some(1), Some("two.one")),
      Value2(2, Some(2), Some("two.Two")),
      Value2(4, Some(1), Some("four"))
    ).toDS()

    val expected = Seq(
      Row("N", 1, 1, "one", "one"),
      Row("N", 2, 1, "two.one", "two.one"),
      Row("C", 2, 2, "two.two", "two.Two"),
      Row("D", 3, 1, "three", null),
      Row("I", 4, 1, null, "four")
    )

    val actual = left.diff(right, "id", "seq").orderBy("id", "seq")

    assert(actual.columns === Seq("diff", "id", "seq", "left_value", "right_value"))
    assert(actual.collect() === expected)
  }

  test("diff with all id columns") {
    val expected = Seq(
      Row("N", 1, "one"),
      Row("D", 2, "two"),
      Row("I", 2, "Two"),
      Row("D", 3, "three"),
      Row("I", 4, "four")
    )

    val actual = left.diff(right, "id", "value").orderBy("id", "diff")

    assert(actual.columns === Seq("diff", "id", "value"))
    assert(actual.collect() === expected)
  }

  test("diff with null values") {
    val left = Seq(
      Value(1, None),
      Value(2, None),
      Value(3, Some("three")),
      Value(4, None)
    ).toDS()

    val right = Seq(
      Value(1, None),
      Value(2, Some("two")),
      Value(3, None),
      Value(5, None)
    ).toDS()

    val expected = Seq(
      Row("N", 1, null, null),
      Row("C", 2, null, "two"),
      Row("C", 3, "three", null),
      Row("D", 4, null, null),
      Row("I", 5, null, null)
    )

    val actual = left.diff(right, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "left_value", "right_value"))
    assert(actual.collect() === expected)
  }

  test("diff with null id values") {
    val left = Seq(
      Value2(1, None, Some("one")),
      Value2(2, Some(1), Some("two.one")),
      Value2(2, Some(2), Some("two.two")),
      Value2(3, None, Some("three"))
    ).toDS()

    val right = Seq(
      Value2(1, None, Some("one")),
      Value2(2, Some(1), Some("two.one")),
      Value2(2, Some(2), Some("two.Two")),
      Value2(4, None, Some("four"))
    ).toDS()

    val expected = Seq(
      Row("N", 1, None.orNull, "one", "one"),
      Row("N", 2, 1, "two.one", "two.one"),
      Row("C", 2, 2, "two.two", "two.Two"),
      Row("D", 3, None.orNull, "three", None.orNull),
      Row("I", 4, None.orNull, None.orNull, "four")
    )

    val actual = left.diff(right, "id", "seq").orderBy("id", "seq")

    assert(actual.columns === Seq("diff", "id", "seq", "left_value", "right_value"))
    assert(actual.collect() === expected)
  }

  /**
   * Tests the column order of the produced diff DataFrame.
   */
  test("diff column order") {
    // left has same schema as right but different column order
    val left = Seq(
      // value1, id, value2, seq, value3
      ("val1.1.1", 1, "val1.1.2", 1, "val1.1.3"),
      ("val1.2.1", 1, "val1.2.2", 2, "val1.2.3"),
      ("val2.1.1", 2, "val2.1.2", 1, "val2.1.3")
    ).toDF("value1", "id", "value2", "seq", "value3")
    val right = Seq(
      // value2, seq, value3, id, value1
      ("val1.1.2", 1, "val1.1.3", 1, "val1.1.1"),
      ("val1.2.2", 2, "val1.2.3 changed", 1, "val1.2.1"),
      ("val2.2.2", 2, "val2.2.3", 2, "val2.2.1")
    ).toDF("value2", "seq", "value3", "id", "value1")

    // diffing left to right provides schema of result DataFrame different to right-to-left diff
    {
      val expected = Seq(
        Row("N", 1, 1, "val1.1.1", "val1.1.1", "val1.1.2", "val1.1.2", "val1.1.3", "val1.1.3"),
        Row("C", 1, 2, "val1.2.1", "val1.2.1", "val1.2.2", "val1.2.2", "val1.2.3", "val1.2.3 changed"),
        Row("D", 2, 1, "val2.1.1", null, "val2.1.2", null, "val2.1.3", null),
        Row("I", 2, 2, null, "val2.2.1", null, "val2.2.2", null, "val2.2.3")
      )
      val expectedColumns = Seq(
        "diff",
        "id", "seq",
        "left_value1", "right_value1",
        "left_value2", "right_value2",
        "left_value3", "right_value3"
      )

      val actual = left.diff(right, "id", "seq").orderBy("id", "seq")

      assert(actual.columns === expectedColumns)
      assert(actual.collect() === expected)
    }

    // diffing right to left provides different schema of result DataFrame
    {
      val expected = Seq(
        Row("N", 1, 1, "val1.1.2", "val1.1.2", "val1.1.3", "val1.1.3", "val1.1.1", "val1.1.1"),
        Row("C", 1, 2, "val1.2.2", "val1.2.2", "val1.2.3 changed", "val1.2.3", "val1.2.1", "val1.2.1"),
        Row("I", 2, 1, null, "val2.1.2", null, "val2.1.3", null, "val2.1.1"),
        Row("D", 2, 2, "val2.2.2", null, "val2.2.3", null, "val2.2.1", null)
      )
      val expectedColumns = Seq(
        "diff",
        "id", "seq",
        "left_value2", "right_value2",
        "left_value3", "right_value3",
        "left_value1", "right_value1"
      )

      val actual = right.diff(left, "id", "seq").orderBy("id", "seq")

      assert(actual.columns === expectedColumns)
      assert(actual.collect() === expected)
    }

    // diffing left to right without id columns takes column order of left
    {
      val expected = Seq(
        Row("N", "val1.1.1", 1, "val1.1.2", 1, "val1.1.3"),
        Row("D", "val1.2.1", 1, "val1.2.2", 2, "val1.2.3"),
        Row("I", "val1.2.1", 1, "val1.2.2", 2, "val1.2.3 changed"),
        Row("D", "val2.1.1", 2, "val2.1.2", 1, "val2.1.3"),
        Row("I", "val2.2.1", 2, "val2.2.2", 2, "val2.2.3")
      )
      val expectedColumns = Seq(
        "diff", "value1", "id", "value2", "seq", "value3"
      )

      val actual = left.diff(right).orderBy("id", "seq", "diff")

      assert(actual.columns === expectedColumns)
      assert(actual.collect() === expected)
    }

    // diffing right to left without id columns takes column order of right
    {
      val expected = Seq(
        Row("N", "val1.1.1", 1, "val1.1.2", 1, "val1.1.3"),
        Row("D", "val1.2.1", 1, "val1.2.2", 2, "val1.2.3"),
        Row("I", "val1.2.1", 1, "val1.2.2", 2, "val1.2.3 changed"),
        Row("D", "val2.1.1", 2, "val2.1.2", 1, "val2.1.3"),
        Row("I", "val2.2.1", 2, "val2.2.2", 2, "val2.2.3")
      )
      val expectedColumns = Seq(
        "diff", "value1", "id", "value2", "seq", "value3"
      )

      val actual = left.diff(right).orderBy("id", "seq", "diff")

      assert(actual.columns === expectedColumns)
      assert(actual.collect() === expected)
    }
  }

  test("diff DataFrames") {
    val actual = left.toDF().diff(right.toDF(), "id").orderBy("id")
    val reverse = right.toDF().diff(left.toDF(), "id").orderBy("id")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff)
    assert(reverse.columns === expectedDiffColumns)
    assert(reverse.collect() === expectedReverseDiff)
  }

  test("diff with output columns in T") {
    val left = Seq(Value3(1, "left", "right", "value")).toDS()
    val right = Seq(Value3(1, "Left", "Right", "Value")).toDS()

    val actual = left.diff(right, "id")
    val expectedColumns = Seq(
      "diff",
      "id",
      "left_left_value", "right_left_value",
      "left_right_value", "right_right_value",
      "left_value", "right_value"
    )
    val expectedDiff = Seq(
      Row("C", 1, "left", "Left", "right", "Right", "value", "Value")
    )

    assert(actual.columns === expectedColumns)
    assert(actual.collect() === expectedDiff)
  }

  test("diff with id column diff in T") {
    val left = Seq(Value4(1, "diff")).toDS()
    val right = Seq(Value4(1, "Diff")).toDS()

    doTestRequirement(left.diff(right),
      "The id columns must not contain the diff column name 'diff': id, diff")
    doTestRequirement(left.diff(right, "diff"),
      "The id columns must not contain the diff column name 'diff': diff")
    doTestRequirement(left.diff(right, "diff", "id"),
      "The id columns must not contain the diff column name 'diff': diff, id")

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      doTestRequirement(left.withColumnRenamed("diff", "Diff")
        .diff(right.withColumnRenamed("diff", "Diff"), "Diff", "id"),
        "The id columns must not contain the diff column name 'diff': diff, id")
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      left.withColumnRenamed("diff", "Diff")
        .diff(right.withColumnRenamed("diff", "Diff"), "Diff", "id")
    }
  }

  test("diff with non-id column diff in T") {
    val left = Seq(Value4(1, "diff")).toDS()
    val right = Seq(Value4(1, "Diff")).toDS()

    val actual = left.diff(right, "id")
    val expectedColumns = Seq(
      "diff",
      "id",
      "left_diff", "right_diff"
    )
    val expectedDiff = Seq(
      Row("C", 1, "diff", "Diff")
    )

    assert(actual.columns === expectedColumns)
    assert(actual.collect() === expectedDiff)
  }

  test("diff where non-id column produces diff column name") {
    val options = DiffOptions.default
      .withDiffColumn("a_value")
      .withLeftColumnPrefix("a")
      .withRightColumnPrefix("b")

    doTestRequirement(left.diff(right, options, "id"),
      "The column prefixes 'a' and 'b', together with these non-id columns " +
        "must not produce the diff column name 'a_value': value")
  }

  test("diff with left-side mode where non-id column would produce diff column name") {
    val options = DiffOptions.default
      .withDiffColumn("a_value")
      .withLeftColumnPrefix("a")
      .withRightColumnPrefix("b")
      .withDiffMode(DiffMode.LeftSide)

    left.diff(right, options, "id")
  }

  test("diff with right-side mode where non-id column would produce diff column name") {
    val options = DiffOptions.default
      .withDiffColumn("a_value")
      .withLeftColumnPrefix("a")
      .withRightColumnPrefix("b")
      .withDiffMode(DiffMode.RightSide)

    left.diff(right, options, "id")
  }

  test("diff where case-insensitive non-id column produces diff column name") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val options = DiffOptions.default
        .withDiffColumn("a_value")
        .withLeftColumnPrefix("A")
        .withRightColumnPrefix("B")

      doTestRequirement(left.diff(right, options, "id"),
        "The column prefixes 'A' and 'B', together with these non-id columns " +
          "must not produce the diff column name 'a_value': value")
    }
  }

  test("diff with left-side mode where case-insensitive non-id column would produce diff column name") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val options = DiffOptions.default
        .withDiffColumn("a_value")
        .withLeftColumnPrefix("A")
        .withRightColumnPrefix("B")
        .withDiffMode(DiffMode.LeftSide)

      left.diff(right, options, "id")
    }
  }

  test("diff with right-side mode where case-insensitive non-id column would produce diff column name") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val options = DiffOptions.default
        .withDiffColumn("a_value")
        .withLeftColumnPrefix("A")
        .withRightColumnPrefix("B")
        .withDiffMode(DiffMode.RightSide)

      left.diff(right, options, "id")
    }
  }

  test("diff where case-sensitive non-id column produces non-conflicting diff column name") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val options = DiffOptions.default
        .withDiffColumn("a_value")
        .withLeftColumnPrefix("A")
        .withRightColumnPrefix("B")

      val actual = left.diff(right, options, "id").orderBy("id")
      val expectedColumns = Seq(
        "a_value", "id", "A_value", "B_value"
      )

      assert(actual.columns === expectedColumns)
      assert(actual.collect() === expectedDiff)
    }
  }

  test("diff where non-id column produces change column name") {
    val options = DiffOptions.default
      .withChangeColumn("a_value")
      .withLeftColumnPrefix("a")
      .withRightColumnPrefix("b")

    doTestRequirement(left.diff(right, options, "id"),
      "The column prefixes 'a' and 'b', together with these non-id columns " +
        "must not produce the change column name 'a_value': value")
  }

  test("diff where case-insensitive non-id column produces change column name") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val options = DiffOptions.default
        .withChangeColumn("a_value")
        .withLeftColumnPrefix("A")
        .withRightColumnPrefix("B")

      doTestRequirement(left.diff(right, options, "id"),
        "The column prefixes 'A' and 'B', together with these non-id columns " +
          "must not produce the change column name 'a_value': value")
    }
  }

  test("diff where case-sensitive non-id column produces non-conflicting change column name") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val options = DiffOptions.default
        .withChangeColumn("a_value")
        .withLeftColumnPrefix("A")
        .withRightColumnPrefix("B")

      val actual = left7.diff(right7, options, "id").orderBy("id")
      val expectedColumns = Seq(
        "diff", "a_value", "id", "A_value", "B_value", "A_label", "B_label"
      )

      assert(actual.columns === expectedColumns)
      assert(actual.collect() === expectedDiff7WithChanges)
    }
  }

  test("diff where non-id column produces id column name") {
    val options = DiffOptions.default
      .withLeftColumnPrefix("first")
      .withRightColumnPrefix("second")

    val left = Seq(Value5(1, "value")).toDS()
    val right = Seq(Value5(1, "Value")).toDS()

    doTestRequirement(left.diff(right, options, "first_id"),
      "The column prefixes 'first' and 'second', together with these non-id columns " +
        "must not produce any id column name 'first_id': id")
  }

  test("diff where case-insensitive non-id column produces id column name") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val options = DiffOptions.default
        .withLeftColumnPrefix("FIRST")
        .withRightColumnPrefix("SECOND")

      val left = Seq(Value5(1, "value")).toDS()
      val right = Seq(Value5(1, "Value")).toDS()

      doTestRequirement(left.diff(right, options, "first_id"),
        "The column prefixes 'FIRST' and 'SECOND', together with these non-id columns " +
          "must not produce any id column name 'first_id': id")
    }
  }

  test("diff where case-sensitive non-id column produces non-conflicting id column name") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val options = DiffOptions.default
        .withLeftColumnPrefix("FIRST")
        .withRightColumnPrefix("SECOND")

      val left = Seq(Value5(1, "value")).toDS()
      val right = Seq(Value5(1, "Value")).toDS()

      val actual = left.diff(right, options, "first_id")
      val expectedColumns = Seq(
        "diff", "first_id", "FIRST_id", "SECOND_id"
      )

      assert(actual.columns === expectedColumns)
      assert(actual.collect() === Seq(Row("C", 1, "value", "Value")))
    }
  }

  test("diff with custom diff options") {
    val options = DiffOptions("action", "before", "after", "new", "change", "del", "eq")

    val expected = Seq(
      Row("eq", 1, "one", "one"),
      Row("change", 2, "two", "Two"),
      Row("del", 3, "three", null),
      Row("new", 4, null, "four")
    )

    val actual = left.diff(right, options, "id").orderBy("id", "action")

    assert(actual.columns === Seq("action", "id", "before_value", "after_value"))
    assert(actual.collect() === expected)
  }

  test("diff of empty schema") {
    val left = Seq(Empty()).toDS()
    val right = Seq(Empty()).toDS()

    doTestRequirement(left.diff(right), "The schema must not be empty")
  }

  test("diff with different types") {
    // different value types only compiles with DataFrames
    val left = Seq((1, "str")).toDF("id", "value")
    val right = Seq((1, 2)).toDF("id", "value")

    doTestRequirement(left.diff(right),
      "The datasets do not have the same schema.\n" +
        "Left extra columns: value (StringType)\n" +
        "Right extra columns: value (IntegerType)")
  }

  test("diff with different nullability") {
    val leftSchema = StructType(left.schema.fields.map(_.copy(nullable = true)))
    val rightSchema = StructType(right.schema.fields.map(_.copy(nullable = false)))

    // different value types only compiles with DataFrames
    val left2 = sql.createDataFrame(left.toDF().rdd, leftSchema)
    val right2 = sql.createDataFrame(right.toDF().rdd, rightSchema)

    val actual = left2.diff(right2, "id").orderBy("id")
    val reverse = right2.diff(left2, "id").orderBy("id")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff)
    assert(reverse.columns === expectedDiffColumns)
    assert(reverse.collect() === expectedReverseDiff)
  }

  test("diff with different column names") {
    // different column names only compiles with DataFrames
    val left = Seq((1, "str")).toDF("id", "value")
    val right = Seq((1, "str")).toDF("id", "comment")

    doTestRequirement(left.diff(right, "id"),
      "The datasets do not have the same schema.\n" +
        "Left extra columns: value (StringType)\n" +
        "Right extra columns: comment (StringType)")
  }

  test("diff with case-insensitive column names") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      // different column names only compiles with DataFrames
      val left = this.left.toDF("id", "value")
      val right = this.right.toDF("ID", "VaLuE")

      val actual = left.diff(right, "id").orderBy("id")
      val reverse = right.diff(left, "id").orderBy("id")

      assert(actual.columns === expectedDiffColumns)
      assert(actual.collect() === expectedDiff)
      assert(reverse.columns === Seq("diff", "id", "left_VaLuE", "right_VaLuE"))
      assert(reverse.collect() === expectedReverseDiff)
    }
  }

  test("diff with case-sensitive column names") {
    // different column names only compiles with DataFrames
    val left = this.left.toDF("id", "value")
    val right = this.right.toDF("ID", "VaLuE")

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      doTestRequirement(left.diff(right, "id"),
        "The datasets do not have the same schema.\n" +
          "Left extra columns: id (IntegerType), value (StringType)\n" +
          "Right extra columns: ID (IntegerType), VaLuE (StringType)")
    }
  }

  test("diff of non-existing id column") {
    doTestRequirement(left.diff(right, "does not exists"),
      "Some id columns do not exist: does not exists missing among id, value")
  }

  test("diff with different number of column") {
    // different column names only compiles with DataFrames
    val left = Seq((1, "str")).toDF("id", "value")
    val right = Seq((1, 1, "str")).toDF("id", "seq", "value")

    doTestRequirement(left.diff(right, "id"),
      "The number of columns doesn't match.\n" +
        "Left column names (2): id, value\n" +
        "Right column names (3): id, seq, value")
  }

  test("diff as U") {
    val actual = left.diffAs[DiffAs](right, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "left_value", "right_value"))
    assert(actual.collect() === expectedDiffAs)
  }

  test("diff as U with encoder") {
    val encoder = Encoders.product[DiffAs]

    val actual = left.diffAs(right, encoder, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "left_value", "right_value"))
    assert(actual.collect() === expectedDiffAs)
  }

  test("diff as U with encoder and custom options") {
    val options = DiffOptions("action", "before", "after", "new", "change", "del", "eq")
    val encoder = Encoders.product[DiffAsCustom]

    val actions = Seq(
      (DiffOptions.default.insertDiffValue, "new"),
      (DiffOptions.default.changeDiffValue, "change"),
      (DiffOptions.default.deleteDiffValue, "del"),
      (DiffOptions.default.nochangeDiffValue, "eq")
    ).toDF("diff", "action")

    val expected = expectedDiffAs.toDS()
      .join(actions, "diff")
      .select($"action", $"id", $"left_value".as("before_value"), $"right_value".as("after_value"))
      .as[DiffAsCustom]
      .collect()

    val actual = left.diffAs(right, options, encoder, "id").orderBy("id")

    assert(actual.columns === Seq("action", "id", "before_value", "after_value"))
    assert(actual.collect() === expected)
  }

  test("diff as U with subset of columns") {
    val expected = expectedDiff.map(row => DiffAsSubset(row.getString(0), row.getInt(1), Option(row.getString(2))))

    val actual = left.diffAs[DiffAsSubset](right, "id").orderBy("id")

    assert(Seq("diff", "id", "left_value").forall(column => actual.columns.contains(column)))
    assert(actual.collect() === expected)
  }

  test("diff as U with extra column") {
    doTestRequirement(left.diffAs[DiffAsExtra](right, "id"),
      "Diff encoder's columns must be part of the diff result schema, these columns are unexpected: extra")
  }

  test("diff with change column") {
    val options = DiffOptions.default.withChangeColumn("changes")
    val actual = left7.diff(right7, options, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "changes", "id", "left_value", "right_value", "left_label", "right_label"))
    assert(actual.schema === StructType(Seq(
      StructField("diff", StringType, nullable = false),
      StructField("changes", ArrayType(StringType, containsNull = false), nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("left_value", StringType, nullable = true),
      StructField("right_value", StringType, nullable = true),
      StructField("left_label", StringType, nullable = true),
      StructField("right_label", StringType, nullable = true)
    )))
    assert(actual.collect() === expectedDiff7WithChanges)
  }

  test("diff with change column without id columns") {
    val options = DiffOptions.default.withChangeColumn("changes")
    val actual = left7.diff(right7, options)

    assert(actual.columns === Seq("diff", "changes", "id", "value", "label"))
    assert(actual.schema === StructType(Seq(
      StructField("diff", StringType, nullable = false),
      StructField("changes", ArrayType(StringType, containsNull = false), nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("value", StringType, nullable = true),
      StructField("label", StringType, nullable = true)
    )))
    assert(actual.select($"diff", $"changes").distinct().orderBy($"diff").collect() ===
      Seq(Row("D", null), Row("I", null), Row("N", Seq.empty[String])))
  }

  test("diff with change column name in non-id columns") {
    val options = DiffOptions.default.withChangeColumn("value")
    val actual = left7.diff(right7, options, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "value", "id", "left_value", "right_value", "left_label", "right_label"))
    assert(actual.collect() === expectedDiff7WithChanges)
  }

  test("diff with change column name in id columns") {
    val options = DiffOptions.default.withChangeColumn("value")
    doTestRequirement(left.diff(right, options, "id", "value"),
      "The id columns must not contain the change column name 'value': id, value")
  }

  test("diff with column-by-column diff mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.ColumnByColumn)
    val actual = left7.diff(right7, options, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "left_value", "right_value", "left_label", "right_label"))
    assert(actual.collect() === expectedDiff7)
  }

  test("diff with side-by-side diff mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.SideBySide)
    val actual = left7.diff(right7, options, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "left_value", "left_label", "right_value", "right_label"))
    assert(actual.collect() === expectedSideBySideDiff7)
  }

  test("diff with left-side diff mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.LeftSide)
    val actual = left7.diff(right7, options, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "value", "label"))
    assert(actual.collect() === expectedLeftSideDiff7)
  }

  test("diff with right-side diff mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.RightSide)
    val actual = left7.diff(right7, options, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "value", "label"))
    assert(actual.collect() === expectedRightSideDiff7)
  }

  test("diff as U with left-side diff mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.LeftSide)
    val actual = left.diffAs[DiffAsOneSide](right, options, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "value"))
    val expected: Seq[DiffAsOneSide] = Seq(
      DiffAsOneSide("N", 1, Some("one")),
      DiffAsOneSide("C", 2, Some("two")),
      DiffAsOneSide("D", 3, Some("three")),
      DiffAsOneSide("I", 4, None)
    )
    assert(actual.collect() === expected)
  }

  test("diff as U with right-side diff mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.RightSide)
    val actual = left.diffAs[DiffAsOneSide](right, options, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "value"))
    val expected: Seq[DiffAsOneSide] = Seq(
      DiffAsOneSide("N", 1, Some("one")),
      DiffAsOneSide("C", 2, Some("Two")),
      DiffAsOneSide("D", 3, None),
      DiffAsOneSide("I", 4, Some("four"))
    )
    assert(actual.collect() === expected)
  }

  test("diff with left-side diff mode and diff column name in value columns") {
    val options = DiffOptions.default.withDiffColumn("value").withDiffMode(DiffMode.LeftSide)
    doTestRequirement(left.diff(right, options, "id"),
      "The non-id columns must not contain the diff column name 'value': value")
  }

  test("diff with right-side diff mode and diff column name in value columns") {
    val options = DiffOptions.default.withDiffColumn("value").withDiffMode(DiffMode.RightSide)
    doTestRequirement(right.diff(right, options, "id"),
    "The non-id columns must not contain the diff column name 'value': value")
  }

  test("diff with left-side diff mode and change column name in value columns") {
    val options = DiffOptions.default.withChangeColumn("value").withDiffMode(DiffMode.LeftSide)
    doTestRequirement(left.diff(right, options, "id"),
      "The non-id columns must not contain the change column name 'value': value")
  }

  test("diff with right-side diff mode and change column name in value columns") {
    val options = DiffOptions.default.withChangeColumn("value").withDiffMode(DiffMode.RightSide)
    doTestRequirement(right.diff(right, options, "id"),
      "The non-id columns must not contain the change column name 'value': value")
  }

  test("diff with dots in diff column") {
    val options = DiffOptions.default
      .withDiffColumn("the.diff")

    val actual = left.diff(right, options, "id").orderBy("id")
    val expectedDiffColumns = Seq("the.diff", "id", "left_value", "right_value")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff)
  }

  test("diff with dots in change column") {
    val options = DiffOptions.default
      .withChangeColumn("the.changes")

    val actual = left7.diff(right7, options, "id").orderBy("id")
    val expectedDiffColumns = Seq("diff", "the.changes", "id", "left_value", "right_value", "left_label", "right_label")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff7WithChanges)
  }

  test("diff with dots in prefixes") {
    val options = DiffOptions.default
      .withLeftColumnPrefix("left.prefix")
      .withRightColumnPrefix("right.prefix")

    val actual = left.diff(right, options, "id").orderBy("id")
    val expectedDiffColumns = Seq("diff", "id", "left.prefix_value", "right.prefix_value")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff)
  }

  test("diff with dot in id column") {
    val l = left7.withColumnRenamed("id", "the.id")
    val r = right7.withColumnRenamed("id", "the.id")

    val actual = l.diff(r, "the.id").orderBy("`the.id`")
    val expectedDiffColumns = Seq("diff", "the.id", "left_value", "right_value", "left_label", "right_label")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff7)
  }

  test("diff with dot in value column") {
    val l = left7.withColumnRenamed("value", "the.value")
    val r = right7.withColumnRenamed("value", "the.value")

    val actual = l.diff(r, "id").orderBy("id")
    val expectedDiffColumns = Seq("diff", "id", "left_the.value", "right_the.value", "left_label", "right_label")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff7)
  }

  test("diff with left-side diff mode and dot in value column") {
    val l = left7.withColumnRenamed("value", "the.value")
    val r = right7.withColumnRenamed("value", "the.value")
    val options = DiffOptions.default.withDiffMode(DiffMode.LeftSide)

    val actual = l.diff(r, options, "id").orderBy("id")
    val expectedDiffColumns = Seq("diff", "id", "the.value", "label")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedLeftSideDiff7)
  }

  test("diff with right-side diff mode and dot in value column") {
    val l = left7.withColumnRenamed("value", "the.value")
    val r = right7.withColumnRenamed("value", "the.value")
    val options = DiffOptions.default.withDiffMode(DiffMode.RightSide)

    val actual = l.diff(r, options, "id").orderBy("id")
    val expectedDiffColumns = Seq("diff", "id", "the.value", "label")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedRightSideDiff7)
  }

  test("diff with column-by-column and sparse mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.ColumnByColumn).withSparseMode(true)
    val actual = left7.diff(right7, options, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "left_value", "right_value", "left_label", "right_label"))
    assert(actual.collect() === expectedSparseDiff7)
  }

  test("diff with side-by-side and sparse mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.SideBySide).withSparseMode(true)
    val actual = left7.diff(right7, options, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "left_value", "left_label", "right_value", "right_label"))
    assert(actual.collect() === expectedSideBySideSparseDiff7)
  }

  test("diff with left side and sparse mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.LeftSide).withSparseMode(true)
    val actual = left7.diff(right7, options, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "value", "label"))
    assert(actual.collect() === expectedLeftSideSparseDiff7)
  }

  test("diff with right side and sparse mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.RightSide).withSparseMode(true)
    val actual = left7.diff(right7, options, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "value", "label"))
    assert(actual.collect() === expectedRightSideSparseDiff7)
  }

  def assertIgnoredColumns[T](actual: Dataset[T], expected: Seq[T],
                              idColumns: Seq[String] = Seq("id", "seq"),
                              changedColumn: Option[String] = None,
                              diffColumns: Seq[String] = Seq("left_value", "right_value", "left_meta", "right_meta")): Unit = {
    val expectedColumns = Seq("diff") ++ changedColumn.map(Seq(_)).getOrElse(Seq.empty) ++ idColumns ++ diffColumns
    assert(actual.columns === expectedColumns)
    assert(actual.orderBy("id", "seq").collect() === expected)
  }

  test("diff with ignored columns") {
    assertIgnoredColumns(left8.diff(right8, Seq("id", "seq"), Seq("meta")), expectedDiff8)
    assertIgnoredColumns(Diff.of(left8, right8, Seq("id", "seq"), Seq("meta")), expectedDiff8)

    assertIgnoredColumns[DiffAs8](left8.diffAs(right8, Seq("id", "seq"), Seq("meta")), expectedDiffAs8)
    assertIgnoredColumns[DiffAs8](Diff.ofAs(left8, right8, Seq("id", "seq"), Seq("meta")), expectedDiffAs8)
  }

  test("diff with ignored and change columns") {
    val options = DiffOptions.default.withChangeColumn("changed")
    val diff = new Diff(options)

    assertIgnoredColumns(left8.diff(right8, options, Seq("id", "seq"), Seq("meta")), expectedDiff8WithChanges, changedColumn = options.changeColumn)
    assertIgnoredColumns(diff.of(left8, right8, Seq("id", "seq"), Seq("meta")), expectedDiff8WithChanges, changedColumn = options.changeColumn)
  }

  test("diff with ignored columns and column-by-column diff mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.ColumnByColumn)
    val diff = new Diff(options)

    assertIgnoredColumns(left8.diff(right8, options, Seq("id", "seq"), Seq("meta")), expectedDiff8)
    assertIgnoredColumns(diff.of(left8, right8, Seq("id", "seq"), Seq("meta")), expectedDiff8)
  }

  test("diff with ignored columns and side-by-side diff mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.SideBySide)
    val diff = new Diff(options)

    assertIgnoredColumns(left8.diff(right8, options, Seq("id", "seq"), Seq("meta")), expectedSideBySideDiff8, diffColumns = Seq("left_value", "left_meta", "right_value", "right_meta"))
    assertIgnoredColumns(diff.of(left8, right8, Seq("id", "seq"), Seq("meta")), expectedSideBySideDiff8, diffColumns = Seq("left_value", "left_meta", "right_value", "right_meta"))
  }

  test("diff with ignored columns and left-side diff mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.LeftSide)
    val diff = new Diff(options)

    assertIgnoredColumns(left8.diff(right8, options, Seq("id", "seq"), Seq("meta")), expectedLeftSideDiff8, diffColumns = Seq("value", "meta"))
    assertIgnoredColumns(diff.of(left8, right8, Seq("id", "seq"), Seq("meta")), expectedLeftSideDiff8, diffColumns = Seq("value", "meta"))
  }

  test("diff with ignored columns and right-side diff mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.RightSide)
    val diff = new Diff(options)

    assertIgnoredColumns(left8.diff(right8, options, Seq("id", "seq"), Seq("meta")), expectedRightSideDiff8, diffColumns = Seq("value", "meta"))
    assertIgnoredColumns(diff.of(left8, right8, Seq("id", "seq"), Seq("meta")), expectedRightSideDiff8, diffColumns = Seq("value", "meta"))
  }

  test("diff with ignored columns, column-by-column diff and sparse mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.ColumnByColumn).withSparseMode(true)
    val diff = new Diff(options)

    assertIgnoredColumns(left8.diff(right8, options, Seq("id", "seq"), Seq("meta")), expectedSparseDiff8)
    assertIgnoredColumns(diff.of(left8, right8, Seq("id", "seq"), Seq("meta")), expectedSparseDiff8)
  }

  test("diff with ignored columns, side-by-side diff and sparse mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.SideBySide).withSparseMode(true)
    val diff = new Diff(options)

    assertIgnoredColumns(left8.diff(right8, options, Seq("id", "seq"), Seq("meta")), expectedSideBySideSparseDiff8, diffColumns = Seq("left_value", "left_meta", "right_value", "right_meta"))
    assertIgnoredColumns(diff.of(left8, right8, Seq("id", "seq"), Seq("meta")), expectedSideBySideSparseDiff8, diffColumns = Seq("left_value", "left_meta", "right_value", "right_meta"))
  }

  test("diff with ignored columns, left-side diff and sparse mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.LeftSide).withSparseMode(true)
    val diff = new Diff(options)

    val expected = expectedSparseDiff8.map(r => Row(r.get(0), r.get(1), r.get(2), r.get(3), r.get(5)))
    assertIgnoredColumns(left8.diff(right8, options, Seq("id", "seq"), Seq("meta")), expectedLeftSideSparseDiff8, diffColumns = Seq("value", "meta"))
    assertIgnoredColumns(diff.of(left8, right8, Seq("id", "seq"), Seq("meta")), expectedLeftSideSparseDiff8, diffColumns = Seq("value", "meta"))
  }

  test("diff with ignored columns, right-side diff and sparse mode") {
    val options = DiffOptions.default.withDiffMode(DiffMode.RightSide).withSparseMode(true)
    val diff = new Diff(options)

    assertIgnoredColumns(left8.diff(right8, options, Seq("id", "seq"), Seq("meta")), expectedRightSideSparseDiff8, diffColumns = Seq("value", "meta"))
    assertIgnoredColumns(diff.of(left8, right8, Seq("id", "seq"), Seq("meta")), expectedRightSideSparseDiff8, diffColumns = Seq("value", "meta"))
  }

  test("diff with ignored columns case-insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val left = left8.toDF("id", "seq", "value", "meta")
      val right = right8.toDF("ID", "SEQ", "VALUE", "META")

      assertIgnoredColumns(left.diff(right, Seq("iD", "sEq"), Seq("MeTa")), expectedDiff8, idColumns = Seq("iD", "sEq"))
      assertIgnoredColumns(Diff.of(left, right, Seq("Id", "SeQ"), Seq("mEtA")), expectedDiff8, idColumns = Seq("Id", "SeQ"))

      assertIgnoredColumns[DiffAs8](left.diffAs(right, Seq("id", "seq"), Seq("MeTa")), expectedDiffAs8)
      assertIgnoredColumns[DiffAs8](Diff.ofAs(left, right, Seq("id", "seq"), Seq("mEtA")), expectedDiffAs8)
    }
  }

  test("diff with ignored columns case-sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val left = left8.toDF("id", "seq", "value", "meta")
      val right = right8.toDF("ID", "SEQ", "VALUE", "META")

      doTestRequirement(left.diff(right, Seq("Id", "SeQ"), Seq("MeTa")),
        "The datasets do not have the same schema.\nLeft extra columns: id (IntegerType), seq (IntegerType), value (StringType), meta (StringType)\nRight extra columns: ID (IntegerType), SEQ (IntegerType), VALUE (StringType), META (StringType)")

      doTestRequirement(left8.diff(right8, Seq("Id", "SeQ"), Seq("MeTa")),
        "Some id columns do not exist: Id, SeQ missing among id, seq, value, meta")
    }
  }

  def doTestRequirement(f: => Any, expected: String): Unit = {
    assert(intercept[IllegalArgumentException](f).getMessage === s"requirement failed: $expected")
  }

}
