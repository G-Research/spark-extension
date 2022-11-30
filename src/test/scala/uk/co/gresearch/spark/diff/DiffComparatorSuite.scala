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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{abs, lit, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.diff.DiffComparatorSuite.{decimalEnc, optionsWithRelaxedComparators, optionsWithTightComparators}
import uk.co.gresearch.spark.diff.comparator._

import java.sql.{Date, Timestamp}
import java.time.Duration

case class Numbers(id: Int, longValue: Long, floatValue: Float, doubleValue: Double, decimalValue: Decimal, someInt: Option[Int], someLong: Option[Long])
case class Dates(id: Int, date: Date)
case class Times(id: Int, time: Timestamp)
case class Maps(id: Int, map: Map[Int, Long])

class DiffComparatorSuite extends AnyFunSuite with SparkTestSession {

  import spark.implicits._

  lazy val left: Dataset[Numbers] = Seq(
    Numbers(1, 1L, 1.0f, 1.0, Decimal(10, 8, 3), None, None),
    Numbers(2, 2L, 2.0f, 2.0, Decimal(20, 8, 3), Some(2), Some(2L)),
    Numbers(3, 3L, 3.0f, 3.0, Decimal(30, 8, 3), Some(3), None),
    Numbers(4, 4L, 4.0f, 4.0, Decimal(40, 8, 3), None, Some(4L)),
  ).toDS()

  lazy val right: Dataset[Numbers] = Seq(
    Numbers(1, 1L, 1.0f, 1.0, Decimal(10, 8, 3), None, None),
    Numbers(2, 3L, 2.001f, 2.001, Decimal(21, 8, 3), Some(3), Some(3L)),
    Numbers(3, 3L, 3.0f, 3.0, Decimal(30, 8, 3), None, Some(3L)),
    Numbers(5, 5L, 5.0f, 5.0, Decimal(50, 8, 3), Some(5), Some(5L)),
  ).toDS()

  lazy val rightSign: Dataset[Numbers] = Seq(
    Numbers(1, 1L, 1.0f, 1.0, Decimal(10, 8, 3), None, None),
    Numbers(2, -2L, -2.0f, -2.0, Decimal(-20, 8, 3), Some(-2), Some(-2L)),
    Numbers(3, 3L, 3.0f, 3.0, Decimal(30, 8, 3), None, Some(3L)),
    Numbers(5, 5L, 5.0f, 5.0, Decimal(50, 8, 3), Some(5), Some(5L)),
  ).toDS()

  lazy val leftDates: Dataset[Dates] = Seq(
    Dates(1, Date.valueOf("2000-01-01")),
    Dates(2, Date.valueOf("2000-01-02")),
    Dates(3, Date.valueOf("2000-01-03")),
    Dates(4, Date.valueOf("2000-01-04")),
  ).toDS()

  lazy val rightDates: Dataset[Dates] = Seq(
    Dates(1, Date.valueOf("2000-01-01")),
    Dates(2, Date.valueOf("2000-01-03")),
    Dates(3, Date.valueOf("2000-01-03")),
    Dates(5, Date.valueOf("2000-01-05")),
  ).toDS()

  lazy val leftTimes: Dataset[Times] = Seq(
    Times(1, Timestamp.valueOf("2000-01-01 12:01:00")),
    Times(2, Timestamp.valueOf("2000-01-02 12:02:00")),
    Times(3, Timestamp.valueOf("2000-01-03 12:03:00")),
    Times(4, Timestamp.valueOf("2000-01-04 12:04:00")),
  ).toDS()

  lazy val rightTimes: Dataset[Times] = Seq(
    Times(1, Timestamp.valueOf("2000-01-01 12:01:00")),
    Times(2, Timestamp.valueOf("2000-01-02 12:03:00")),
    Times(3, Timestamp.valueOf("2000-01-03 12:03:00")),
    Times(5, Timestamp.valueOf("2000-01-04 12:05:00")),
  ).toDS()

  lazy val leftMaps: Dataset[Maps] = Seq(
    Maps(1, Map(1 -> 1L, 2 -> 2L, 3 -> 3L)),
    Maps(2, Map(1 -> 2L, 2 -> 2L, 3 -> 3L)),
    Maps(3, Map(1 -> 3L, 2 -> 2L, 3 -> 3L)),
    Maps(4, Map(1 -> 4L, 2 -> 2L, 3 -> 3L)),
  ).toDS()

  lazy val rightMaps: Dataset[Maps] = Seq(
    Maps(1, Map(1 -> 1L, 2 -> 2L, 3 -> 3L)),
    Maps(2, Map(1 -> 2L, 2 -> 3L, 3 -> 3L)),
    Maps(3, Map(1 -> 3L, 2 -> 2L, 4 -> 4L)),
    Maps(5, Map(1 -> 4L, 2 -> 2L, 3 -> 3L)),
  ).toDS()

  def doTest(optionsWithTightComparators: DiffOptions,
             optionsWithRelaxedComparators: DiffOptions,
             left: DataFrame = this.left.toDF(),
             right: DataFrame = this.right.toDF()): Unit = {
    // left and right numbers have some differences
    val actualWithoutComparators = left.diff(right, "id").orderBy($"id")

    // our tight comparators are just too strict to still see differences
    val actualWithTightComparators = left.diff(right, optionsWithTightComparators, "id").orderBy($"id")
    val expectedWithTightComparators = actualWithoutComparators
    assert(actualWithTightComparators.collect() === expectedWithTightComparators.collect())

    // the relaxed comparators are just relaxed enough to not see any differences
    // they still see changes to / from null values
    val actualWithRelaxedComparators = left.diff(right, optionsWithRelaxedComparators, "id").orderBy($"id")
    val expectedWithRelaxedComparators = actualWithoutComparators
      // the comparators are relaxed so that all changes disappear
      .withColumn("diff", when($"id" === 2, lit("N")).otherwise($"diff"))
    assert(actualWithRelaxedComparators.collect() === expectedWithRelaxedComparators.collect())
  }

  Seq("true", "false").foreach { codegen =>
    test(s"diff with custom comparator - codegen enabled=$codegen") {
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen,
        SQLConf.CODEGEN_FALLBACK.key -> "false"
      ) {
        doTest(optionsWithTightComparators, optionsWithRelaxedComparators)
      }
    }
  }

  Seq(
    "default diff comparator" -> DiffOptions.default
      .withDefaultComparator((left: Column, right: Column) => abs(left) <=> abs(right)),
    "default encoder equiv" -> DiffOptions.default
      .withDefaultComparator((left: Int, right: Int) => left.abs == right.abs)
      // the non-default comparator here are required because the default only supports int
      // see "encoder equiv" test below
      .withComparator((left: Long, right: Long) => left.abs == right.abs)
      .withComparator((left: Float, right: Float) => left.abs == right.abs)
      .withComparator((left: Double, right: Double) => left.abs == right.abs)
      .withComparator((left: Decimal, right: Decimal) => left.abs == right.abs),
    "default typed equiv" -> DiffOptions.default
      .withDefaultComparator((left: Int, right: Int) => left.abs == right.abs, IntegerType)
      // the non-default comparator here are required because the default only supports int
      // see "encoder equiv" test below
      .withComparator((left: Long, right: Long) => left.abs == right.abs)
      .withComparator((left: Float, right: Float) => left.abs == right.abs)
      .withComparator((left: Double, right: Double) => left.abs == right.abs)
      .withComparator((left: Decimal, right: Decimal) => left.abs == right.abs),
    "default any equiv" -> DiffOptions.default
      .withDefaultComparator((_: Any, _: Any) => true),

    "typed diff comparator" -> DiffOptions.default
      .withComparator(EquivDiffComparator((left: Int, right: Int) => left.abs == right.abs))
      .withComparator((left: Column, right: Column) => abs(left) <=> abs(right), LongType, FloatType, DoubleType, DecimalType(38, 18)),
    "typed diff comparator for type" -> DiffOptions.default
      // only works if data type is equal to input type of typed diff comparator
      .withComparator(EquivDiffComparator((left: Int, right: Int) => left.abs == right.abs), IntegerType)
      .withComparator((left: Column, right: Column) => abs(left) <=> abs(right), LongType, FloatType, DoubleType, DecimalType(38, 18)),

    "diff comparator for type" -> DiffOptions.default
      .withComparator((left: Column, right: Column) => abs(left) <=> abs(right), IntegerType)
      .withComparator((left: Column, right: Column) => abs(left) <=> abs(right), LongType, FloatType, DoubleType, DecimalType(38, 18)),
    "diff comparator for name" -> DiffOptions.default
      .withComparator((left: Column, right: Column) => abs(left) <=> abs(right), "someInt")
      .withComparator((left: Column, right: Column) => abs(left) <=> abs(right), "longValue", "floatValue", "doubleValue", "someLong", "decimalValue"),

    "encoder equiv" -> DiffOptions.default
      .withComparator((left: Int, right: Int) => left.abs == right.abs)
      .withComparator((left: Long, right: Long) => left.abs == right.abs)
      .withComparator((left: Float, right: Float) => left.abs == right.abs)
      .withComparator((left: Double, right: Double) => left.abs == right.abs)
      .withComparator((left: Decimal, right: Decimal) => left.abs == right.abs),
    "encoder equiv for column name" -> DiffOptions.default
      .withComparator((left: Int, right: Int) => left.abs == right.abs, "someInt")
      .withComparator((left: Long, right: Long) => left.abs == right.abs, "longValue", "someLong")
      .withComparator((left: Float, right: Float) => left.abs == right.abs, "floatValue")
      .withComparator((left: Double, right: Double) => left.abs == right.abs, "doubleValue")
      .withComparator((left: Decimal, right: Decimal) => left.abs == right.abs, "decimalValue"),
    "equiv encoder for column name" -> DiffOptions.default
      .withComparator((left: Int, right: Int) => left.abs == right.abs, Encoders.scalaInt, "someInt")
      .withComparator((left: Long, right: Long) => left.abs == right.abs, Encoders.scalaLong, "longValue", "someLong")
      .withComparator((left: Float, right: Float) => left.abs == right.abs, Encoders.scalaFloat, "floatValue")
      .withComparator((left: Double, right: Double) => left.abs == right.abs, Encoders.scalaDouble, "doubleValue")
      .withComparator((left: Decimal, right: Decimal) => left.abs == right.abs, ExpressionEncoder[Decimal](), "decimalValue"),

    "any equiv for type" -> DiffOptions.default
      .withComparator((_: Any, _: Any) => true, IntegerType)
      .withComparator((_: Any, _: Any) => true, LongType, FloatType, DoubleType, DecimalType(38, 18)),
    "any equiv for column name" -> DiffOptions.default
      .withComparator((_: Any, _: Any) => true, "someInt")
      .withComparator((_: Any, _: Any) => true, "longValue", "floatValue", "doubleValue", "someLong", "decimalValue")
  ).foreach { case (label, options) =>
    test(s"with comparator - $label") {
      val diffWithoutComparators = left.diff(rightSign, "id")
      assert(diffWithoutComparators.where($"diff" === "C").count() === 2)

      val expected = diffWithoutComparators.withColumn("diff", when($"id" === 2, lit("N")).otherwise($"diff"))
      assert(expected.where($"diff" === "C").count() === 1)

      val actual = left.diff(rightSign, options, "id").orderBy($"id").collect()
      assert(actual !== diffWithoutComparators.orderBy($"id").collect())
      assert(actual === expected.orderBy($"id").collect())
    }
  }

  Seq(
    "diff comparator" -> (DiffOptions.default
      .withDefaultComparator((_: Column, _: Column) => lit(1)),
      Seq(
        "'(1 AND 1)' requires boolean type, not int",  // until Spark 3.3
        "\"(1 AND 1)\" due to data type mismatch: " +  // Spark 3.4 and beyond
          "the binary operator requires the input type \"BOOLEAN\", not \"INT\"."
      )
    ),
    "encoder equiv" -> (DiffOptions.default
      .withDefaultComparator((_: Int, _: Int) => true),
      Seq(
        "'(`longValue` ≡ `longValue`)' requires int type, not bigint",  // Spark 3.0 and 3.1
        "'(longValue ≡ longValue)' requires int type, not bigint",  // Spark 3.2 and 3.3
        "\"(longValue ≡ longValue)\" due to data type mismatch: " +  // Spark 3.4 and beyond
          "the binary operator requires the input type \"INT\", not \"BIGINT\"."
      )
    ),
    "typed equiv" -> (DiffOptions.default
      .withDefaultComparator(EquivDiffComparator((left: Int, right: Int) => left.abs == right.abs, IntegerType)),
      Seq(
        "'(`longValue` ≡ `longValue`)' requires int type, not bigint",  // Spark 3.0 and 3.1
        "'(longValue ≡ longValue)' requires int type, not bigint",  // Spark 3.2 and 3.3
        "\"(longValue ≡ longValue)\" due to data type mismatch: " +  // Spark 3.4 and beyond
          "the binary operator requires the input type \"INT\", not \"BIGINT\"."
      )
    )
  ).foreach { case (label, (options, expecteds)) =>
    test(s"with comparator of incompatible type - $label") {
      val exception = intercept[AnalysisException] {
        left.diff(right, options, "id")
      }
      assert(expecteds.nonEmpty)
      assert(expecteds.exists(expected => exception.message.contains(expected)), exception.message)
    }
  }

  test("absolute epsilon comparator (inclusive)") {
    val optionsWithTightComparator = DiffOptions.default.withDefaultComparator(DiffComparator.epsilon(0.5).asAbsolute().asInclusive())
    val optionsWithRelaxedComparator = DiffOptions.default.withDefaultComparator(DiffComparator.epsilon(1.0).asAbsolute().asInclusive())
    doTest(optionsWithTightComparator, optionsWithRelaxedComparator)
  }

  test("absolute epsilon comparator (exclusive)") {
    val optionsWithTightComparator = DiffOptions.default.withDefaultComparator(DiffComparator.epsilon(1.0).asAbsolute().asExclusive())
    val optionsWithRelaxedComparator = DiffOptions.default.withDefaultComparator(DiffComparator.epsilon(1.001).asAbsolute().asExclusive())
    doTest(optionsWithTightComparator, optionsWithRelaxedComparator)
  }

  test("relative epsilon comparator (inclusive)") {
    val optionsWithTightComparator = DiffOptions.default.withDefaultComparator(DiffComparator.epsilon(0.1).asRelative().asInclusive())
    val optionsWithRelaxedComparator = DiffOptions.default.withDefaultComparator(DiffComparator.epsilon(1/3.0).asRelative().asInclusive())
    doTest(optionsWithTightComparator, optionsWithRelaxedComparator)
  }

  test("relative epsilon comparator (exclusive)") {
    val optionsWithTightComparator = DiffOptions.default.withDefaultComparator(DiffComparator.epsilon(1/3.0).asRelative().asExclusive())
    val optionsWithRelaxedComparator = DiffOptions.default.withDefaultComparator(DiffComparator.epsilon(1/3.0 + .001).asRelative().asExclusive())
    doTest(optionsWithTightComparator, optionsWithRelaxedComparator)
  }

  if (DurationDiffComparator.isNotSupportedBySpark) {
    test("duration comparator not supported") {
      assertThrows[UnsupportedOperationException] {
        DiffComparator.duration(Duration.ofHours(1))
      }
      assertThrows[UnsupportedOperationException] {
        DurationDiffComparator(Duration.ofHours(1))
      }
    }
  } else {
    test("duration comparator with date (inclusive)") {
      val optionsWithTightComparator = DiffOptions.default.withComparator(DiffComparator.duration(Duration.ofHours(23)).asInclusive(), "date")
      val optionsWithRelaxedComparator = DiffOptions.default.withComparator(DiffComparator.duration(Duration.ofHours(24)).asInclusive(), "date")
      doTest(optionsWithTightComparator, optionsWithRelaxedComparator, leftDates.toDF, rightDates.toDF)
    }

    test("duration comparator with date (exclusive)") {
      val optionsWithTightComparator = DiffOptions.default.withComparator(DiffComparator.duration(Duration.ofHours(24)).asExclusive(), "date")
      val optionsWithRelaxedComparator = DiffOptions.default.withComparator(DiffComparator.duration(Duration.ofHours(25)).asExclusive(), "date")
      doTest(optionsWithTightComparator, optionsWithRelaxedComparator, leftDates.toDF, rightDates.toDF)
    }

    test("duration comparator with time (inclusive)") {
      val optionsWithTightComparator = DiffOptions.default.withComparator(DiffComparator.duration(Duration.ofSeconds(59)).asInclusive(), "time")
      val optionsWithRelaxedComparator = DiffOptions.default.withComparator(DiffComparator.duration(Duration.ofSeconds(60)).asInclusive(), "time")
      doTest(optionsWithTightComparator, optionsWithRelaxedComparator, leftTimes.toDF, rightTimes.toDF)
    }

    test("duration comparator with time (exclusive)") {
      val optionsWithTightComparator = DiffOptions.default.withComparator(DiffComparator.duration(Duration.ofSeconds(60)).asExclusive(), "time")
      val optionsWithRelaxedComparator = DiffOptions.default.withComparator(DiffComparator.duration(Duration.ofSeconds(61)).asExclusive(), "time")
      doTest(optionsWithTightComparator, optionsWithRelaxedComparator, leftTimes.toDF, rightTimes.toDF)
    }
  }

  Seq("true", "false").foreach { codegen =>
    test(s"map comparator - codegen enabled=$codegen") {
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen,
        SQLConf.CODEGEN_FALLBACK.key -> "false"
      ) {
        val options = DiffOptions.default.withComparator(DiffComparator.map[Int, Long](), "map")

        val actual = leftMaps.diff(rightMaps, options, "id").orderBy($"id").collect()
        val diffs = Seq((1, "N"), (2, "C"), (3, "C"), (4, "D"), (5, "I")).toDF("id", "diff")
        val expected = leftMaps.withColumnRenamed("map", "left_map")
          .join(rightMaps.withColumnRenamed("map", "right_map"), Seq("id"), "fullouter")
          .join(diffs, "id")
          .select($"diff", $"id", $"left_map", $"right_map")
          .orderBy($"id").collect()
        assert(actual === expected)
      }
    }
  }

  case object IntEquiv extends math.Equiv[Int] {
    override def equiv(x: Int, y: Int): Boolean = true
  }

  case object AnyEquiv extends math.Equiv[Any] {
    override def equiv(x: Any, y: Any): Boolean = true
  }

  val diffComparatorMethodTests: Seq[(String, (() => DiffComparator, DiffComparator))] =
    if(DurationDiffComparator.isSupportedBySpark) {
      Seq("duration" -> (() => DiffComparator.duration(Duration.ofSeconds(1)).asExclusive(), DurationDiffComparator(Duration.ofSeconds(1), inclusive = false)))
    } else { Seq.empty } ++ Seq(
      "default" -> (() => DiffComparator.default(), DefaultDiffComparator),
      "nullSafeEqual" -> (() => DiffComparator.nullSafeEqual(), NullSafeEqualDiffComparator),
      "equiv with encoder" -> (() => DiffComparator.equiv(IntEquiv), EquivDiffComparator(IntEquiv)),
      "equiv with type" -> (() => DiffComparator.equiv(IntEquiv, IntegerType), EquivDiffComparator(IntEquiv, IntegerType)),
      "equiv with any" -> (() => DiffComparator.equiv(AnyEquiv), EquivDiffComparator(AnyEquiv)),
      "epsilon" -> (() => DiffComparator.epsilon(1.0).asAbsolute().asExclusive(), EpsilonDiffComparator(1.0, relative = false, inclusive = false))
    )

  diffComparatorMethodTests.foreach { case (label, (method, expected)) =>
    test(s"DiffComparator.$label") {
      val actual = method()
      assert(actual === expected)
    }
  }
}

object DiffComparatorSuite {
  implicit val intEnc: Encoder[Int] = Encoders.scalaInt
  implicit val longEnc: Encoder[Long] = Encoders.scalaLong
  implicit val floatEnc: Encoder[Float] = Encoders.scalaFloat
  implicit val doubleEnc: Encoder[Double] = Encoders.scalaDouble
  implicit val decimalEnc: Encoder[Decimal] = ExpressionEncoder()

  val tightIntComparator: EquivDiffComparator[Int] = EquivDiffComparator((x: Int, y: Int) => math.abs(x - y) < 1)
  val tightLongComparator: EquivDiffComparator[Long] = EquivDiffComparator((x: Long, y: Long) => math.abs(x - y) < 1)
  val tightFloatComparator: EquivDiffComparator[Float] = EquivDiffComparator((x: Float, y: Float) => math.abs(x - y) < 0.001)
  val tightDoubleComparator: EquivDiffComparator[Double] = EquivDiffComparator((x: Double, y: Double) => math.abs(x - y) < 0.001)
  val tightDecimalComparator: EquivDiffComparator[Decimal] = EquivDiffComparator[Decimal]((x: Decimal, y: Decimal) => (x - y).abs < Decimal(0.001))

  val optionsWithTightComparators: DiffOptions = DiffOptions.default
    .withComparator(tightIntComparator, IntegerType)
    .withComparator(tightLongComparator, LongType)
    .withComparator(tightFloatComparator, "floatValue")
    .withComparator(tightDoubleComparator, "doubleValue")
    .withComparator(tightDecimalComparator, DecimalType(38, 18))

  val relaxedIntComparator: EquivDiffComparator[Int] = EquivDiffComparator((x: Int, y: Int) => math.abs(x - y) <= 1)
  val relaxedLongComparator: EquivDiffComparator[Long] = EquivDiffComparator((x: Long, y: Long) => math.abs(x - y) <= 1)
  val relaxedFloatComparator: EquivDiffComparator[Float] = EquivDiffComparator((x: Float, y: Float) => math.abs(x - y) <= 0.001)
  val relaxedDoubleComparator: EquivDiffComparator[Double] = EquivDiffComparator((x: Double, y: Double) => math.abs(x - y) <= 0.001)
  val relaxedDecimalComparator: EquivDiffComparator[Decimal] = EquivDiffComparator[Decimal]((x: Decimal, y: Decimal) => (x - y).abs <= Decimal(0.001))

  val optionsWithRelaxedComparators: DiffOptions = DiffOptions.default
    .withComparator(relaxedIntComparator, IntegerType)
    .withComparator(relaxedLongComparator, LongType)
    .withComparator(relaxedFloatComparator, "floatValue")
    .withComparator(relaxedDoubleComparator, "doubleValue")
    .withComparator(relaxedDecimalComparator, DecimalType(38, 18))
}
