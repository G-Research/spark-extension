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

package uk.co.gresearch.spark

import org.apache.spark.sql.{AnalysisException, Dataset, DataFrame}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

case class IntValue(id: Int, title: String, value: Int)
case class DoubleValue(id: Int, title: String, value: Double)

class HistogramSuite extends AnyFunSuite with SparkTestSession {

  import spark.implicits._

  val ints: Dataset[IntValue] = Seq(
    IntValue(1, "one", 1),
    IntValue(1, "one", 2),
    IntValue(1, "one", 3),
    IntValue(2, "two", 10),
    IntValue(2, "two", 11),
    IntValue(2, "two", 11),
    IntValue(2, "two", 12),
    IntValue(3, "three", -123),
    IntValue(3, "three", 100),
    IntValue(3, "three", 1024),
    IntValue(4, "four", 0)
  ).toDS()
  val intThresholds = Seq(-200, -100, 0, 100, 200)

  val doubles: Dataset[DoubleValue] = Seq(
    DoubleValue(1, "one", 1.0),
    DoubleValue(1, "one", 2.0),
    DoubleValue(1, "one", 3.0),
    DoubleValue(2, "two", 10.0),
    DoubleValue(2, "two", 11.0),
    DoubleValue(2, "two", 11.0),
    DoubleValue(2, "two", 12.0),
    DoubleValue(3, "three", -123.0),
    DoubleValue(3, "three", 100.0),
    DoubleValue(3, "three", 1024.0),
    DoubleValue(4, "four", 0.0)
  ).toDS()
  val doubleThresholds = Seq(-200.0, -100.0, 0.0, 100.0, 200.0)

  val expectedHistogram = Seq(
    Seq(1, 0, 0, 0, 3, 0, 0),
    Seq(2, 0, 0, 0, 4, 0, 0),
    Seq(3, 0, 1, 0, 1, 0, 1),
    Seq(4, 0, 0, 1, 0, 0, 0)
  )
  val expectedSchema: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("≤-200", LongType, nullable = true),
    StructField("≤-100", LongType, nullable = true),
    StructField("≤0", LongType, nullable = true),
    StructField("≤100", LongType, nullable = true),
    StructField("≤200", LongType, nullable = true),
    StructField(">200", LongType, nullable = true)
  ))
  val expectedSchema2: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("title", StringType, nullable = true),
    StructField("≤-200", LongType, nullable = true),
    StructField("≤-100", LongType, nullable = true),
    StructField("≤0", LongType, nullable = true),
    StructField("≤100", LongType, nullable = true),
    StructField("≤200", LongType, nullable = true),
    StructField(">200", LongType, nullable = true)
  ))
  val expectedDoubleSchema: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("≤-200.0", LongType, nullable = true),
    StructField("≤-100.0", LongType, nullable = true),
    StructField("≤0.0", LongType, nullable = true),
    StructField("≤100.0", LongType, nullable = true),
    StructField("≤200.0", LongType, nullable = true),
    StructField(">200.0", LongType, nullable = true)
  ))

  test("histogram with no aggregate columns") {
    val histogram = ints.histogram(intThresholds, $"value")
    val actual = histogram.collect().toSeq.map(_.toSeq)
    assert(histogram.schema === StructType(expectedSchema.fields.toSeq.filterNot(_.name.equals("id"))))
    assert(actual === Seq(Seq(0, 1, 1, 8, 0, 1)))
  }

  test("histogram with one aggregate column") {
    val histogram = ints.histogram(intThresholds, $"value", $"id")
    val actual = histogram.orderBy($"id").collect().toSeq.map(_.toSeq)
    assert(histogram.schema === expectedSchema)
    assert(actual === expectedHistogram)
  }

  test("histogram with two aggregate columns") {
    val histogram = ints.histogram(intThresholds, $"value", $"id", $"title")
    val actual = histogram.orderBy($"id").collect().toSeq.map(_.toSeq)
    assert(histogram.schema === expectedSchema2)
    assert(actual === Seq(
      Seq(1, "one", 0, 0, 0, 3, 0, 0),
      Seq(2, "two", 0, 0, 0, 4, 0, 0),
      Seq(3, "three", 0, 1, 0, 1, 0, 1),
      Seq(4, "four", 0, 0, 1, 0, 0, 0)
    ))
  }

  test("histogram with int values") {
    val histogram = ints.histogram(intThresholds, $"value", $"id")
    val actual = histogram.orderBy($"id").collect().toSeq.map(_.toSeq)
    assert(histogram.schema === expectedSchema)
    assert(actual === expectedHistogram)
  }

  test("histogram with double values") {
    val histogram = doubles.histogram(doubleThresholds, $"value", $"id")
    val actual = histogram.orderBy($"id").collect().toSeq.map(_.toSeq)
    assert(histogram.schema === expectedDoubleSchema)
    assert(actual === expectedHistogram)
  }

  test("histogram with int values and double thresholds") {
    val histogram = ints.histogram(doubleThresholds, $"value", $"id")
    val actual = histogram.orderBy($"id").collect().toSeq.map(_.toSeq)
    assert(histogram.schema === expectedDoubleSchema)
    assert(actual === expectedHistogram)
  }

  test("histogram with double values and int thresholds") {
    val histogram = doubles.histogram(intThresholds, $"value", $"id")
    val actual = histogram.orderBy($"id").collect().toSeq.map(_.toSeq)
    assert(histogram.schema === expectedSchema)
    assert(actual === expectedHistogram)
  }

  test("histogram with no thresholds") {
    val exception = intercept[IllegalArgumentException] {
      ints.histogram(Seq.empty[Int], $"value", $"id")
    }
    assert(exception.getMessage === "Thresholds must not be empty")
  }

  test("histogram with one threshold") {
    val histogram = ints.histogram(Seq(0), $"value", $"id")
    val actual = histogram.orderBy($"id").collect().toSeq.map(_.toSeq)
    assert(histogram.schema === StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("≤0", LongType, nullable = true),
      StructField(">0", LongType, nullable = true)
    ))
    )
    assert(actual === Seq(
      Seq(1, 0, 3),
      Seq(2, 0, 4),
      Seq(3, 1, 2),
      Seq(4, 1, 0)
    ))
  }

  test("histogram with duplicate thresholds") {
    val exception = intercept[IllegalArgumentException] {
      ints.histogram(Seq(-200, -100, 0, 0, 100, 200), $"value", $"id")
    }
    assert(exception.getMessage === "Thresholds must not contain duplicates: -200,-100,0,0,100,200")
  }

  test("histogram with non-existing value columns") {
    val exception = intercept[AnalysisException] {
      ints.histogram(Seq(0, -200, 100, -100, 200), $"does-not-exist", $"id")
    }
    assert(
      exception.getMessage.startsWith("cannot resolve '`does-not-exist`' given input columns: [id, title, value]") ||
        exception.getMessage.startsWith("Column '`does-not-exist`' does not exist. Did you mean one of the following? [title, id, value]") ||
        exception.getMessage.startsWith("[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `does-not-exist` cannot be resolved. Did you mean one of the following? [`title`, `id`, `value`]")
    )
  }

  test("histogram with non-existing aggregate column") {
    val exception = intercept[AnalysisException] {
      ints.histogram(intThresholds, $"value", $"does-not-exist")
    }
    assert(
      exception.getMessage.startsWith("cannot resolve '`does-not-exist`' given input columns: [") ||
        exception.getMessage.startsWith("Column '`does-not-exist`' does not exist. Did you mean one of the following? [title, id, value, ≤-100, ≤-200, >200, ≤0, ≤100, ≤200]") ||
        exception.getMessage.startsWith("[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `does-not-exist` cannot be resolved. Did you mean one of the following? [`≤-100`, `≤-200`, `>200`, `title`, `≤0`]")
    )
  }

  test("histogram with int values on DataFrame") {
    val histogram = ints.toDF().histogram(intThresholds, $"value", $"id")
    val actual = histogram.orderBy($"id").collect().toSeq.map(_.toSeq)
    assert(histogram.schema === expectedSchema)
    assert(actual === expectedHistogram)
  }

  test("histogram with double values on DataFrame") {
    val histogram = doubles.toDF().histogram(doubleThresholds, $"value", $"id")
    val actual = histogram.orderBy($"id").collect().toSeq.map(_.toSeq)
    assert(histogram.schema === expectedDoubleSchema)
    assert(actual === expectedHistogram)
  }

}
