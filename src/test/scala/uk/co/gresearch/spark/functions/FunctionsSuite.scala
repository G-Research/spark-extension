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

package uk.co.gresearch.spark.functions

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.spark.SparkTestSession

class FunctionsSuite extends AnyFunSuite with SparkTestSession {

  import spark.implicits._

  test("Function count_null(String)") {
    val df = Seq((1, "one"), (2, null), (3, "three"), (4, null), (5, "five")).toDF("id", "val")
    val actual = df.select(count_null("val"))
    assert(actual.schema === StructType(Seq(StructField("count_null(val)", LongType, nullable = false))))
    assert(actual.as[Long].collect() === Seq(2))
  }

  test("Function count_null(Column)") {
    val df = Seq((1, "one"), (2, null), (3, "three"), (4, null), (5, "five")).toDF("id", "val")
    val actual = df.select(count_null($"val"))
    assert(actual.schema === StructType(Seq(StructField("count_null(val)", LongType, nullable = false))))
    assert(actual.as[Long].collect() === Seq(2))
  }

}
