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

import org.apache.spark.sql.functions.{count, lit, sum}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FunSuite

class ObservationSuite extends FunSuite with SparkTestSession {

  implicit val s: SparkSession = spark

  import spark.implicits._

  test("observation") {
    val df = Seq((1, "a"), (2, "b"), (4, "c"), (8, "d")).toDF("id", "value")
    val observation = Observation("stats", count(lit(1)), sum($"id"))
    assert(observation.option.isEmpty === true)
    assert(observation.option.isDefined === false)

    val cnt = df.repartition(10, $"id").cache.observe(observation).count()

    assert(observation.waitAndGet === Row(4, 15))
    assert(observation.option.isEmpty === false)
    assert(observation.option.isDefined === true)
    assert(observation.get === Row(4, 15))
    assert(cnt === 4)
  }

}
