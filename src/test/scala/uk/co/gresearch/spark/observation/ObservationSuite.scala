/*
 * Copyright 2022 G-Research
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

package uk.co.gresearch.spark.observation

import org.apache.spark.sql.Observation
import org.apache.spark.sql.functions.{abs, avg, col, collect_set, count, lit, max, max_by, min, min_by}
import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.functions.count_null
import uk.co.gresearch.spark.observation.expression.{As, Set}

class ObservationSuite extends AnyFunSuite with SparkTestSession {

  import spark.implicits._

  test("As") {
    val observation = Observation("test")
    spark.range(10).observe(observation, As(count("*"), "count all")).collect()
    observation.get.contains("count all")
    assert(observation.get("count all") === 10)
  }

  test("Collect") {
    val observation = Observation("test")
    spark.range(10).observe(observation, Set(col("id"), "distinct ids")).collect()
    observation.get.contains("distinct ids")
    assert(observation.get("distinct ids") === 0.until(10))
  }

  test("native") {
    val observation = Observation("test")

    spark.range(10).withColumn("val", lit(1) / ($"id" - 5))
      .observe(
        observation,
        count_null("val").as("null-vals"),
        count("val").as("non-null-vals"),
        min("val").as("min-val"),
        max("val").as("max-val"),
        min_by($"id", $"val").as("id-of-min-val"),
        max_by($"id", $"val").as("id-of-max-val"),
        avg($"val").as("avg-val"),
        collect_set(abs($"val")).as("abs-vals")
      ).collect()

    assert(observation.get.keySet === scala.collection.Set("null-vals", "non-null-vals", "min-val", "max-val", "id-of-min-val", "id-of-max-val", "avg-val", "abs-vals"))
    assert(observation.get("null-vals") === 1)
    assert(observation.get("non-null-vals") === 9)
    assert(observation.get("min-val") === -1.0)
    assert(observation.get("max-val") === 1.0)
    assert(observation.get("id-of-min-val") === 4)
    assert(observation.get("id-of-max-val") === 6)
    // 1/5 / 9 is expected, but this is not quite the same as the actual average
    assert(observation.get("avg-val") === (-1.0 / 5 - 1.0 / 4 - 1.0 / 3 - 1.0 / 2 - 1.0 / 1 + 1.0 / 1 + 1.0 / 2 + 1.0 / 3 + 1.0 / 4) / 9)
    assert(observation.get("abs-vals").asInstanceOf[Seq[Double]].toSet === scala.collection.Set(1, 0.5, 1.0 / 3, 0.25, 0.2))
  }
}
