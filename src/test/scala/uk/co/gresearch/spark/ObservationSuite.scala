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

import org.apache.spark.sql.functions.{count, lit, mean, sum}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.FunSuite

import java.util.UUID
import java.util.concurrent.TimeUnit

class ObservationSuite extends FunSuite with SparkTestSession {

  implicit val s: SparkSession = spark

  import spark.implicits._

  val df: Dataset[Row] = Seq((1, "a"), (2, "b"), (4, "c"), (8, "d"))
    .toDF("id", "value")
    .repartition(10, $"id")
    .cache

  test("Dataset.observe(Observation)") {
    doTestObservation((df: DataFrame, observation: Observation) => df.observe(observation))
  }

  test("Observation.observe(Dataset)") {
    doTestObservation((df: DataFrame, observation: Observation) => observation.observe(df))
  }

  def assertTime(assertion: Long => Boolean)(function: => Any): Unit = {
    val start = System.nanoTime()
    function
    assert(assertion(System.nanoTime() - start))
  }

  def assertEmptyObservation(observation: Observation): Unit = {
    assertTime(_ >= 250000000L) {
      assert(!observation.waitCompleted(250, TimeUnit.MILLISECONDS))
    }
    assertTime(_ >= 250000000L) {
      assert(observation.option(250, TimeUnit.MILLISECONDS).isEmpty)
    }
    assertTime(_ >= 250000000L) {
      assertThrows[NoSuchElementException] { observation.get(250, TimeUnit.MILLISECONDS) }
    }
  }

  def assertObservation(observation: Observation, count: Long): Unit = {
    assert(observation.waitCompleted(250, TimeUnit.MILLISECONDS))
    assert(observation.waitCompleted())

    assert(observation.option(100, TimeUnit.MILLISECONDS).isDefined)
    assert(observation.option.isDefined)
    assert(observation.option.get === Row(4, 15, 3.75))

    assert(observation.get(100, TimeUnit.MILLISECONDS) === Row(4, 15, 3.75))
    assert(observation.get === Row(4, 15, 3.75))

    assert(count === 4)
  }

  def doTestObservation(observe: (DataFrame, Observation) => DataFrame): Unit = {
    // observation names need to be unique here so that subsequent tests do not interfere
    val observation = Observation(UUID.randomUUID().toString, count(lit(1)), sum($"id"), mean($"id"))
    assertEmptyObservation(observation)

    val observed = observe(df, observation)
    assertEmptyObservation(observation)

    val cnt = observed.count()
    assertObservation(observation, cnt)

    observation.reset()
    assertEmptyObservation(observation)

    val cnt2 = observed.count()
    assertObservation(observation, cnt2)

    observation.reset()
    assertEmptyObservation(observation)

    val list = observed.collect()
    assertObservation(observation, list.length)

    observation.reset()
    assertEmptyObservation(observation)

    val list2 = observe(df.orderBy("id"), observation).collect()
    assertObservation(observation, list2.length)

    // this does not guarantee that I see the third observation or still the second
    // but sufficient for testing this does produce either result
    val cnt3 = observed.count()
    assertObservation(observation, cnt3)

    observation.close()
    assertObservation(observation, cnt3)

    // evaluating the observe dataset after closing the observation still works
    observed.collect()
  }

  test("observe without name") {
    val observation = Observation(count(lit(1)))
    df.observe(observation).collect()
    assert(observation.get(100, TimeUnit.MILLISECONDS).getLong(0) === 4)
  }

  test("observation with concurrent action") {
    val observation = Observation("concurrent action", count(lit(1)), sum($"id"), mean($"id"))
    val observed = df.observe(observation)
    assertEmptyObservation(observation)

    df.collect()
    assertEmptyObservation(observation)

    val cnt = observed.count()
    assertObservation(observation, cnt)
  }

}
