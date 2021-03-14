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

  def assertEmptyObservation(observation: Observation): Unit = {
    assert(observation.option.isEmpty === true)
    assert(observation.option.isDefined === false)
    assertThrows[NoSuchElementException] { observation.get }
    val start = System.nanoTime()
    assert(!observation.waitCompleted(250, TimeUnit.MILLISECONDS))
    assert(System.nanoTime() - start >= 250000000L)
    assert(observation.option.isEmpty === true)
    assert(observation.option.isDefined === false)
    assertThrows[NoSuchElementException] { observation.get }
  }

  def assertObservation(observation: Observation, count: Long): Unit = {
    assert(observation.waitCompleted(250, TimeUnit.SECONDS))
    assert(observation.waitAndGet === Row(4, 15, 3.75))
    assert(observation.option.isEmpty === false)
    assert(observation.option.isDefined === true)
    assert(observation.get === Row(4, 15, 3.75))
    assert(count === 4)
  }

  def doTestObservation(observe: (DataFrame, Observation) => DataFrame): Unit = {
    val observation = Observation("observation", count(lit(1)), sum($"id"), mean($"id"))
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
  }

  test("observation with concurrent action") {
    val observation = Observation("observation", count(lit(1)), sum($"id"), mean($"id"))
    val observed = df.observe(observation)
    assertEmptyObservation(observation)

    df.collect()
    assertEmptyObservation(observation)

    val cnt = observed.count()
    assertObservation(observation, cnt)
  }

}
