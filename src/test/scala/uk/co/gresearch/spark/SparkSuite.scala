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

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.FunSuite
import uk.co.gresearch.spark.SparkSuite.Value

import scala.reflect.ClassTag

class SparkSuite extends FunSuite with SparkTestSession {

  import spark.implicits._

  test("backticks") {
    assert(backticks("column") === "column")
    assert(backticks("a.column") === "`a.column`")
    assert(backticks("`a.column`") === "`a.column`")
    assert(backticks("column", "a.field") === "column.`a.field`")
    assert(backticks("a.column", "a.field") === "`a.column`.`a.field`")
    assert(backticks("the.alias", "a.column", "a.field") === "`the.alias`.`a.column`.`a.field`")
  }


  def assertIsDataFrame[T : ClassTag](actual: Dataset[T]): Unit = {
    val clsTag = implicitly[ClassTag[T]]
    assert(clsTag === ClassTag(classOf[Row]))
  }

  test("call dataset-to-dataset transformation") {
    assertIsDataFrame(spark.emptyDataset[Value].call(_.sort()))
  }

  test("call dataset-to-dataframe transformation") {
    assertIsDataFrame(spark.emptyDataset[Value].call(_.drop("string")))
  }

  test("call dataframe-to-dataset transformation") {
    assertIsDataFrame(spark.createDataFrame(Seq.empty[Value]).call(_.as[Value]))
  }

  test("call dataframe-to-dataframe transformation") {
    assertIsDataFrame(spark.createDataFrame(Seq.empty[Value]).call(_.drop("string")))
  }


  test("when true call dataset-to-dataset transformation") {
    assertIsDataFrame(spark.emptyDataset[Value].when(true).call(_.sort()))
  }

  test("when true call dataset-to-dataframe transformation") {
    assertIsDataFrame(spark.emptyDataset[Value].when(true).call(_.drop("string")))
  }

  test("when true call dataframe-to-dataset transformation") {
    assertIsDataFrame(spark.createDataFrame(Seq.empty[Value]).when(true).call(_.as[Value]))
  }

  test("when true call dataframe-to-dataframe transformation") {
    assertIsDataFrame(spark.createDataFrame(Seq.empty[Value]).when(true).call(_.drop("string")))
  }


  test("when false call implicit dataset-to-dataset transformation") {
    assertIsDataFrame(spark.emptyDataset[Value].when(false).call(_.sort()))
  }

  test("when false call explicit dataset-to-dataframe transformation") {
    assertIsDataFrame(spark.emptyDataset[Value].when(false).call(_.drop("string")))
  }

  test("when false call explicit dataframe-to-dataset transformation") {
    assertIsDataFrame(spark.createDataFrame(Seq.empty[Value]).when(false).call(_.as[Value]))
  }

  test("when false call implicit dataframe-to-dataframe transformation") {
    assertIsDataFrame(spark.createDataFrame(Seq.empty[Value]).when(false).call(_.drop("string")))
  }

}

object SparkSuite {
  case class Value(id: Int, string: String)
}
