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

import org.apache.spark.sql._
import org.scalatest.FunSuite
import uk.co.gresearch.ExtendedAny
import uk.co.gresearch.spark.SparkSuite.Value

class SparkSuite extends FunSuite with SparkTestSession {

  import spark.implicits._

  val emptyDataset: Dataset[Value] = spark.emptyDataset[Value]
  val emptyDataFrame: DataFrame = spark.createDataFrame(Seq.empty[Value])

  test("backticks") {
    assert(backticks("column") === "column")
    assert(backticks("a.column") === "`a.column`")
    assert(backticks("`a.column`") === "`a.column`")
    assert(backticks("column", "a.field") === "column.`a.field`")
    assert(backticks("a.column", "a.field") === "`a.column`.`a.field`")
    assert(backticks("the.alias", "a.column", "a.field") === "`the.alias`.`a.column`.`a.field`")
  }

  def assertIsDataset[T](actual: Dataset[T]): Unit = {
    // if calling class compiles, we assert success
    // further we evaluate the dataset to see this works as well
    actual.collect()
  }

  def assertIsGenericType[T](actual: T): Unit = {
    // if calling class compiles, we assert success
  }

  test("call dataset-to-dataset transformation") {
    assertIsDataset[Value](spark.emptyDataset[Value].transform(_.sort()))
    assertIsDataset[Value](spark.emptyDataset[Value].call(_.sort()))
  }

  test("call dataset-to-dataframe transformation") {
    assertIsDataset[Row](spark.emptyDataset[Value].transform(_.drop("string")))
    assertIsDataset[Row](spark.emptyDataset[Value].call(_.drop("string")))
  }

  test("call dataframe-to-dataset transformation") {
    assertIsDataset[Value](spark.createDataFrame(Seq.empty[Value]).transform(_.as[Value]))
    assertIsDataset[Value](spark.createDataFrame(Seq.empty[Value]).call(_.as[Value]))
  }

  test("call dataframe-to-dataframe transformation") {
    assertIsDataset[Row](spark.createDataFrame(Seq.empty[Value]).transform(_.drop("string")))
    assertIsDataset[Value](spark.createDataFrame(Seq.empty[Value]).call(_.as[Value]))
  }


  Seq(true, false).foreach { condition =>
    test(s"call on $condition condition dataset-to-dataset transformation") {
      assertIsGenericType[Dataset[Value]](
        emptyDataset.transform(_.on(condition).call(_.sort()))
      )
      assertIsGenericType[Dataset[Value]](
        emptyDataset.on(condition).call(_.sort())
      )
    }

    test(s"call on $condition condition dataframe-to-dataframe transformation") {
      assertIsGenericType[DataFrame](
        emptyDataFrame.transform(_.on(condition).call(_.drop("string")))
      )
      assertIsGenericType[DataFrame](
        emptyDataFrame.on(condition).call(_.drop("string"))
      )
    }

    test(s"when $condition call dataset-to-dataset transformation") {
      assertIsDataset[Value](
        emptyDataset.transform(_.when(condition).call(_.sort()))
      )
      assertIsDataset[Value](
        emptyDataset.when(condition).call(_.sort())
      )
    }

    test(s"when $condition call dataframe-to-dataframe transformation") {
      assertIsDataset[Row](
        emptyDataFrame.transform(_.when(condition).call(_.drop("string")))
      )
      assertIsDataset[Row](
        emptyDataFrame.when(condition).call(_.drop("string"))
      )
    }


    test(s"call on $condition condition either dataset-to-dataset transformation") {
      assertIsGenericType[Dataset[Value]](
        spark.emptyDataset[Value]
          .transform(
            _.on(condition)
              .either(_.sort())
              .or(_.orderBy())
          )
      )
    }

    test(s"call on $condition condition either dataset-to-dataframe transformation") {
      assertIsGenericType[DataFrame](
        spark.emptyDataset[Value]
          .transform(
            _.on(condition)
              .either(_.drop("string"))
              .or(_.withColumnRenamed("string", "value"))
          )
      )
    }

    test(s"call on $condition condition either dataframe-to-dataset transformation") {
      assertIsGenericType[Dataset[Value]](
        spark.createDataFrame(Seq.empty[Value])
          .transform(
            _.on(condition)
              .either(_.as[Value])
              .or(_.as[Value])
          )
      )
    }

    test(s"call on $condition condition either dataframe-to-dataframe transformation") {
      assertIsGenericType[DataFrame](
        spark.createDataFrame(Seq.empty[Value])
          .transform(
            _.on(condition)
              .either(_.drop("string"))
              .or(_.withColumnRenamed("string", "value"))
          )
      )
    }
  }


  test("on true condition call either writer-to-writer methods") {
    assertIsGenericType[DataFrameWriter[Value]](
      spark
        .emptyDataset[Value]
        .write
        .on(true)
        .either(_.partitionBy("id"))
        .or(_.bucketBy(10, "id"))
        .mode(SaveMode.Overwrite)
    )
  }

  test("on false condition call either writer-to-writer methods") {
    assertIsGenericType[DataFrameWriter[Value]](
      spark
        .emptyDataset[Value]
        .write
        .on(false)
        .either(_.partitionBy("id"))
        .or(_.bucketBy(10, "id"))
        .mode(SaveMode.Overwrite)
    )
  }

  test("on true condition call either writer-to-unit methods") {
    withTempPath { dir =>
      assertIsGenericType[Unit](
        spark
          .emptyDataset[Value]
          .write
          .on(true)
          .either(_.csv(dir.getAbsolutePath))
          .or(_.csv(dir.getAbsolutePath))
      )
    }
  }

  test("on false condition call either writer-to-unit methods") {
    withTempPath { dir =>
      assertIsGenericType[Unit](
        spark
          .emptyDataset[Value]
          .write
          .on(false)
          .either(_.csv(dir.getAbsolutePath))
          .or(_.csv(dir.getAbsolutePath))
      )
    }
  }

}

object SparkSuite {
  case class Value(id: Int, string: String)
}
