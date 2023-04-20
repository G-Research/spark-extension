/*
 * Copyright 2023 G-Research
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

import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.spark.SparkTestSession

import java.io.File


class AppSuite extends AnyFunSuite with SparkTestSession {
  import spark.implicits._

  test("run app with file and hive table") {
    withTempPath { path =>
      // write left dataframe as csv
      val leftPath = new File(path, "left.csv").getAbsolutePath
      DiffSuite.left(spark).write.csv(leftPath)

      // write right dataframe as parquet table
      DiffSuite.right(spark).write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("right_parquet")

      // launch app
      val jsonPath = new File(path, "diff.json").getAbsolutePath
      App.main(Array(
        "--left-format", "csv",
        "--left-schema", "id int, value string",
        "--output-format", "json",
        "--id", "id",
        leftPath,
        "right_parquet",
        jsonPath
      ))

      // assert written diff
      val actual = spark.read.json(jsonPath)
      assert(actual.orderBy($"id").collect() === DiffSuite.expectedDiff)
    }
  }
}
