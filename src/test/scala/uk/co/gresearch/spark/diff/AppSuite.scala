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
      App.main(
        Array(
          "--left-format",
          "csv",
          "--left-schema",
          "id int, value string",
          "--output-format",
          "json",
          "--id",
          "id",
          leftPath,
          "right_parquet",
          jsonPath
        )
      )

      // assert written diff
      val actual = spark.read.json(jsonPath)
      assert(actual.orderBy($"id").collect() === DiffSuite.expectedDiff)
    }
  }

  Seq(Set("I"), Set("C"), Set("D"), Set("N"), Set("I", "C", "D")).foreach { filter =>
    test(s"run app with filter ${filter.mkString("[", ",", "]")}") {
      withTempPath { path =>
        // write left dataframe as parquet
        val leftPath = new File(path, "left.parquet").getAbsolutePath
        DiffSuite.left(spark).write.parquet(leftPath)

        // write right dataframe as csv
        val rightPath = new File(path, "right.parquet").getAbsolutePath
        DiffSuite.right(spark).write.parquet(rightPath)

        // launch app
        val outputPath = new File(path, "diff.parquet").getAbsolutePath
        App.main(
          Array(
            "--format",
            "parquet",
            "--id",
            "id",
          ) ++ filter.toSeq.flatMap(f => Array("--filter", f)) ++ Array(
            leftPath,
            rightPath,
            outputPath
          )
        )

        // assert written diff
        val actual = spark.read.parquet(outputPath).orderBy($"id").collect()
        val expected = DiffSuite.expectedDiff.filter(row => filter.contains(row.getString(0)))
        assert(actual === expected)
        assert(expected.nonEmpty)
      }
    }
  }

  test(s"run app with unknown filter") {
    withTempPath { path =>
      // write left dataframe as parquet
      val leftPath = new File(path, "left.parquet").getAbsolutePath
      DiffSuite.left(spark).write.parquet(leftPath)

      // write right dataframe as csv
      val rightPath = new File(path, "right.parquet").getAbsolutePath
      DiffSuite.right(spark).write.parquet(rightPath)

      // launch app
      val outputPath = new File(path, "diff.parquet").getAbsolutePath
      assertThrows[RuntimeException](
        App.main(
          Array(
            "--format",
            "parquet",
            "--id",
            "id",
            "--filter",
            "A",
            leftPath,
            rightPath,
            outputPath
          )
        )
      )
    }
  }

  test("run app writing stats") {
    withTempPath { path =>
      // write left dataframe as parquet
      val leftPath = new File(path, "left.parquet").getAbsolutePath
      DiffSuite.left(spark).write.parquet(leftPath)

      // write right dataframe as csv
      val rightPath = new File(path, "right.parquet").getAbsolutePath
      DiffSuite.right(spark).write.parquet(rightPath)

      // launch app
      val outputPath = new File(path, "diff.parquet").getAbsolutePath
      App.main(
        Array(
          "--format",
          "parquet",
          "--statistics",
          "--id",
          "id",
          leftPath,
          rightPath,
          outputPath
        )
      )

      // assert written diff
      val actual = spark.read.parquet(outputPath).as[(String, Long)].collect().toMap
      val expected = DiffSuite.expectedDiff.groupBy(row => row.getString(0)).mapValues(_.length).toMap
      assert(actual === expected)
    }
  }
}
