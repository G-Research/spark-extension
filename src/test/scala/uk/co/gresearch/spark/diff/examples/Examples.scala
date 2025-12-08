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

package uk.co.gresearch.spark.diff.examples

import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.diff.{DatasetDiff, DiffMode, DiffOptions}
import uk.co.gresearch.test.Suite

case class Value(id: Int, value: Option[String], label: Option[String])

class Examples extends Suite with SparkTestSession {

  test("issue") {
    import spark.implicits._
    val originalDF =
      Seq((1, "gaurav", "jaipur", 550, 70000), (2, "sunil", "noida", 600, 80000), (3, "rishi", "ahmedabad", 510, 65000))
        .toDF("id", "name", "city", "credit_score", "credit_limit")
    val changedDF =
      Seq((1, "gaurav", "jaipur", 550, 70000), (2, "sunil", "noida", 650, 90000), (4, "Joshua", "cochin", 612, 85000))
        .toDF("id", "name", "city", "credit_score", "credit_limit")
    val options = DiffOptions.default.withChangeColumn("changes")
    val diff = originalDF.diff(changedDF, options, "id")
    diff.show(false)
  }

  test("examples") {
    import spark.implicits._

    val left = Seq(
      Value(1, Some("one"), None),
      Value(2, Some("two"), Some("number two")),
      Value(3, Some("three"), Some("number three")),
      Value(4, Some("four"), Some("number four")),
      Value(5, Some("five"), Some("number five"))
    ).toDS

    val right = Seq(
      Value(1, Some("one"), Some("one")),
      Value(2, Some("Two"), Some("number two")),
      Value(3, Some("Three"), Some("number Three")),
      Value(4, Some("four"), Some("number four")),
      Value(6, Some("six"), Some("number six"))
    ).toDS

    {
      Seq(DiffMode.ColumnByColumn, DiffMode.SideBySide, DiffMode.LeftSide, DiffMode.RightSide).foreach { mode =>
        Seq(false, true).foreach { sparse =>
          val options = DiffOptions.default.withDiffMode(mode)
          left.diff(right, options, "id").orderBy("id").show(false)
        }
      }
    }
  }

}
