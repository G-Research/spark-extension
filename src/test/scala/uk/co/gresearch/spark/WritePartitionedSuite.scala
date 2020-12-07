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

import java.sql.Timestamp

import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

case class Value(id: Int, ts: Timestamp, value: String)

class WritePartitionedSuite extends FunSuite with SparkTestSession {

  import spark.implicits._

  val values: Dataset[Value] = Seq(
    Value(1, Timestamp.valueOf("2020-07-01 00:00:00"), "one"),
    Value(1, Timestamp.valueOf("2020-07-02 00:00:00"), "One"),
    Value(1, Timestamp.valueOf("2020-07-03 00:00:00"), "ONE"),
    Value(1, Timestamp.valueOf("2020-07-04 00:00:00"), "one"),
    Value(2, Timestamp.valueOf("2020-07-01 00:00:00"), "two"),
    Value(2, Timestamp.valueOf("2020-07-02 00:00:00"), "Two"),
    Value(2, Timestamp.valueOf("2020-07-03 00:00:00"), "TWO"),
    Value(2, Timestamp.valueOf("2020-07-04 00:00:00"), "two"),
    Value(3, Timestamp.valueOf("2020-07-01 00:00:00"), "three"),
    Value(4, Timestamp.valueOf("2020-07-01 00:00:00"), "four")
  ).toDS()

  test("write with one partition column") {
    withTempPath { dir =>
      values.writePartitionedBy(Seq($"id")).csv(dir.getAbsolutePath)
      dir.list().foreach(println)
    }
  }

}
