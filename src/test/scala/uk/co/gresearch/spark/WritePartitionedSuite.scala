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

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, reverse}
import org.scalatest.FunSuite
import uk.co.gresearch.spark.WritePartitionedSuite.Value

import java.io.File
import java.sql.Date
import scala.io.Source

class WritePartitionedSuite extends FunSuite with SparkTestSession {

  import spark.implicits._

  val values: Dataset[Value] = Seq(
    Value(1, Date.valueOf("2020-07-01"), "one"),
    Value(1, Date.valueOf("2020-07-02"), "One"),
    Value(1, Date.valueOf("2020-07-03"), "ONE"),
    Value(1, Date.valueOf("2020-07-04"), "one"),
    Value(2, Date.valueOf("2020-07-01"), "two"),
    Value(2, Date.valueOf("2020-07-02"), "Two"),
    Value(2, Date.valueOf("2020-07-03"), "TWO"),
    Value(2, Date.valueOf("2020-07-04"), "two"),
    Value(3, Date.valueOf("2020-07-01"), "three"),
    Value(4, Date.valueOf("2020-07-01"), "four")
  ).toDS()

  test("write with one partition column") {
    withTempPath { dir =>
      values.writePartitionedBy(Seq($"id")).csv(dir.getAbsolutePath)
      val partitions = dir.list().filter(_.startsWith("id=")).sorted
      assert(partitions === Seq("id=1", "id=2", "id=3", "id=4"))
      partitions.foreach { partition =>
        val files = new File(dir, partition).list().filter(file => file.startsWith("part-") && file.endsWith(".csv"))
        assert(files.length === 1)
      }
    }
  }

  test("write with two partition column") {
    withTempPath { dir =>
      values.writePartitionedBy(Seq($"id", $"date")).csv(dir.getAbsolutePath)
      val ids = dir.list().filter(_.startsWith("id=")).sorted
      assert(ids === Seq("id=1", "id=2", "id=3", "id=4"))
      val dates = ids.flatMap { id =>
        new File(dir, id).list().filter(file => file.startsWith("date="))
      }.toSet
      assert(dates === Set("date=2020-07-01", "date=2020-07-04", "date=2020-07-02", "date=2020-07-03"))
    }
  }

  test("write with more partition columns") {
    withTempPath { dir =>
      values.writePartitionedBy(Seq($"id"), Seq($"date")).csv(dir.getAbsolutePath)
      val partitions = dir.list().filter(_.startsWith("id=")).sorted
      assert(partitions === Seq("id=1", "id=2", "id=3", "id=4"))
      partitions.foreach { partition =>
        val files = new File(dir, partition).list().filter(file => file.startsWith("part-") && file.endsWith(".csv"))
        files.foreach(println)
        assert(files.length >= 1 && files.length <= 2)
      }
    }
  }

  test("write with one partition") {
    withTempPath { dir =>
      values.writePartitionedBy(Seq($"id"), Seq($"date"), partitions=Some(1)).csv(dir.getAbsolutePath)
      val partitions = dir.list().filter(_.startsWith("id=")).sorted
      assert(partitions === Seq("id=1", "id=2", "id=3", "id=4"))
      val files = partitions.flatMap { partition =>
        new File(dir, partition).list().filter(file => file.startsWith("part-") && file.endsWith(".csv"))
      }
      assert(files.toSet.size === 1)
    }
  }

  test("write with partition order") {
    withTempPath { dir =>
      values.writePartitionedBy(Seq($"id"), Seq.empty, Seq($"date")).csv(dir.getAbsolutePath)
      val partitions = dir.list().filter(_.startsWith("id=")).sorted
      assert(partitions === Seq("id=1", "id=2", "id=3", "id=4"))
      partitions.foreach { partition =>
        val file = new File(dir, partition)
        val files = file.list().filter(file => file.startsWith("part-") && file.endsWith(".csv"))
        assert(files.length === 1)

        val source = Source.fromFile(new File(file, files(0)))
        val lines = try source.getLines().toList finally source.close()
        partition match {
          case "id=1" => assert(lines === Seq(
            "2020-07-01,one",
            "2020-07-02,One",
            "2020-07-03,ONE",
            "2020-07-04,one"
          ))
          case "id=2" => assert(lines === Seq(
            "2020-07-01,two",
            "2020-07-02,Two",
            "2020-07-03,TWO",
            "2020-07-04,two"
          ))
          case "id=3" => assert(lines === Seq(
            "2020-07-01,three"
          ))
          case "id=4" => assert(lines === Seq(
            "2020-07-01,four"
          ))
        }
      }
    }
  }

  test("write with desc partition order") {
    withTempPath { dir =>
      values.writePartitionedBy(Seq($"id"), Seq.empty, Seq($"date".desc)).csv(dir.getAbsolutePath)
      val partitions = dir.list().filter(_.startsWith("id=")).sorted
      assert(partitions === Seq("id=1", "id=2", "id=3", "id=4"))
      partitions.foreach { partition =>
        val file = new File(dir, partition)
        val files = file.list().filter(file => file.startsWith("part-") && file.endsWith(".csv"))
        assert(files.length === 1)

        val source = Source.fromFile(new File(file, files(0)))
        val lines = try source.getLines().toList finally source.close()
        partition match {
          case "id=1" => assert(lines === Seq(
            "2020-07-04,one",
            "2020-07-03,ONE",
            "2020-07-02,One",
            "2020-07-01,one"
          ))
          case "id=2" => assert(lines === Seq(
            "2020-07-04,two",
            "2020-07-03,TWO",
            "2020-07-02,Two",
            "2020-07-01,two"
          ))
          case "id=3" => assert(lines === Seq(
            "2020-07-01,three"
          ))
          case "id=4" => assert(lines === Seq(
            "2020-07-01,four"
          ))
        }
      }
    }
  }

  test("write with write projection") {
    val projection = Some(Seq(col("id"), reverse(col("value"))))
    withTempPath { path =>
      values.writePartitionedBy(Seq($"id"), Seq.empty, Seq($"date"), writtenProjection = projection).csv(path.getAbsolutePath)
      val partitions = path.list().filter(_.startsWith("id=")).sorted
      assert(partitions === Seq("id=1", "id=2", "id=3", "id=4"))
      partitions.foreach { partition =>
        val dir = new File(path, partition)
        val files = dir.list().filter(file => file.startsWith("part-") && file.endsWith(".csv"))
        assert(files.length === 1)

        val lines = files.flatMap { file =>
          val source = Source.fromFile(new File(dir, file))
          try source.getLines().toList finally source.close()
        }

        partition match {
          case "id=1" => assert(lines === Seq("eno", "enO", "ENO", "eno"))
          case "id=2" => assert(lines === Seq("owt", "owT", "OWT", "owt"))
          case "id=3" => assert(lines === Seq("eerht"))
          case "id=4" => assert(lines === Seq("ruof"))
        }
      }
    }
  }

}

object WritePartitionedSuite {
  case class Value(id: Int, date: Date, value: String)
}
