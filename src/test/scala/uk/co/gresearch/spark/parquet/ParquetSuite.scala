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

package uk.co.gresearch.spark.parquet

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.spark.{SparkTestSession, SparkVersion}
import org.apache.spark.sql.functions.{regexp_replace, spark_partition_id}
import org.scalatest.tagobjects.Slow
import uk.co.gresearch._

class ParquetSuite extends AnyFunSuite with SparkTestSession with SparkVersion {

  import spark.implicits._

  // These parquet test files have been created as follows:
  //   import org.apache.spark.sql.SaveMode
  //   spark.sparkContext.hadoopConfiguration.setInt("parquet.block.size", 1024)
  //   spark.range(100).select($"id", rand().as("val")).repartitionByRange(1, $"id").write.parquet("test.parquet")
  //   spark.range(100, 300, 1).select($"id", rand().as("val")).repartitionByRange(1, $"id").write.mode(SaveMode.Append).parquet("test.parquet")eq((3, "three"), (4, "four"), (5, "five"), (6, "six"), (7, "seven")).toDF("id", "value").repartitionByRange(1, $"id").write.mode(SaveMode.Append).parquet("test.parquet")
  val testFile = "src/test/files/test.parquet"


  def assertDf(actual: DataFrame, expected: Seq[Row]): Unit = {
    val replaced =
      actual
        .withColumn("filename", regexp_replace($"filename", ".*/test.parquet/", ""))
        .when(actual.columns.contains("schema"))
        .call(_.withColumn("schema", regexp_replace($"schema", "\n", "\\\\n")))
    assert(replaced.collect() === expected)
  }

  test("read parquet metadata") {
    val createdBy = "parquet-mr version 1.12.2 (build 77e30c8093386ec52c3cfa6c34b7ef3321322c94)"
    val schema = "message spark_schema {\\n  required int64 id;\\n  required double val;\\n}\\n"

    assertDf(
      spark.read
        .parquetMetadata(testFile)
        .orderBy($"filename"),
      Seq(
        Row("file1.parquet", 1, 1652, 1268, 100, createdBy, schema),
        Row("file2.parquet", 2, 3302, 2539, 200, createdBy, schema),
      )
    )
  }

  test("read parquet blocks") {
    assertDf(
      spark.read
        .parquetBlocks(testFile)
        .orderBy($"filename", $"block"),
      Seq(
        Row("file1.parquet", 1, 4, 1652, 1268, 100),
        Row("file2.parquet", 1, 4, 1651, 1269, 100),
        Row("file2.parquet", 2, 1273, 1651, 1270, 100),
      )
    )
  }

  test("read parquet block columns") {
    assertDf(
      spark.read
        .parquetBlockColumns(testFile)
        .orderBy($"filename", $"block", $"column"),
      Seq(
        Row("file1.parquet", 1, "[id]", "SNAPPY", "required int64 id", "[BIT_PACKED, PLAIN]", "0", "99", 4, 826, 437, 100),
        Row("file1.parquet", 1, "[val]", "SNAPPY", "required double val", "[BIT_PACKED, PLAIN]", "0.005067503372006343", "0.9973357672164814", 441, 826, 831, 100),
        Row("file2.parquet", 1, "[id]", "SNAPPY", "required int64 id", "[BIT_PACKED, PLAIN]", "100", "199", 4, 825, 438, 100),
        Row("file2.parquet", 1, "[val]", "SNAPPY", "required double val", "[BIT_PACKED, PLAIN]", "0.010617521596503865", "0.999189783846449", 442, 826, 831, 100),
        Row("file2.parquet", 2, "[id]", "SNAPPY", "required int64 id", "[BIT_PACKED, PLAIN]", "200", "299", 1273, 826, 440, 100),
        Row("file2.parquet", 2, "[val]", "SNAPPY", "required double val", "[BIT_PACKED, PLAIN]", "0.011277044401634018", "0.970525681750662", 1713, 825, 830, 100)
      )
    )
  }

  if (sys.env.get("CI_SLOW_TESTS").exists(_.equals("1"))) {
    Seq(1, 3, 7, 13, 19, 29, 61, 127, 251).foreach { partitionSize =>
      test(s"read parquet partitions ($partitionSize bytes) ${Slow.name}", Slow) {
        withSQLConf("spark.sql.files.maxPartitionBytes" -> partitionSize.toString) {
          val parquet = spark.read.parquet(testFile).cache()

          val rows = spark.read
            .parquet(testFile)
            .mapPartitions(it => Iterator(it.length))
            .select(spark_partition_id().as("partition"), $"value".as("actual_rows"))
          val partitions = spark.read
            .parquetPartitions(testFile)
            .join(rows, Seq("partition"), "left")
            .select($"filename", $"partition", $"start", $"end", $"partitionLength", $"rows", $"actual_rows")

          if (partitions.where($"rows" =!= $"actual_rows" || ($"rows" =!= 0 || $"actual_rows" =!= 0) && $"partitionLength" =!= partitionSize).head(1).nonEmpty) {
            partitions
              .orderBy($"start")
              .where($"rows" =!= 0 || $"actual_rows" =!= 0)
              .show(false)
            fail()
          }

          parquet.unpersist()
        }
      }
    }
  }

  Map(
    None -> Seq(
      Row("file1.parquet", 0, 1930, 1930, 1930, 100),
      Row("file2.parquet", 0, 3493, 3493, 3493, 200),
    ),
    Some(8192) -> Seq(
      Row("file1.parquet", 0, 1930, 1930, 1930, 100),
      Row("file2.parquet", 0, 3493, 3493, 3493, 200),
    ),
    Some(1024) -> Seq(
      Row("file1.parquet", 0, 1024, 1024, 1930, 100),
      Row("file1.parquet", 1024, 1930, 906, 1930, 0),
      Row("file2.parquet", 0, 1024, 1024, 3493, 100),
      Row("file2.parquet", 1024, 2048, 1024, 3493, 100),
      Row("file2.parquet", 2048, 3072, 1024, 3493, 0),
      Row("file2.parquet", 3072, 3493, 421, 3493, 0),
    ),
    Some(512) -> Seq(
      Row("file1.parquet", 0, 512, 512, 1930, 0),
      Row("file1.parquet", 512, 1024, 512, 1930, 100),
      Row("file1.parquet", 1024, 1536, 512, 1930, 0),
      Row("file1.parquet", 1536, 1930, 394, 1930, 0),
      Row("file2.parquet", 0, 512, 512, 3493, 0),
      Row("file2.parquet", 512, 1024, 512, 3493, 100),
      Row("file2.parquet", 1024, 1536, 512, 3493, 0),
      Row("file2.parquet", 1536, 2048, 512, 3493, 100),
      Row("file2.parquet", 2048, 2560, 512, 3493, 0),
      Row("file2.parquet", 2560, 3072, 512, 3493, 0),
      Row("file2.parquet", 3072, 3493, 421, 3493, 0),
    ),
  ).foreach { case (partitionSize, expectedRows) =>
    test(s"read parquet partitions (${partitionSize.getOrElse("default")} bytes)") {
      withSQLConf(partitionSize.map(size => Seq("spark.sql.files.maxPartitionBytes" -> size.toString)).getOrElse(Seq.empty): _*) {
        val expected =
          if (SparkCompatMajorVersion > 3 || SparkCompatMinorVersion >= 3) {
            expectedRows
          } else {
            expectedRows.map(row => Row(row.getString(0), row.getInt(1), row.getInt(2), row.getInt(3), null, row.getInt(5)))
          }

        val actual = spark.read
          .parquetPartitions(testFile)
          .orderBy($"filename", $"start")
          .cache()

        val partitions = actual.select($"partition").as[Int].collect()
        if (partitionSize.isDefined) {
          assert(partitions.indices === partitions.sorted)
        } else {
          assert(Seq(0, 0) === partitions)
        }

        assertDf(actual.drop("partition"), expected)
        actual.unpersist()
      }
    }
  }
}
