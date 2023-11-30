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

import org.apache.spark.sql.Row.unapplySeq
import org.apache.spark.sql.functions.{regexp_replace, spark_partition_id}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.tagobjects.Slow
import uk.co.gresearch._
import uk.co.gresearch.spark.{SparkTestSession, SparkVersion}

class ParquetSuite extends AnyFunSuite with SparkTestSession with SparkVersion {

  import spark.implicits._

  // These parquet test files have been created as follows:
  //   import org.apache.spark.sql.SaveMode
  //   spark.sparkContext.hadoopConfiguration.setInt("parquet.block.size", 1024)
  //   spark.range(100).select($"id", rand().as("val")).repartitionByRange(1, $"id").write.parquet("test.parquet")
  //   spark.range(100, 300, 1).select($"id", rand().as("val")).repartitionByRange(1, $"id").write.mode(SaveMode.Append).parquet("test.parquet")eq((3, "three"), (4, "four"), (5, "five"), (6, "six"), (7, "seven")).toDF("id", "value").repartitionByRange(1, $"id").write.mode(SaveMode.Append).parquet("test.parquet")
  val testFile = "src/test/files/test.parquet"
  val nestedFile = "src/test/files/nested.parquet"

  val parallelisms = Seq(None, Some(1), Some(2), Some(8))

  def assertDf(
      actual: DataFrame,
      order: Seq[Column],
      expectedSchema: StructType,
      expectedRows: Seq[Row],
      expectedParallelism: Option[Int],
      postProcess: DataFrame => DataFrame = identity
  ): Unit = {
    assert(actual.schema === expectedSchema)

    if (expectedParallelism.isDefined) {
      assert(actual.rdd.getNumPartitions === expectedParallelism.get)
    } else {
      assert(actual.rdd.getNumPartitions === actual.sparkSession.sparkContext.defaultParallelism)
    }

    val replaced =
      actual
        .orderBy(order: _*)
        .withColumn(
          "filename",
          regexp_replace(regexp_replace($"filename", ".*/test.parquet/", ""), ".*/nested.parquet", "nested.parquet")
        )
        .when(actual.columns.contains("schema"))
        .call(_.withColumn("schema", regexp_replace($"schema", "\n", "\\\\n")))
        .call(postProcess)
    assert(replaced.collect() === expectedRows)
  }

  val hasEncryption: Boolean = SparkMajorVersion > 3 || SparkMinorVersion > 4
  val UNENCRYPTED: String = if (hasEncryption) "UNENCRYPTED" else null

  parallelisms.foreach { parallelism =>
    test(s"read parquet metadata (parallelism=${parallelism.map(_.toString).getOrElse("None")})") {
      val createdBy = "parquet-mr version 1.12.2 (build 77e30c8093386ec52c3cfa6c34b7ef3321322c94)"
      val schema = "message spark_schema {\\n  required int64 id;\\n  required double val;\\n}\\n"
      val keyValues = Map(
        "org.apache.spark.version" -> "3.3.0",
        "org.apache.spark.sql.parquet.row.metadata" -> """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"val","type":"double","nullable":false,"metadata":{}}]}"""
      )

      assertDf(
        spark.read
          .when(parallelism.isDefined)
          .either(_.parquetMetadata(parallelism.get, testFile))
          .or(_.parquetMetadata(testFile)),
        Seq($"filename"),
        StructType(
          Seq(
            StructField("filename", StringType, nullable = true),
            StructField("blocks", IntegerType, nullable = false),
            StructField("compressedBytes", LongType, nullable = false),
            StructField("uncompressedBytes", LongType, nullable = false),
            StructField("rows", LongType, nullable = false),
            StructField("columns", IntegerType, nullable = false),
            StructField("values", LongType, nullable = false),
            StructField("nulls", LongType, nullable = true),
            StructField("createdBy", StringType, nullable = true),
            StructField("schema", StringType, nullable = true),
            StructField("encryption", StringType, nullable = true),
            StructField("keyValues", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
          )
        ),
        Seq(
          Row("file1.parquet", 1, 1268, 1652, 100, 2, 200, 0, createdBy, schema, UNENCRYPTED, keyValues),
          Row("file2.parquet", 2, 2539, 3302, 200, 2, 400, 0, createdBy, schema, UNENCRYPTED, keyValues),
        ),
        parallelism
      )
    }
  }

  val hasLogicalAnnotation: Boolean = SparkMajorVersion > 3 || SparkMinorVersion > 1
  val TIMESTAMP: String = if (hasLogicalAnnotation) "TIMESTAMP(MICROS,true)" else null
  val STRING: String = if (hasLogicalAnnotation) "STRING" else null

  parallelisms.foreach { parallelism =>
    test(s"read parquet schema (parallelism=${parallelism.map(_.toString).getOrElse("None")})") {
      assertDf(
        spark.read
          .when(parallelism.isDefined)
          .either(_.parquetSchema(parallelism.get, nestedFile))
          .or(_.parquetSchema(nestedFile)),
        Seq($"filename", $"columnPath"),
        StructType(
          Seq(
            StructField("filename", StringType, nullable = true),
            StructField("columnName", StringType, nullable = true),
            StructField("columnPath", ArrayType(StringType, containsNull = true), nullable = true),
            StructField("repetition", StringType, nullable = true),
            StructField("type", StringType, nullable = true),
            StructField("length", IntegerType, nullable = true),
            StructField("originalType", StringType, nullable = true),
            StructField("logicalType", StringType, nullable = true),
            StructField("isPrimitive", BooleanType, nullable = false),
            StructField("primitiveType", StringType, nullable = true),
            StructField("primitiveOrder", StringType, nullable = true),
            StructField("maxDefinitionLevel", IntegerType, nullable = false),
            StructField("maxRepetitionLevel", IntegerType, nullable = false),
          )
        ),
        // format: off
        Seq(
          Row("nested.parquet", "a", Seq("a"), "REQUIRED", "INT64", 0, null, null, true, "INT64", "TYPE_DEFINED_ORDER", 0, 0),
          Row("nested.parquet", "x", Seq("b", "x"), "REQUIRED", "INT32", 0, null, null, true, "INT32", "TYPE_DEFINED_ORDER", 1, 0),
          Row("nested.parquet", "y", Seq("b", "y"), "REQUIRED", "DOUBLE", 0, null, null, true, "DOUBLE", "TYPE_DEFINED_ORDER", 1, 0),
          Row("nested.parquet", "z", Seq("b", "z"), "OPTIONAL", "INT64", 0, "TIMESTAMP_MICROS", TIMESTAMP, true, "INT64", "TYPE_DEFINED_ORDER", 2, 0),
          Row("nested.parquet", "element", Seq("c", "list", "element"), "OPTIONAL", "BINARY", 0, "UTF8", STRING, true, "BINARY", "TYPE_DEFINED_ORDER", 3, 1),
        ),
        // format: on
        parallelism
      )
    }
  }

  parallelisms.foreach { parallelism =>
    test(s"read parquet blocks (parallelism=${parallelism.map(_.toString).getOrElse("None")})") {
      assertDf(
        spark.read
          .when(parallelism.isDefined)
          .either(_.parquetBlocks(parallelism.get, testFile))
          .or(_.parquetBlocks(testFile)),
        Seq($"filename", $"block"),
        StructType(
          Seq(
            StructField("filename", StringType, nullable = true),
            StructField("block", IntegerType, nullable = false),
            StructField("blockStart", LongType, nullable = false),
            StructField("compressedBytes", LongType, nullable = false),
            StructField("uncompressedBytes", LongType, nullable = false),
            StructField("rows", LongType, nullable = false),
            StructField("columns", IntegerType, nullable = false),
            StructField("values", LongType, nullable = false),
            StructField("nulls", LongType, nullable = true),
          )
        ),
        Seq(
          Row("file1.parquet", 1, 4, 1268, 1652, 100, 2, 200, 0),
          Row("file2.parquet", 1, 4, 1269, 1651, 100, 2, 200, 0),
          Row("file2.parquet", 2, 1273, 1270, 1651, 100, 2, 200, 0),
        ),
        parallelism
      )
    }
  }

  parallelisms.foreach { parallelism =>
    test(s"read parquet block columns (parallelism=${parallelism.map(_.toString).getOrElse("None")})") {
      assertDf(
        spark.read
          .when(parallelism.isDefined)
          .either(_.parquetBlockColumns(parallelism.get, testFile))
          .or(_.parquetBlockColumns(testFile)),
        Seq($"filename", $"block", $"column"),
        StructType(
          Seq(
            StructField("filename", StringType, nullable = true),
            StructField("block", IntegerType, nullable = false),
            StructField("column", ArrayType(StringType), nullable = true),
            StructField("codec", StringType, nullable = true),
            StructField("type", StringType, nullable = true),
            StructField("encodings", ArrayType(StringType), nullable = true),
            StructField("minValue", StringType, nullable = true),
            StructField("maxValue", StringType, nullable = true),
            StructField("columnStart", LongType, nullable = false),
            StructField("compressedBytes", LongType, nullable = false),
            StructField("uncompressedBytes", LongType, nullable = false),
            StructField("values", LongType, nullable = false),
            StructField("nulls", LongType, nullable = true),
          )
        ),
        // format: off
        Seq(
          Row("file1.parquet", 1, "[id]", "SNAPPY", "required int64 id", "[BIT_PACKED, PLAIN]", "0", "99", 4, 437, 826, 100, 0),
          Row("file1.parquet", 1, "[val]", "SNAPPY", "required double val", "[BIT_PACKED, PLAIN]", "0.005067503372006343", "0.9973357672164814", 441, 831, 826, 100, 0),
          Row("file2.parquet", 1, "[id]", "SNAPPY", "required int64 id", "[BIT_PACKED, PLAIN]", "100", "199", 4, 438, 825, 100, 0),
          Row("file2.parquet", 1, "[val]", "SNAPPY", "required double val", "[BIT_PACKED, PLAIN]", "0.010617521596503865", "0.999189783846449", 442, 831, 826, 100, 0),
          Row("file2.parquet", 2, "[id]", "SNAPPY", "required int64 id", "[BIT_PACKED, PLAIN]", "200", "299", 1273, 440, 826, 100, 0),
          Row("file2.parquet", 2, "[val]", "SNAPPY", "required double val", "[BIT_PACKED, PLAIN]", "0.011277044401634018", "0.970525681750662", 1713, 830, 825, 100, 0),
        ),
        // format: on
        parallelism,
        (df: DataFrame) =>
          df
            .withColumn("column", $"column".cast(StringType))
            .withColumn("encodings", $"encodings".cast(StringType))
      )
    }
  }

  if (sys.env.get("CI_SLOW_TESTS").exists(_.equals("1"))) {
    Seq(1, 3, 7, 13, 19, 29, 61, 127, 251).foreach { partitionSize =>
      test(s"read parquet partitions ($partitionSize bytes)", Slow) {
        withSQLConf("spark.sql.files.maxPartitionBytes" -> partitionSize.toString) {
          val parquet = spark.read.parquet(testFile).cache()

          val rows = spark.read
            .parquet(testFile)
            .mapPartitions(it => Iterator(it.length))
            .select(spark_partition_id().as("partition"), $"value".as("actual_rows"))
          val partitions = spark.read
            .parquetPartitions(testFile)
            .join(rows, Seq("partition"), "left")
            .select($"partition", $"start", $"end", $"length", $"rows", $"actual_rows", $"filename")

          if (
            partitions
              .where(
                $"rows" =!= $"actual_rows" || ($"rows" =!= 0 || $"actual_rows" =!= 0) && $"length" =!= partitionSize
              )
              .head(1)
              .nonEmpty
          ) {
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
      Row(0, 1930, 1930, 1, 1268, 1652, 100, 2, 200, 0, "file1.parquet", 1930),
      Row(0, 3493, 3493, 2, 2539, 3302, 200, 2, 400, 0, "file2.parquet", 3493),
    ),
    Some(8192) -> Seq(
      Row(0, 1930, 1930, 1, 1268, 1652, 100, 2, 200, 0, "file1.parquet", 1930),
      Row(0, 3493, 3493, 2, 2539, 3302, 200, 2, 400, 0, "file2.parquet", 3493),
    ),
    Some(1024) -> Seq(
      Row(0, 1024, 1024, 1, 1268, 1652, 100, 2, 200, 0, "file1.parquet", 1930),
      Row(1024, 1930, 906, 0, 0, 0, 0, 0, 0, 0, "file1.parquet", 1930),
      Row(0, 1024, 1024, 1, 1269, 1651, 100, 2, 200, 0, "file2.parquet", 3493),
      Row(1024, 2048, 1024, 1, 1270, 1651, 100, 2, 200, 0, "file2.parquet", 3493),
      Row(2048, 3072, 1024, 0, 0, 0, 0, 0, 0, 0, "file2.parquet", 3493),
      Row(3072, 3493, 421, 0, 0, 0, 0, 0, 0, 0, "file2.parquet", 3493),
    ),
    Some(512) -> Seq(
      Row(0, 512, 512, 0, 0, 0, 0, 0, 0, 0, "file1.parquet", 1930),
      Row(512, 1024, 512, 1, 1268, 1652, 100, 2, 200, 0, "file1.parquet", 1930),
      Row(1024, 1536, 512, 0, 0, 0, 0, 0, 0, 0, "file1.parquet", 1930),
      Row(1536, 1930, 394, 0, 0, 0, 0, 0, 0, 0, "file1.parquet", 1930),
      Row(0, 512, 512, 0, 0, 0, 0, 0, 0, 0, "file2.parquet", 3493),
      Row(512, 1024, 512, 1, 1269, 1651, 100, 2, 200, 0, "file2.parquet", 3493),
      Row(1024, 1536, 512, 0, 0, 0, 0, 0, 0, 0, "file2.parquet", 3493),
      Row(1536, 2048, 512, 1, 1270, 1651, 100, 2, 200, 0, "file2.parquet", 3493),
      Row(2048, 2560, 512, 0, 0, 0, 0, 0, 0, 0, "file2.parquet", 3493),
      Row(2560, 3072, 512, 0, 0, 0, 0, 0, 0, 0, "file2.parquet", 3493),
      Row(3072, 3493, 421, 0, 0, 0, 0, 0, 0, 0, "file2.parquet", 3493),
    ),
  ).foreach { case (partitionSize, expectedRows) =>
    parallelisms.foreach { parallelism =>
      test(s"read parquet partitions (${partitionSize
          .getOrElse("default")} bytes) (parallelism=${parallelism.map(_.toString).getOrElse("None")})") {
        withSQLConf(
          partitionSize.map(size => Seq("spark.sql.files.maxPartitionBytes" -> size.toString)).getOrElse(Seq.empty): _*
        ) {
          val expected = expectedRows.map {
            case row if SparkMajorVersion > 3 || SparkMinorVersion >= 3 => row
            case row => Row(unapplySeq(row).get.updated(11, null): _*)
          }

          val actual = spark.read
            .when(parallelism.isDefined)
            .either(_.parquetPartitions(parallelism.get, testFile))
            .or(_.parquetPartitions(testFile))
            .cache()

          val partitions = actual.select($"partition").as[Int].collect()
          if (partitionSize.isDefined) {
            assert(partitions.indices === partitions.sorted)
          } else {
            assert(Seq(0, 0) === partitions)
          }

          val schema = StructType(
            Seq(
              StructField("partition", IntegerType, nullable = false),
              StructField("start", LongType, nullable = false),
              StructField("end", LongType, nullable = false),
              StructField("length", LongType, nullable = false),
              StructField("blocks", IntegerType, nullable = false),
              StructField("compressedBytes", LongType, nullable = false),
              StructField("uncompressedBytes", LongType, nullable = false),
              StructField("rows", LongType, nullable = false),
              StructField("columns", IntegerType, nullable = false),
              StructField("values", LongType, nullable = false),
              StructField("nulls", LongType, nullable = true),
              StructField("filename", StringType, nullable = true),
              StructField("fileLength", LongType, nullable = true),
            )
          )

          assertDf(actual, Seq($"filename", $"start"), schema, expected, parallelism, df => df.drop("partition"))
          actual.unpersist()
        }
      }
    }
  }
}
