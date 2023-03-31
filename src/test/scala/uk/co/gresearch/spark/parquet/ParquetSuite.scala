package uk.co.gresearch.spark.parquet

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.spark.SparkTestSession
import org.apache.spark.sql.functions.regexp_replace
import uk.co.gresearch._

class ParquetSuite extends AnyFunSuite with SparkTestSession {

  import spark.implicits._

  // import org.apache.spark.sql.SaveMode
  // spark.sparkContext.hadoopConfiguration.setInt("parquet.block.size", 1024)
  // spark.range(100).select($"id", rand().as("val")).repartitionByRange(1, $"id").write.parquet("test.parquet")
  // spark.range(100, 300, 1).select($"id", rand().as("val")).repartitionByRange(1, $"id").write.mode(SaveMode.Append).parquet("test.parquet")eq((3, "three"), (4, "four"), (5, "five"), (6, "six"), (7, "seven")).toDF("id", "value").repartitionByRange(1, $"id").write.mode(SaveMode.Append).parquet("test.parquet")
  val testFile = "src/test/files/test.parquet"


  def assertDf(actual: DataFrame, expected: Seq[Row]): Unit = {
    val replaced =
      actual
        .withColumn("filename", regexp_replace($"filename", ".*/test.parquet/", ""))
        .when(actual.columns.contains("schema"))
        .call(_.withColumn("schema", regexp_replace($"schema", "\n", "\\\\n")))
    val act = replaced.collect()
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

  test("read parquet partitions") {
    assertDf(
      spark.read
        .parquetPartitions(testFile)
        .orderBy($"partition", $"filename"),
      Seq(
        Row(0, "file1.parquet", 0, 1930, 1930, 1930),
        Row(0, "file2.parquet", 0, 3493, 3493, 3493),
      )
    )
  }

  test("read parquet partition rows") {
    assertDf(
      spark.read
        .parquetPartitionRows(testFile)
        .orderBy($"partition", $"filename"),
      Seq(
        Row(0, "file1.parquet", 0, 1930, 1930, 1930, 300),
        Row(0, "file2.parquet", 0, 3493, 3493, 3493, 300),
      )
    )
  }
}
