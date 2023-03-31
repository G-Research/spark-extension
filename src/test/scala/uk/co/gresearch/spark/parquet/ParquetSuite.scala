package uk.co.gresearch.spark.parquet

import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.spark.SparkTestSession

class ParquetSuite extends AnyFunSuite with SparkTestSession {

  import spark.implicits._

  test("read parquet metadata") {
    val df = spark.read
      .parquetMetadata("../spark-dgraph-connector/dgraph.dbpedia.rdf-label.parquet")
      .orderBy($"filename")
    df.show(false)
  }

  test("read parquet blocks") {
    val df = spark.read
      .parquetBlocks("../spark-dgraph-connector/dgraph.dbpedia.rdf-label.parquet")
      .orderBy($"filename", $"block")
    df.show(false)
  }

  test("read parquet block columns") {
    val df = spark.read
      .parquetBlockColumns("../spark-dgraph-connector/dgraph.dbpedia.rdf-label.parquet")
      .orderBy($"filename", $"block", $"column")
    df.show(false)
  }

  test("read parquet partitions") {
    val df = spark.read
      .parquetPartitions("../spark-dgraph-connector/dgraph.dbpedia.rdf-label.parquet")
      .orderBy($"partition", $"filename")
    df.show(false)
  }

  test("read parquet partition rows") {
    val df = spark.read
      .parquetPartitionRows("../spark-dgraph-connector/dgraph.dbpedia.rdf-label.parquet")
      .orderBy($"partition", $"filename")
    df.show(false)
  }
}
