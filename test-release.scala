// this requires parquet-hadoop-*-tests.jar
// fetch with mvn dependency:get -Dtransitive=false -Dartifact=org.apache.parquet:parquet-hadoop:1.16.0:jar:tests
// run with dependency ~/.m2/repository/org/apache/parquet/parquet-hadoop/1.16.0/parquet-hadoop-1.16.0-tests.jar

import org.apache.spark.sql.DataFrame

def assertSize(df: DataFrame, size: Long): Unit = {
  Console.println(s"expect $size rows")
  df.show()
  assert(df.collect().size == size)
}


import uk.co.gresearch.spark.diff._
import uk.co.gresearch.spark.parquet._

try {
  val left = Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "value")
  val right = Seq((1, "one"), (2, "Two"), (4, "four")).toDF("id", "value")
  assertSize(left.diff(right), 5)

  Seq(
    ("test.parquet", (2, 4, 3, 6, 2)),
    ("nested.parquet", (1, 5, 1, 5, 1)),
    ("encrypted1.parquet", (1, 2, 1, 2, 1))
  ).foreach { case (file, rows) =>
    Console.println(file)
    val path = s"src/test/files/$file"
    val (metadataRows, schemaRows, blockRows, blockColumnRows, partitionRows) = rows
    assertSize(spark.read.parquetMetadata(path), metadataRows)
    assertSize(spark.read.parquetSchema(path), schemaRows)
    assertSize(spark.read.parquetBlocks(path), blockRows)
    assertSize(spark.read.parquetBlockColumns(path), blockColumnRows)
    if (file != "encrypted1.parquet") {
      assertSize(spark.read.parquetPartitions(path), partitionRows)
    }
  }

  // configure footer key only
  val hc = spark.sparkContext.hadoopConfiguration
  hc.set("parquet.crypto.factory.class", "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
  hc.set("parquet.encryption.kms.client.class", "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS")
  hc.set("parquet.encryption.key.list", "key:AAECAAECAAECAAECAAECAA==")

  Seq(
    ("encrypted1.parquet", (1, 2, 1, 2, 1)),
    ("encrypted2.parquet", (1, 2, 1, 2, 1))
  ).foreach { case (file, rows) =>
    Console.println(file)
    val path = s"src/test/files/$file"
    val (metadataRows, schemaRows, blockRows, blockColumnRows, partitionRows) = rows
    assertSize(spark.read.parquetMetadata(path), metadataRows)
    assertSize(spark.read.parquetSchema(path), schemaRows)
    assertSize(spark.read.parquetBlocks(path), blockRows)
    assertSize(spark.read.parquetBlockColumns(path), blockColumnRows)
    if (file != "encrypted1.parquet") {
      assertSize(spark.read.parquetPartitions(path), partitionRows)
    }
  }
} catch {
  case e: Throwable => sys.exit(1)
}

sys.exit(0)
