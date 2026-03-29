package uk.co.gresearch.spark.source

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read
import org.apache.spark.sql.connector.read.{Batch, InputPartition, Scan}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.Timestamp
import java.util
import scala.collection.JavaConverters._

class DefaultSource() extends TableProvider with DataSourceRegister {
  override def shortName(): String = "example"
  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = Array.empty
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = DefaultSource.schema
  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table =
    BatchTable(
      properties.getOrDefault("partitioned", "false").toBoolean,
      properties.getOrDefault("ordered", "false").toBoolean
    )
}

object DefaultSource {
  val supportsReportingOrder: Boolean = false
  val schema: StructType = StructType(Seq(
    StructField("id", IntegerType),
    StructField("time", TimestampType),
    StructField("value", DoubleType),
  ))
  val ts: Long = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2020-01-01 12:00:00"))
  val data: Map[Int, Array[InternalRow]] = Map(
    1 -> Array(
      InternalRow(1, ts + 1000000, 1.1),
      InternalRow(1, ts + 2000000, 1.2),
      InternalRow(1, ts + 3000000, 1.3),
      InternalRow(3, ts + 1000000, 3.1),
      InternalRow(3, ts + 2000000, 3.2)
    ),
    2 -> Array(
      InternalRow(2, ts + 1000000, 2.1),
      InternalRow(2, ts + 2000000, 2.2),
      InternalRow(4, ts + 1000000, 4.1),
      InternalRow(4, ts + 2000000, 4.2),
      InternalRow(4, ts + 3000000, 4.3)
    )
  )
  val partitions: Int = data.size
}

case class BatchTable(partitioned: Boolean, ordered: Boolean) extends Table with SupportsRead {
  override def name(): String = "table"
  override def schema(): StructType = DefaultSource.schema
  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava
  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): read.ScanBuilder =
    new ScanBuilder(partitioned, ordered)
}

class ScanBuilder(partitioned: Boolean, ordered: Boolean) extends read.ScanBuilder {
  override def build(): Scan = BatchScan(partitioned, ordered)
}

case class BatchScan(partitioned: Boolean, ordered: Boolean) extends read.Scan with read.Batch with Reporting {
  override def readSchema(): StructType = DefaultSource.schema
  override def toBatch: Batch = this
  override def planInputPartitions(): Array[InputPartition] = DefaultSource.data.keys.map(Partition).toArray
  override def createReaderFactory(): read.PartitionReaderFactory = PartitionReaderFactory()
}

case class Partition(id: Int) extends InputPartition

case class PartitionReaderFactory() extends read.PartitionReaderFactory {
  override def createReader(partition: InputPartition): read.PartitionReader[InternalRow] = PartitionReader(partition)
}


case class PartitionReader(partition: InputPartition) extends read.PartitionReader[InternalRow] {
  val rows: Iterator[InternalRow] = DefaultSource.data.getOrElse(partition.asInstanceOf[Partition].id, Array.empty[InternalRow]).iterator
  def next: Boolean = rows.hasNext
  def get: InternalRow = rows.next()
  def close(): Unit = { }
}
