package uk.co.gresearch.spark.source

import org.apache.spark.sql.connector.read.{Batch, SupportsReportPartitioning, partitioning}
import org.apache.spark.sql.connector.read.partitioning.{ClusteredDistribution, Distribution}

trait Reporting extends SupportsReportPartitioning {
  this: Batch =>

  def partitioned: Boolean
  def ordered: Boolean

  def outputPartitioning: partitioning.Partitioning =
    Partitioning(this.planInputPartitions().length, partitioned)
}

case class Partitioning(partitions: Int, partitioned: Boolean) extends partitioning.Partitioning {
  override def numPartitions(): Int = partitions
  override def satisfy(distribution: Distribution): Boolean =  distribution match {
    case c: ClusteredDistribution => partitioned && c.clusteredColumns.contains("id")
    case _ => false
  }
}
