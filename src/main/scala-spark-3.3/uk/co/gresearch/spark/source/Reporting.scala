package uk.co.gresearch.spark.source

import org.apache.spark.sql.connector.expressions.{Expression, NamedReference, Transform}
import org.apache.spark.sql.connector.read.{Batch, SupportsReportPartitioning, partitioning}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, UnknownPartitioning}
import uk.co.gresearch.spark.source.Reporting.namedReference

trait Reporting extends SupportsReportPartitioning {
  this: Batch =>

  def partitioned: Boolean
  def ordered: Boolean

  val partitionKeys: Array[Expression] = Array(namedReference("id"))

  def outputPartitioning: partitioning.Partitioning = if (partitioned) {
    new KeyGroupedPartitioning(partitionKeys, this.planInputPartitions().length)
  } else {
    new UnknownPartitioning(this.planInputPartitions().length)
  }
}

object Reporting {
  def namedReference(columnName: String): Expression =
    new Transform {
      override def name(): String = "identity"
      override def references(): Array[NamedReference] = Array.empty
      override def arguments(): Array[Expression] = Array(new NamedReference {
        override def fieldNames(): Array[String] = Array(columnName)
      })
    }
}
