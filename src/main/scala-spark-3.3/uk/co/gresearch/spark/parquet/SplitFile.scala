package uk.co.gresearch.spark.parquet

import org.apache.spark.sql.execution.datasources.PartitionedFile

case class SplitFile(filePath: String, start: Long, length: Long, fileSize: Option[Long])

object SplitFile {
  def apply(file: PartitionedFile): SplitFile = SplitFile(file.filePath, file.start, file.length, Some(file.fileSize))
}
