package uk.co.gresearch.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.functions.spark_partition_id
import org.apache.spark.sql.{DataFrame, DataFrameReader, Encoder, Encoders}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

package object parquet {
  private implicit val intEncoder: Encoder[Int] = Encoders.scalaInt

  /**
   * Implicit class to extend a Spark DataFrameReader.
   *
   * @param reader data frame reader
   */
  implicit class ExtendedDataFrameReader(reader: DataFrameReader) {
    def parquetMetadata(path: String): DataFrame = parquetMetadata(Seq(path): _*)

    @scala.annotation.varargs
    def parquetMetadata(paths: String*): DataFrame = {
      val df = reader.parquet(paths: _*)
      val files = df.rdd.partitions.flatMap(_.asInstanceOf[FilePartition].files.map(file => file.filePath)).toSeq.distinct

      val spark = df.sparkSession
      import spark.implicits._

      spark.createDataset(files).flatMap { file =>
        val conf = new Configuration()
        val inputPath = new Path(file)
        val inputFileStatus = inputPath.getFileSystem(conf).getFileStatus(inputPath)
        val footers = ParquetFileReader.readFooters(conf, inputFileStatus, false)
        footers.asScala.map { footer =>
          (
            footer.getFile.toString,
            footer.getParquetMetadata.getBlocks.size(),
            footer.getParquetMetadata.getBlocks.asScala.map(_.getTotalByteSize).sum,
            footer.getParquetMetadata.getBlocks.asScala.map(_.getCompressedSize).sum,
            footer.getParquetMetadata.getBlocks.asScala.map(_.getRowCount).sum,
            footer.getParquetMetadata.getFileMetaData.getCreatedBy,
            footer.getParquetMetadata.getFileMetaData.getSchema.toString,
          )
        }
      }.toDF("filename", "blocks", "totalSizeBytes", "compressedSizeBytes", "totalRowCount", "createdBy", "schema")
    }

    @scala.annotation.varargs
    def parquetBlocks(paths: String*): DataFrame = {
      val df = reader.parquet(paths: _*)
      val files = df.rdd.partitions.flatMap(_.asInstanceOf[FilePartition].files.map(file => file.filePath)).toSeq.distinct

      val spark = df.sparkSession
      import spark.implicits._

      spark.createDataset(files).flatMap { file =>
        val conf = new Configuration()
        val inputPath = new Path(file)
        val inputFileStatus = inputPath.getFileSystem(conf).getFileStatus(inputPath)
        val footers = ParquetFileReader.readFooters(conf, inputFileStatus, false)
        footers.asScala.flatMap { footer =>
          footer.getParquetMetadata.getBlocks.asScala.zipWithIndex.map { case (block, idx) =>
            (
              footer.getFile.toString,
              idx + 1,
              block.getStartingPos,
              block.getTotalByteSize,
              block.getCompressedSize,
              block.getRowCount,
            )
          }
        }
      }.toDF("filename", "block", "startPos", "totalSizeBytes", "compressedSizeBytes", "totalRowCount")
    }

    @scala.annotation.varargs
    def parquetBlockColumns(paths: String*): DataFrame = {
      val df = reader.parquet(paths: _*)
      val files = df.rdd.partitions.flatMap(_.asInstanceOf[FilePartition].files.map(file => file.filePath)).toSeq.distinct

      val spark = df.sparkSession
      import spark.implicits._

      spark.createDataset(files).flatMap { file =>
        val conf = new Configuration()
        val inputPath = new Path(file)
        val inputFileStatus = inputPath.getFileSystem(conf).getFileStatus(inputPath)
        val footers = ParquetFileReader.readFooters(conf, inputFileStatus, false)
        footers.asScala.flatMap { footer =>
          footer.getParquetMetadata.getBlocks.asScala.zipWithIndex.flatMap { case (block, idx) =>
            block.getColumns.asScala.map { column =>
              (
                footer.getFile.toString,
                idx + 1,
                column.getPath.toString,
                column.getCodec.toString,
                column.getPrimitiveType.toString,
                column.getEncodings.toString,
                column.getStatistics.minAsString(),
                column.getStatistics.maxAsString(),
                column.getStartingPos,
                column.getTotalUncompressedSize,
                column.getTotalSize,
                column.getValueCount,
              )
            }
          }
        }
      }.toDF("filename", "block", "column", "codec", "type", "encodings", "minValue", "maxValue", "startPos", "sizeBytes", "compressedSizeBytes", "valueCount")
    }

    @scala.annotation.varargs
    def parquetPartitions(paths: String*): DataFrame = {
      val df = reader.parquet(paths: _*)
      val files = df.rdd.partitions.flatMap(part => part.asInstanceOf[FilePartition].files.map(file => (part.index, file.filePath, file.start, file.length, file.fileSize))).toSeq

      val spark = df.sparkSession
      import spark.implicits._

      spark.createDataset(files).flatMap { case (part, filePath, start, length, fileSize) =>
        val conf = new Configuration()
        val inputPath = new Path(filePath)
        val inputFileStatus = inputPath.getFileSystem(conf).getFileStatus(inputPath)
        val footers = ParquetFileReader.readFooters(conf, inputFileStatus, false)
        footers.asScala.map { footer => (
          part,
          footer.getFile.toString,
          start,
          start + length,
          length,
          fileSize,
        )}
      }.toDF("partition", "filename", "start", "end", "partitionLength", "fileLength")
    }

    @scala.annotation.varargs
    def parquetPartitionRows(paths: String*): DataFrame = {
      val df = reader.parquet(paths: _*)

      val spark = df.sparkSession
      import spark.implicits._

      val partLengths =
        df.select()
          .mapPartitions(it => Iterator(it.length))(intEncoder)
          .select(spark_partition_id().as("partition"), $"value".as("rows"))

      parquetPartitions(paths: _*).join(partLengths, Seq("partition"), "left")
    }
  }

}
