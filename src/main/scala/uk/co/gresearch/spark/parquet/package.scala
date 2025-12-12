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

package uk.co.gresearch.spark

// hadoop and parquet dependencies provided by Spark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.{Footer, ParquetFileReader}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.FilePartition
import uk.co.gresearch._

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsScalaMapConverter}
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

package object parquet {

  private def conf: Configuration = SparkContext.getOrCreate().hadoopConfiguration

  /**
   * Implicit class to extend a Spark DataFrameReader.
   *
   * @param reader
   *   data frame reader
   */
  implicit class ExtendedDataFrameReader(reader: DataFrameReader) {

    /**
     * Read the metadata of Parquet files into a Dataframe.
     *
     * The returned DataFrame has as many partitions as there are Parquet files, at most
     * `spark.sparkContext.defaultParallelism` partitions.
     *
     * This provides the following per-file information:
     *   - filename (string): The file name
     *   - blocks (int): Number of blocks / RowGroups in the Parquet file
     *   - compressedBytes (long): Number of compressed bytes of all blocks
     *   - uncompressedBytes (long): Number of uncompressed bytes of all blocks
     *   - rows (long): Number of rows in the file
     *   - columns (int): Number of columns in the file
     *   - values (long): Number of values in the file
     *   - nulls (long): Number of null values in the file
     *   - createdBy (string): The createdBy string of the Parquet file, e.g. library used to write the file
     *   - schema (string): The schema
     *   - encryption (string): The encryption
     *   - keyValues (string-to-string map): Key-value data of the file
     *
     * @param paths
     *   one or more paths to Parquet files or directories
     * @return
     *   dataframe with Parquet metadata
     */
    @scala.annotation.varargs
    def parquetMetadata(paths: String*): DataFrame = parquetMetadata(None, paths)

    /**
     * Read the metadata of Parquet files into a Dataframe.
     *
     * The returned DataFrame has as many partitions as specified via `parallelism`.
     *
     * This provides the following per-file information:
     *   - filename (string): The file name
     *   - blocks (int): Number of blocks / RowGroups in the Parquet file
     *   - compressedBytes (long): Number of compressed bytes of all blocks
     *   - uncompressedBytes (long): Number of uncompressed bytes of all blocks
     *   - rows (long): Number of rows in the file
     *   - columns (int): Number of columns in the file
     *   - values (long): Number of values in the file
     *   - nulls (long): Number of null values in the file
     *   - createdBy (string): The createdBy string of the Parquet file, e.g. library used to write the file
     *   - schema (string): The schema
     *   - encryption (string): The encryption
     *   - keyValues (string-to-string map): Key-value data of the file
     *
     * @param parallelism
     *   number of partitions of returned DataFrame
     * @param paths
     *   one or more paths to Parquet files or directories
     * @return
     *   dataframe with Parquet metadata
     */
    @scala.annotation.varargs
    def parquetMetadata(parallelism: Int, paths: String*): DataFrame = parquetMetadata(Some(parallelism), paths)

    private def parquetMetadata(parallelism: Option[Int], paths: Seq[String]): DataFrame = {
      val files = getFiles(parallelism, paths)

      import files.sparkSession.implicits._

      files
        .flatMap { case (_, file) =>
          readFooters(file).map { footer =>
            val guard = FooterGuard(footer)
            (
              footer.getFile.toString,
              footer.getParquetMetadata.getBlocks.size(),
              guard { footer.getParquetMetadata.getBlocks.asScala.map(_.getCompressedSize).sum },
              guard { footer.getParquetMetadata.getBlocks.asScala.map(_.getTotalByteSize).sum },
              footer.getParquetMetadata.getBlocks.asScala.map(_.getRowCount).sum,
              footer.getParquetMetadata.getFileMetaData.getSchema.getColumns.size(),
              guard {
                footer.getParquetMetadata.getBlocks.asScala.map(_.getColumns.map(_.getValueCount).sum).sum
              },
              // when all columns have statistics, count the null values
              guard {
                Option(
                  footer.getParquetMetadata.getBlocks.asScala.flatMap(_.getColumns.map(c => Option(c.getStatistics)))
                )
                  .filter(_.forall(_.isDefined))
                  .map(_.map(_.get.getNumNulls).sum)
              },
              footer.getParquetMetadata.getFileMetaData.getCreatedBy,
              footer.getParquetMetadata.getFileMetaData.getSchema.toString,
              ParquetMetaDataUtil.getEncryptionType(footer.getParquetMetadata.getFileMetaData),
              footer.getParquetMetadata.getFileMetaData.getKeyValueMetaData.asScala,
            )
          }
        }
        .toDF(
          "filename",
          "blocks",
          "compressedBytes",
          "uncompressedBytes",
          "rows",
          "columns",
          "values",
          "nulls",
          "createdBy",
          "schema",
          "encryption",
          "keyValues"
        )
    }

    /**
     * Read the schema of Parquet files into a Dataframe.
     *
     * The returned DataFrame has as many partitions as there are Parquet files, at most
     * `spark.sparkContext.defaultParallelism` partitions.
     *
     * This provides the following per-file information:
     *   - filename (string): The Parquet file name
     *   - columnName (string): The column name
     *   - columnPath (string array): The column path
     *   - repetition (string): The repetition
     *   - type (string): The data type
     *   - length (int): The length of the type
     *   - originalType (string): The original type
     *   - isPrimitive (boolean: True if type is primitive
     *   - primitiveType (string: The primitive type
     *   - primitiveOrder (string: The order of the primitive type
     *   - maxDefinitionLevel (int): The max definition level
     *   - maxRepetitionLevel (int): The max repetition level
     *
     * @param paths
     *   one or more paths to Parquet files or directories
     * @return
     *   dataframe with Parquet metadata
     */
    @scala.annotation.varargs
    def parquetSchema(paths: String*): DataFrame = parquetSchema(None, paths)

    /**
     * Read the schema of Parquet files into a Dataframe.
     *
     * The returned DataFrame has as many partitions as specified via `parallelism`.
     *
     * This provides the following per-file information:
     *   - filename (string): The Parquet file name
     *   - columnName (string): The column name
     *   - columnPath (string array): The column path
     *   - repetition (string): The repetition
     *   - type (string): The data type
     *   - length (int): The length of the type
     *   - originalType (string): The original type
     *   - isPrimitive (boolean: True if type is primitive
     *   - primitiveType (string: The primitive type
     *   - primitiveOrder (string: The order of the primitive type
     *   - maxDefinitionLevel (int): The max definition level
     *   - maxRepetitionLevel (int): The max repetition level
     *
     * @param parallelism
     *   number of partitions of returned DataFrame
     * @param paths
     *   one or more paths to Parquet files or directories
     * @return
     *   dataframe with Parquet metadata
     */
    @scala.annotation.varargs
    def parquetSchema(parallelism: Int, paths: String*): DataFrame = parquetSchema(Some(parallelism), paths)

    private def parquetSchema(parallelism: Option[Int], paths: Seq[String]): DataFrame = {
      val files = getFiles(parallelism, paths)

      import files.sparkSession.implicits._

      files
        .flatMap { case (_, file) =>
          readFooters(file).flatMap { footer =>
            footer.getParquetMetadata.getFileMetaData.getSchema.getColumns.map { column =>
              (
                footer.getFile.toString,
                Option(column.getPrimitiveType).map(_.getName),
                column.getPath,
                Option(column.getPrimitiveType).flatMap(v => Option(v.getRepetition)).map(_.name),
                Option(column.getPrimitiveType).flatMap(v => Option(v.getPrimitiveTypeName)).map(_.name),
                Option(column.getPrimitiveType).map(_.getTypeLength),
                Option(column.getPrimitiveType).flatMap(v => Option(v.getOriginalType)).map(_.name),
                Option(column.getPrimitiveType).flatMap(ParquetMetaDataUtil.getLogicalTypeAnnotation),
                column.getPrimitiveType.isPrimitive,
                Option(column.getPrimitiveType).map(_.getPrimitiveTypeName.name),
                Option(column.getPrimitiveType).flatMap(v => Option(v.columnOrder)).map(_.getColumnOrderName.name),
                column.getMaxDefinitionLevel,
                column.getMaxRepetitionLevel,
              )
            }
          }
        }
        .toDF(
          "filename",
          "columnName",
          "columnPath",
          "repetition",
          "type",
          "length",
          "originalType",
          "logicalType",
          "isPrimitive",
          "primitiveType",
          "primitiveOrder",
          "maxDefinitionLevel",
          "maxRepetitionLevel",
        )
    }

    /**
     * Read the metadata of Parquet blocks into a Dataframe.
     *
     * The returned DataFrame has as many partitions as there are Parquet files, at most
     * `spark.sparkContext.defaultParallelism` partitions.
     *
     * This provides the following per-block information:
     *   - filename (string): The file name
     *   - block (int): Block / RowGroup number starting at 1
     *   - blockStart (long): Start position of the block in the Parquet file
     *   - compressedBytes (long): Number of compressed bytes in block
     *   - uncompressedBytes (long): Number of uncompressed bytes in block
     *   - rows (long): Number of rows in block
     *   - columns (int): Number of columns in block
     *   - values (long): Number of values in block
     *   - nulls (long): Number of null values in block
     *
     * @param paths
     *   one or more paths to Parquet files or directories
     * @return
     *   dataframe with Parquet block metadata
     */
    @scala.annotation.varargs
    def parquetBlocks(paths: String*): DataFrame = parquetBlocks(None, paths)

    /**
     * Read the metadata of Parquet blocks into a Dataframe.
     *
     * The returned DataFrame has as many partitions as specified via `parallelism`.
     *
     * This provides the following per-block information:
     *   - filename (string): The file name
     *   - block (int): Block / RowGroup number starting at 1 (block ordinal + 1)
     *   - blockStart (long): Start position of the block in the Parquet file
     *   - compressedBytes (long): Number of compressed bytes in block
     *   - uncompressedBytes (long): Number of uncompressed bytes in block
     *   - rows (long): Number of rows in block
     *   - columns (int): Number of columns in block
     *   - values (long): Number of values in block
     *   - nulls (long): Number of null values in block
     *
     * @param parallelism
     *   number of partitions of returned DataFrame
     * @param paths
     *   one or more paths to Parquet files or directories
     * @return
     *   dataframe with Parquet block metadata
     */
    @scala.annotation.varargs
    def parquetBlocks(parallelism: Int, paths: String*): DataFrame = parquetBlocks(Some(parallelism), paths)

    private def parquetBlocks(parallelism: Option[Int], paths: Seq[String]): DataFrame = {
      val files = getFiles(parallelism, paths)

      import files.sparkSession.implicits._

      files
        .flatMap { case (_, file) =>
          readFooters(file).flatMap { footer =>
            val guard = FooterGuard(footer)
            footer.getParquetMetadata.getBlocks.asScala.zipWithIndex.map { case (block, idx) =>
              (
                footer.getFile.toString,
                ParquetMetaDataUtil.getOrdinal(block).getOrElse(idx) + 1,
                block.getStartingPos,
                guard { block.getCompressedSize },
                block.getTotalByteSize,
                block.getRowCount,
                block.getColumns.asScala.size,
                guard { block.getColumns.asScala.map(_.getValueCount).sum },
                // when all columns have statistics, count the null values
                guard {
                  Option(block.getColumns.asScala.map(c => Option(c.getStatistics)))
                    .filter(_.forall(_.isDefined))
                    .map(_.map(_.get.getNumNulls).sum)
                },
              )
            }
          }
        }
        .toDF(
          "filename",
          "block",
          "blockStart",
          "compressedBytes",
          "uncompressedBytes",
          "rows",
          "columns",
          "values",
          "nulls"
        )
    }

    /**
     * Read the metadata of Parquet block columns into a Dataframe.
     *
     * The returned DataFrame has as many partitions as there are Parquet files, at most
     * `spark.sparkContext.defaultParallelism` partitions.
     *
     * This provides the following per-block-column information:
     *   - filename (string): The file name
     *   - block (int): Block / RowGroup number starting at 1
     *   - column (string): Block / RowGroup column name
     *   - codec (string): The coded used to compress the block column values
     *   - type (string): The data type of the block column
     *   - encodings (string): Encodings of the block column
     *   - minValue (string): Minimum value of this column in this block
     *   - maxValue (string): Maximum value of this column in this block
     *   - columnStart (long): Start position of the block column in the Parquet file
     *   - compressedBytes (long): Number of compressed bytes of this block column
     *   - uncompressedBytes (long): Number of uncompressed bytes of this block column
     *   - values (long): Number of values in this block column
     *   - nulls (long): Number of null values in block
     *
     * @param paths
     *   one or more paths to Parquet files or directories
     * @return
     *   dataframe with Parquet block metadata
     */
    @scala.annotation.varargs
    def parquetBlockColumns(paths: String*): DataFrame = parquetBlockColumns(None, paths)

    /**
     * Read the metadata of Parquet block columns into a Dataframe.
     *
     * The returned DataFrame has as many partitions as specified via `parallelism`.
     *
     * This provides the following per-block-column information:
     *   - filename (string): The file name
     *   - block (int): Block / RowGroup number starting at 1 (block ordinal + 1)
     *   - column (string): Block / RowGroup column name
     *   - codec (string): The coded used to compress the block column values
     *   - type (string): The data type of the block column
     *   - encodings (string): Encodings of the block column
     *   - minValue (string): Minimum value of this column in this block
     *   - maxValue (string): Maximum value of this column in this block
     *   - columnStart (long): Start position of the block column in the Parquet file
     *   - compressedBytes (long): Number of compressed bytes of this block column
     *   - uncompressedBytes (long): Number of uncompressed bytes of this block column
     *   - values (long): Number of values in this block column
     *   - nulls (long): Number of null values in block
     *
     * @param parallelism
     *   number of partitions of returned DataFrame
     * @param paths
     *   one or more paths to Parquet files or directories
     * @return
     *   dataframe with Parquet block metadata
     */
    @scala.annotation.varargs
    def parquetBlockColumns(parallelism: Int, paths: String*): DataFrame = parquetBlockColumns(Some(parallelism), paths)

    private def parquetBlockColumns(parallelism: Option[Int], paths: Seq[String]): DataFrame = {
      val files = getFiles(parallelism, paths)

      import files.sparkSession.implicits._

      files
        .flatMap { case (_, file) =>
          readFooters(file).flatMap { footer =>
            val guard = FooterGuard(footer)
            footer.getParquetMetadata.getBlocks.asScala.zipWithIndex.flatMap { case (block, idx) =>
              block.getColumns.asScala.map { column =>
                (
                  footer.getFile.toString,
                  ParquetMetaDataUtil.getOrdinal(block).getOrElse(idx) + 1,
                  column.getPath.toSeq,
                  guard { column.getCodec.toString },
                  guard { column.getPrimitiveType.toString },
                  guard { column.getEncodings.asScala.toSeq.map(_.toString).sorted },
                  ParquetMetaDataUtil.isEncrypted(column),
                  guard { Option(column.getStatistics).map(_.minAsString) },
                  guard { Option(column.getStatistics).map(_.maxAsString) },
                  guard { column.getStartingPos },
                  guard { column.getTotalSize },
                  guard { column.getTotalUncompressedSize },
                  guard { column.getValueCount },
                  guard { Option(column.getStatistics).map(_.getNumNulls) },
                )
              }
            }
          }
        }
        .toDF(
          "filename",
          "block",
          "column",
          "codec",
          "type",
          "encodings",
          "encrypted",
          "minValue",
          "maxValue",
          "columnStart",
          "compressedBytes",
          "uncompressedBytes",
          "values",
          "nulls"
        )
    }

    /**
     * Read the metadata of how Spark partitions Parquet files into a Dataframe.
     *
     * The returned DataFrame has as many partitions as there are Parquet files, at most
     * `spark.sparkContext.defaultParallelism` partitions.
     *
     * This provides the following per-partition information:
     *   - partition (int): The Spark partition id
     *   - start (long): The start position of the partition
     *   - end (long): The end position of the partition
     *   - length (long): The length of the partition
     *   - blocks (int): The number of Parquet blocks / RowGroups in this partition
     *   - compressedBytes (long): The number of compressed bytes in this partition
     *   - uncompressedBytes (long): The number of uncompressed bytes in this partition
     *   - rows (long): The number of rows in this partition
     *   - columns (int): Number of columns in the file
     *   - values (long): The number of values in this partition
     *   - filename (string): The Parquet file name
     *   - fileLength (long): The length of the Parquet file
     *
     * @param paths
     *   one or more paths to Parquet files or directories
     * @return
     *   dataframe with Spark Parquet partition metadata
     */
    @scala.annotation.varargs
    def parquetPartitions(paths: String*): DataFrame = parquetPartitions(None, paths)

    /**
     * Read the metadata of how Spark partitions Parquet files into a Dataframe.
     *
     * The returned DataFrame has as many partitions as specified via `parallelism`.
     *
     * This provides the following per-partition information:
     *   - partition (int): The Spark partition id
     *   - start (long): The start position of the partition
     *   - end (long): The end position of the partition
     *   - length (long): The length of the partition
     *   - blocks (int): The number of Parquet blocks / RowGroups in this partition
     *   - compressedBytes (long): The number of compressed bytes in this partition
     *   - uncompressedBytes (long): The number of uncompressed bytes in this partition
     *   - rows (long): The number of rows in this partition
     *   - columns (int): Number of columns in the file
     *   - values (long): The number of values in this partition
     *   - filename (string): The Parquet file name
     *   - fileLength (long): The length of the Parquet file
     *
     * @param parallelism
     *   number of partitions of returned DataFrame
     * @param paths
     *   one or more paths to Parquet files or directories
     * @return
     *   dataframe with Spark Parquet partition metadata
     */
    @scala.annotation.varargs
    def parquetPartitions(parallelism: Int, paths: String*): DataFrame = parquetPartitions(Some(parallelism), paths)

    private def parquetPartitions(parallelism: Option[Int], paths: Seq[String]): DataFrame = {
      val files = getFiles(parallelism, paths)

      import files.sparkSession.implicits._

      files
        .flatMap { case (part, file) =>
          readFooters(file)
            .map(footer => (footer, getBlocks(footer, file.start, file.length)))
            .map { case (footer, blocks) =>
              (
                part,
                file.start,
                file.start + file.length,
                file.length,
                blocks.size,
                blocks.map(_.getCompressedSize).sum,
                blocks.map(_.getTotalByteSize).sum,
                blocks.map(_.getRowCount).sum,
                blocks
                  .map(_.getColumns.map(_.getPath.mkString(".")).toSet)
                  .foldLeft(Set.empty[String])((left, right) => left.union(right))
                  .size,
                blocks.map(_.getColumns.asScala.map(_.getValueCount).sum).sum,
                // when all columns have statistics, count the null values
                Option(blocks.flatMap(_.getColumns.asScala.map(c => Option(c.getStatistics))))
                  .filter(_.forall(_.isDefined))
                  .map(_.map(_.get.getNumNulls).sum),
                footer.getFile.toString,
                file.fileSize,
              )
            }
        }
        .toDF(
          "partition",
          "start",
          "end",
          "length",
          "blocks",
          "compressedBytes",
          "uncompressedBytes",
          "rows",
          "columns",
          "values",
          "nulls",
          "filename",
          "fileLength"
        )
    }

    private def getFiles(parallelism: Option[Int], paths: Seq[String]): Dataset[(Int, SplitFile)] = {
      val df = reader.parquet(paths: _*)
      val parts = df.rdd.partitions
        .flatMap(part =>
          part
            .asInstanceOf[FilePartition]
            .files
            .map(file => (part.index, SplitFile(file)))
        )
        .toSeq
        .distinct

      import df.sparkSession.implicits._

      parts
        .toDS()
        .when(parallelism.isDefined)
        .call(_.repartition(parallelism.get))
    }
  }

  private def readFooters(file: SplitFile): Iterable[Footer] = {
    val path = new Path(file.filePath)
    val status = path.getFileSystem(conf).getFileStatus(path)
    ParquetFileReader.readFooters(conf, status, false).asScala
  }

  private def getBlocks(footer: Footer, start: Long, length: Long): Seq[BlockMetaData] = {
    footer.getParquetMetadata.getBlocks.asScala
      .map(block => (block, block.getStartingPos + block.getCompressedSize / 2))
      .filter { case (_, midBlock) => start <= midBlock && midBlock < start + length }
      .map(_._1)
      .toSeq
  }

}
