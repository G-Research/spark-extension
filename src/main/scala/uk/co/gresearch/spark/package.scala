/*
 * Copyright 2020 G-Research
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

package uk.co.gresearch

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{coalesce, col, lit, max, monotonically_increasing_id, spark_partition_id, sum}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import uk.co.gresearch.spark.group.SortedGroupByDataset

package object spark {

  /**
   * Provides a prefix that makes any string distinct w.r.t. the given strings.
   * @param existing strings
   * @return distinct prefix
   */
  private[spark] def distinctPrefixFor(existing: Seq[String]): String = {
    "_" * (existing.map(_.takeWhile(_ == '_').length).reduceOption(_ max _).getOrElse(0) + 1)
  }

  /**
   * Encloses the given strings with backticks if needed. Multiple strings will be enclosed individually and
   * concatenated with dots (`.`).
   *
   * This is useful when referencing column names that contain special characters like dots (`.`).
   *
   * Examples:
   * {{{
   *   col("a.column")                        // this references the field "column" of column "a"
   *   col("`a.column`")                      // this reference the column with the name "a.column"
   *   col(backticks("column"))               // produces "column"
   *   col(backticks("a.column"))             // produces "`a.column`"
   *   col(backticks("`a.column`"))           // produces "`a.column`"
   *   col(backticks("a.column", "a.field"))  // produces "`a.column`.`a.field`"
   * }}}
   *
   * @param string  a string
   * @param strings more strings
   * @return
   */
  @scala.annotation.varargs
  def backticks(string: String, strings: String*): String =
    Backticks.column_name(string, strings: _*)

  /**
   * Implicit class to extend a Spark Dataset.
   *
   * @param ds dataset
   * @tparam T inner type of dataset
   */
  implicit class ExtendedDataset[T: Encoder](ds: Dataset[T]) {
    /**
     * Compute the histogram of a column when aggregated by aggregate columns.
     * Thresholds are expected to be provided in ascending order.
     * The result dataframe contains the aggregate and histogram columns only.
     * For each threshold value in thresholds, there will be a column named s"≤threshold".
     * There will also be a final column called s">last_threshold", that counts the remaining
     * values that exceed the last threshold.
     *
     * @param thresholds       sequence of thresholds, must implement <= and > operators w.r.t. valueColumn
     * @param valueColumn      histogram is computed for values of this column
     * @param aggregateColumns histogram is computed against these columns
     * @tparam T type of histogram thresholds
     * @return dataframe with aggregate and histogram columns
     */
    def histogram[T: Ordering](thresholds: Seq[T], valueColumn: Column, aggregateColumns: Column*): DataFrame =
      Histogram.of(ds, thresholds, valueColumn, aggregateColumns: _*)

    /**
     * Writes the Dataset / DataFrame via DataFrameWriter.partitionBy. In addition to partitionBy,
     * this method sorts the data to improve partition file size. Small partitions will contain few
     * files, large partitions contain more files. Partition ids are contained in a single partition
     * file per `partitionBy` partition only. Rows within the partition files are also sorted,
     * if partitionOrder is defined.
     *
     * Calling:
     * {{{
     *   df.writePartitionedBy(Seq("a"), Seq("b"), Seq("c"), Some(10), Seq($"a", concat($"b", $"c")))
     * }}}
     *
     * is equivalent to:
     * {{{
     *   df.repartitionByRange(10, $"a", $"b")
     *     .sortWithinPartitions($"a", $"b", $"c")
     *     .select($"a", concat($"b", $"c"))
     *     .write
     *     .partitionBy("a")
     * }}}
     *
     * @param partitionColumns  columns used for partitioning
     * @param moreFileColumns   columns where individual values are written to a single file
     * @param moreFileOrder     additional columns to sort partition files
     * @param partitions        optional number of partition files
     * @param writtenProjection additional transformation to be applied before calling write
     * @return configured DataFrameWriter
     */
    def writePartitionedBy(partitionColumns: Seq[Column],
                           moreFileColumns: Seq[Column] = Seq.empty,
                           moreFileOrder: Seq[Column] = Seq.empty,
                           partitions: Option[Int] = None,
                           writtenProjection: Option[Seq[Column]] = None): DataFrameWriter[Row] = {
      if (partitionColumns.isEmpty)
        throw new IllegalArgumentException(s"partition columns must not be empty")

      if (partitionColumns.exists(!_.expr.isInstanceOf[NamedExpression]))
        throw new IllegalArgumentException(s"partition columns must be named: ${partitionColumns.mkString(",")}")

      val partitionColumnsMap = partitionColumns.map(c => c.expr.asInstanceOf[NamedExpression].name -> c).toMap
      val partitionColumnNames = partitionColumnsMap.keys.map(col).toSeq
      val rangeColumns = partitionColumnNames ++ moreFileColumns
      val sortColumns = partitionColumnNames ++ moreFileColumns ++ moreFileOrder
      ds.toDF
        .call(ds => partitionColumnsMap.foldLeft(ds) { case (ds, (name, col)) => ds.withColumn(name, col) })
        .when(partitions.isEmpty).call(_.repartitionByRange(rangeColumns: _*))
        .when(partitions.isDefined).call(_.repartitionByRange(partitions.get, rangeColumns: _*))
        .sortWithinPartitions(sortColumns: _*)
        .when(writtenProjection.isDefined).call(_.select(writtenProjection.get: _*))
        .write
        .partitionBy(partitionColumnsMap.keys.toSeq: _*)
    }

    def groupBySorted[K: Ordering : Encoder](cols: Column*)(order: Column*): SortedGroupByDataset[K, T] = {
      implicit val encoder: Encoder[(K, T)] = Encoders.tuple(implicitly[Encoder[K]], implicitly[Encoder[T]])
      SortedGroupByDataset(ds, cols, order, None)
    }

    def groupBySorted[K: Ordering : Encoder](partitions: Int)(cols: Column*)(order: Column*): SortedGroupByDataset[K, T] = {
      implicit val encoder: Encoder[(K, T)] = Encoders.tuple(implicitly[Encoder[K]], implicitly[Encoder[T]])
      SortedGroupByDataset(ds, cols, order, Some(partitions))
    }

    def groupByKeySorted[K: Ordering : Encoder, O: Encoder](key: T => K, partitions: Int)(order: T => O): SortedGroupByDataset[K, T] =
      groupByKeySorted(key, Some(partitions))(order)

    def groupByKeySorted[K: Ordering : Encoder, O: Encoder](key: T => K, partitions: Int)(order: T => O, reverse: Boolean): SortedGroupByDataset[K, T] =
      groupByKeySorted(key, Some(partitions))(order, reverse)

    def groupByKeySorted[K: Ordering : Encoder, O: Encoder](key: T => K, partitions: Option[Int] = None)(order: T => O, reverse: Boolean = false): SortedGroupByDataset[K, T] = {
      SortedGroupByDataset(ds, key, order, partitions, reverse)
    }

    @scala.annotation.varargs
    def withRowNumbers(cols: Column*): DataFrame =
      addRowNumbers(cols=cols)

    @scala.annotation.varargs
    def withRowNumbers(rowNumberColumnName: String, cols: Column*): DataFrame =
      addRowNumbers(rowNumberColumnName=rowNumberColumnName, cols=cols)

    @scala.annotation.varargs
    def withRowNumbers(storageLevel: StorageLevel, cols: Column*): DataFrame =
      addRowNumbers(storageLevel=storageLevel, cols=cols)

    @scala.annotation.varargs
    def withRowNumbers(unpersistHandle: UnpersistHandle, cols: Column*): DataFrame =
      addRowNumbers(unpersistHandle=unpersistHandle, cols=cols)

    @scala.annotation.varargs
    def withRowNumbers(rowNumberColumnName: String,
                       storageLevel: StorageLevel,
                       cols: Column*): DataFrame =
      addRowNumbers(rowNumberColumnName=rowNumberColumnName, storageLevel=storageLevel, cols=cols)

    @scala.annotation.varargs
    def withRowNumbers(rowNumberColumnName: String,
                       unpersistHandle: UnpersistHandle,
                       cols: Column*): DataFrame =
      addRowNumbers(rowNumberColumnName=rowNumberColumnName, unpersistHandle=unpersistHandle, cols=cols)

    @scala.annotation.varargs
    def withRowNumbers(storageLevel: StorageLevel,
                       unpersistHandle: UnpersistHandle,
                       cols: Column*): DataFrame =
      addRowNumbers(storageLevel=storageLevel, unpersistHandle=unpersistHandle, cols=cols)

    @scala.annotation.varargs
    def withRowNumbers(rowNumberColumnName: String,
                       storageLevel: StorageLevel,
                       unpersistHandle: UnpersistHandle,
                       cols: Column*): DataFrame =
      addRowNumbers(rowNumberColumnName, storageLevel, unpersistHandle, cols)

    private def addRowNumbers(rowNumberColumnName: String = "row_number",
                              storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
                              unpersistHandle: UnpersistHandle = UnpersistHandle.Noop,
                              cols: Seq[Column] = Seq.empty): DataFrame = {
      // define some column names that do not exist in ds
      val prefix = distinctPrefixFor(ds.columns)
      val monoIdColumnName = prefix + "mono_id"
      val partitionIdColumnName = prefix + "partition_id"
      val localRowNumberColumnName = prefix + "local_row_number"
      val maxLocalRowNumberColumnName = prefix + "max_local_row_number"
      val cumRowNumbersColumnName = prefix + "cum_row_numbers"
      val partitionOffsetColumnName = prefix + "partition_offset"

      // if no order is given, we preserve existing order
      val dfOrdered = if (cols.isEmpty) ds.withColumn(monoIdColumnName, monotonically_increasing_id()) else ds.orderBy(cols: _*)
      val order = if (cols.isEmpty) Seq(col(monoIdColumnName)) else cols

      // add partition ids and local row numbers
      val localRowNumberWindow = Window.partitionBy(partitionIdColumnName).orderBy(order: _*)
      val dfWithPartitionId = dfOrdered
        .withColumn(partitionIdColumnName, spark_partition_id())
        .persist(storageLevel)
      unpersistHandle.setDataFrame(dfWithPartitionId)
      val dfWithLocalRowNumbers = dfWithPartitionId
        .withColumn(localRowNumberColumnName, functions.row_number().over(localRowNumberWindow))

      // compute row offset for the partitions
      val cumRowNumbersWindow = Window.orderBy(partitionIdColumnName)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
      val partitionOffsets = dfWithLocalRowNumbers
        .groupBy(partitionIdColumnName)
        .agg(max(localRowNumberColumnName).alias(maxLocalRowNumberColumnName))
        .withColumn(cumRowNumbersColumnName, sum(maxLocalRowNumberColumnName).over(cumRowNumbersWindow))
        .select(col(partitionIdColumnName) + 1 as partitionIdColumnName, col(cumRowNumbersColumnName).as(partitionOffsetColumnName))

      // compute global row number by adding local row number with partition offset
      val partitionOffsetColumn = coalesce(col(partitionOffsetColumnName), lit(0))
      dfWithLocalRowNumbers.join(partitionOffsets, Seq(partitionIdColumnName), "left")
        .withColumn(rowNumberColumnName, col(localRowNumberColumnName) + partitionOffsetColumn)
        .drop(monoIdColumnName, partitionIdColumnName, localRowNumberColumnName, partitionOffsetColumnName)
    }

  }

  class UnpersistHandle {
    var df: Option[DataFrame] = None

    def setDataFrame(dataframe: DataFrame): Unit = {
      if (df.isDefined) throw new IllegalStateException("DataFrame has been set already. It cannot be reused once used with withRowNumbers.")
      this.df = Some(dataframe)
    }

    def apply(): Unit = {
      this.df.getOrElse(throw new IllegalStateException("DataFrame has to be set first")).unpersist()
    }

    def apply(blocking: Boolean): Unit = {
      this.df.getOrElse(throw new IllegalStateException("DataFrame has to be set first")).unpersist(blocking)
    }
  }

  object UnpersistHandle {
    def apply(): UnpersistHandle = new UnpersistHandle()
    def Noop = new UnpersistHandle()
  }

  /**
   * Implicit class to extend a Spark Dataframe, which is a Dataset[Row].
   *
   * @param df dataframe
   */
  implicit class ExtendedDataframe(df: DataFrame) extends ExtendedDataset[Row](df)(RowEncoder(df.schema))

}
