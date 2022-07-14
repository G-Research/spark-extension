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
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel
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

    /**
     * Groups the Dataset and sorts the groups using the specified columns, so we can run
     * further process the sorted groups. See [[SortedGroupByDataset]] for all the available
     * functions.
     *
     * {{{
     *   // Enumerate elements in the sorted group
     *   ds.groupBySorted($"department")($"salery")
     *     .flatMapSortedGroups((key, it) => it.zipWithIndex)
     * }}}
     *
     * @param cols grouping columns
     * @param order sort columns
     */
    def groupBySorted[K: Ordering : Encoder](cols: Column*)(order: Column*): SortedGroupByDataset[K, T] = {
      implicit val encoder: Encoder[(K, T)] = Encoders.tuple(implicitly[Encoder[K]], implicitly[Encoder[T]])
      SortedGroupByDataset(ds, cols, order, None)
    }

    /**
     * Groups the Dataset and sorts the groups using the specified columns, so we can run
     * further process the sorted groups. See [[SortedGroupByDataset]] for all the available
     * functions.
     *
     * {{{
     *   // Enumerate elements in the sorted group
     *   ds.groupBySorted(10)($"department")($"salery")
     *     .flatMapSortedGroups((key, it) => it.zipWithIndex)
     * }}}
     *
     * @param partitions number of partitions
     * @param cols grouping columns
     * @param order sort columns
     */
    def groupBySorted[K: Ordering : Encoder](partitions: Int)(cols: Column*)(order: Column*): SortedGroupByDataset[K, T] = {
      implicit val encoder: Encoder[(K, T)] = Encoders.tuple(implicitly[Encoder[K]], implicitly[Encoder[T]])
      SortedGroupByDataset(ds, cols, order, Some(partitions))
    }

    /**
     * Groups the Dataset and sorts the groups using the specified columns, so we can run
     * further process the sorted groups. See [[SortedGroupByDataset]] for all the available
     * functions.
     *
     * {{{
     *   // Enumerate elements in the sorted group
     *   ds.groupByKeySorted(row => row.getInt(0), 10)(row => row.getInt(1))
     *     .flatMapSortedGroups((key, it) => it.zipWithIndex)
     * }}}
     *
     * @param partitions number of partitions
     * @param key grouping key
     * @param order sort key
     */
    def groupByKeySorted[K: Ordering : Encoder, O: Encoder](key: T => K, partitions: Int)(order: T => O): SortedGroupByDataset[K, T] =
      groupByKeySorted(key, Some(partitions))(order)

    /**
     * Groups the Dataset and sorts the groups using the specified columns, so we can run
     * further process the sorted groups. See [[SortedGroupByDataset]] for all the available
     * functions.
     *
     * {{{
     *   // Enumerate elements in the sorted group
     *   ds.groupByKeySorted(row => row.getInt(0), 10)(row => row.getInt(1), true)
     *     .flatMapSortedGroups((key, it) => it.zipWithIndex)
     * }}}
     *
     * @param partitions number of partitions
     * @param key grouping key
     * @param order sort key
     * @param reverse sort reverse order
     */
    def groupByKeySorted[K: Ordering : Encoder, O: Encoder](key: T => K, partitions: Int)(order: T => O, reverse: Boolean): SortedGroupByDataset[K, T] =
      groupByKeySorted(key, Some(partitions))(order, reverse)

    /**
     * Groups the Dataset and sorts the groups using the specified columns, so we can run
     * further process the sorted groups. See [[SortedGroupByDataset]] for all the available
     * functions.
     *
     * {{{
     *   // Enumerate elements in the sorted group
     *   ds.groupByKeySorted(row => row.getInt(0))(row => row.getInt(1), true)
     *     .flatMapSortedGroups((key, it) => it.zipWithIndex)
     * }}}
     *
     * @param partitions optional number of partitions
     * @param key grouping key
     * @param order sort key
     * @param reverse sort reverse order
     */
    def groupByKeySorted[K: Ordering : Encoder, O: Encoder](key: T => K, partitions: Option[Int] = None)(order: T => O, reverse: Boolean = false): SortedGroupByDataset[K, T] = {
      SortedGroupByDataset(ds, key, order, partitions, reverse)
    }

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[ExtendedDataset#withRowNumbers(String, StorageLevel, UnpersistHandle, Column*)]] for details.
     */
    def withRowNumbers(order: Column*): DataFrame =
      RowNumbers.withOrderColumns(order: _*).of(ds)

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[ExtendedDataset#withRowNumbers(String, StorageLevel, UnpersistHandle, Column*)]] for details.
     */
    def withRowNumbers(rowNumberColumnName: String, order: Column*): DataFrame =
      RowNumbers.withRowNumberColumnName(rowNumberColumnName).withOrderColumns(order).of(ds)

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[ExtendedDataset#withRowNumbers(String, StorageLevel, UnpersistHandle, Column*)]] for details.
     */
    def withRowNumbers(storageLevel: StorageLevel, order: Column*): DataFrame =
      RowNumbers.withStorageLevel(storageLevel).withOrderColumns(order).of(ds)

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[ExtendedDataset#withRowNumbers(String, StorageLevel, UnpersistHandle, Column*)]] for details.
     */
    def withRowNumbers(unpersistHandle: UnpersistHandle, order: Column*): DataFrame =
      RowNumbers.withUnpersistHandle(unpersistHandle).withOrderColumns(order).of(ds)

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[ExtendedDataset#withRowNumbers(String, StorageLevel, UnpersistHandle, Column*)]] for details.
     */
    def withRowNumbers(rowNumberColumnName: String,
                       storageLevel: StorageLevel,
                       order: Column*): DataFrame =
      RowNumbers.withRowNumberColumnName(rowNumberColumnName).withStorageLevel(storageLevel).withOrderColumns(order).of(ds)

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[ExtendedDataset#withRowNumbers(String, StorageLevel, UnpersistHandle, Column*)]] for details.
     */
    def withRowNumbers(rowNumberColumnName: String,
                       unpersistHandle: UnpersistHandle,
                       order: Column*): DataFrame =
      RowNumbers.withRowNumberColumnName(rowNumberColumnName).withUnpersistHandle(unpersistHandle).withOrderColumns(order).of(ds)

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[ExtendedDataset#withRowNumbers(String, StorageLevel, UnpersistHandle, Column*)]] for details.
     */
    def withRowNumbers(storageLevel: StorageLevel,
                       unpersistHandle: UnpersistHandle,
                       order: Column*): DataFrame =
      RowNumbers.withStorageLevel(storageLevel).withUnpersistHandle(unpersistHandle).withOrderColumns(order).of(ds)

    /**
     * Adds a global continuous row number starting at 1, after sorting rows by the given columns.
     * When no columns are given, the existing order is used.
     *
     * Hence, the following examples are equivalent:
     * {{{
     *   ds.withRowNumbers($"a".desc, $"b")
     *   ds.orderBy($"a".desc, $"b").withRowNumbers()
     * }}}
     *
     * The column name of the column with the row numbers can be set via the `rowNumberColumnName` argument.
     *
     * To avoid some known issues optimizing the query plan, this function has to internally call
     * `Dataset.persist(StorageLevel)` on an intermediate DataFrame. The storage level of that cached
     * DataFrame can be set via `storageLevel`, where the default is `StorageLevel.MEMORY_AND_DISK`.
     *
     * That cached intermediate DataFrame can be un-persisted / un-cached as follows:
     * {{{
     *   import uk.co.gresearch.spark.UnpersistHandle
     *
     *   val unpersist = UnpersistHandle()
     *   ds.withRowNumbers(unpersist).show()
     *   unpersist()
     * }}}
     *
     * @param rowNumberColumnName name of the row number column
     * @param storageLevel storage level of the cached intermediate DataFrame
     * @param unpersistHandle handle to un-persist intermediate DataFrame
     * @param order columns to order dataframe before assigning row numbers
     * @return dataframe with row numbers
     */
    def withRowNumbers(rowNumberColumnName: String,
                       storageLevel: StorageLevel,
                       unpersistHandle: UnpersistHandle,
                       order: Column*): DataFrame =
      RowNumbers.withRowNumberColumnName(rowNumberColumnName).withStorageLevel(storageLevel).withUnpersistHandle(unpersistHandle).withOrderColumns(order).of(ds)
  }

  /**
   * Implicit class to extend a Spark Dataframe, which is a Dataset[Row].
   *
   * @param df dataframe
   */
  implicit class ExtendedDataframe(df: DataFrame) extends ExtendedDataset[Row](df)(RowEncoder(df.schema))

}
