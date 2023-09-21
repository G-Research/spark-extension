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

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Dataset, functions}
import org.apache.spark.sql.functions.{coalesce, col, lit, max, monotonically_increasing_id, spark_partition_id, sum}
import org.apache.spark.storage.StorageLevel

case class RowNumbersFunc(rowNumberColumnName: String = "row_number",
                          storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
                          unpersistHandle: UnpersistHandle = UnpersistHandle.Noop,
                          orderColumns: Seq[Column] = Seq.empty) {

  def withRowNumberColumnName(rowNumberColumnName: String): RowNumbersFunc =
    this.copy(rowNumberColumnName = rowNumberColumnName)

  def withStorageLevel(storageLevel: StorageLevel): RowNumbersFunc =
    this.copy(storageLevel = storageLevel)

  def withUnpersistHandle(unpersistHandle: UnpersistHandle): RowNumbersFunc =
    this.copy(unpersistHandle = unpersistHandle)

  def withOrderColumns(orderColumns: Seq[Column]): RowNumbersFunc =
    this.copy(orderColumns = orderColumns)

  def of[D](df: Dataset[D]): DataFrame = {
    if (storageLevel.equals(StorageLevel.NONE) && (SparkMajorVersion > 3 || SparkMajorVersion == 3 && SparkMinorVersion >= 5)) {
      throw new IllegalArgumentException(s"Storage level $storageLevel not supported with Spark 3.5.0 and above.")
    }

    // define some column names that do not exist in ds
    val prefix = distinctPrefixFor(df.columns)
    val monoIdColumnName = prefix + "mono_id"
    val partitionIdColumnName = prefix + "partition_id"
    val localRowNumberColumnName = prefix + "local_row_number"
    val maxLocalRowNumberColumnName = prefix + "max_local_row_number"
    val cumRowNumbersColumnName = prefix + "cum_row_numbers"
    val partitionOffsetColumnName = prefix + "partition_offset"

    // if no order is given, we preserve existing order
    val dfOrdered = if (orderColumns.isEmpty) df.withColumn(monoIdColumnName, monotonically_increasing_id()) else df.orderBy(orderColumns: _*)
    val order = if (orderColumns.isEmpty) Seq(col(monoIdColumnName)) else orderColumns

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

object RowNumbers {
  def default(): RowNumbersFunc = RowNumbersFunc()

  def withRowNumberColumnName(rowNumberColumnName: String): RowNumbersFunc =
    default().withRowNumberColumnName(rowNumberColumnName)

  def withStorageLevel(storageLevel: StorageLevel): RowNumbersFunc =
    default().withStorageLevel(storageLevel)

  def withUnpersistHandle(unpersistHandle: UnpersistHandle): RowNumbersFunc =
    default().withUnpersistHandle(unpersistHandle)

  @scala.annotation.varargs
  def withOrderColumns(orderColumns: Column*): RowNumbersFunc =
    default().withOrderColumns(orderColumns)

  def of[D](ds: Dataset[D]): DataFrame = default().of(ds)
}
