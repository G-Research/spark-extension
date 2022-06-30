package uk.co.gresearch.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Dataset, functions}
import org.apache.spark.sql.functions.{coalesce, col, lit, max, monotonically_increasing_id, spark_partition_id, sum}
import org.apache.spark.storage.StorageLevel

object RowNumbers {
  def of[D](df: Dataset[D],
            rowNumberColumnName: String = "row_number",
            storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
            unpersistHandle: UnpersistHandle = UnpersistHandle.Noop,
            orderColumns: Seq[Column] = Seq.empty): DataFrame = {
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
