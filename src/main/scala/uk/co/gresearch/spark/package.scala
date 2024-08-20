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

import org.apache.spark.extension.ExpressionExtension
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions.{col, count, lit, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, LongType, TimestampType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkFiles}
import uk.co.gresearch.spark.group.SortedGroupByDataset

import java.nio.file.{Files, Paths}

package object spark extends Logging with SparkVersion with BuildVersion {

  /**
   * Provides a prefix that makes any string distinct w.r.t. the given strings.
   * @param existing
   *   strings
   * @return
   *   distinct prefix
   */
  private[spark] def distinctPrefixFor(existing: Seq[String]): String = {
    "_" * (existing.map(_.takeWhile(_ == '_').length).reduceOption(_ max _).getOrElse(0) + 1)
  }

  /**
   * Create a temporary directory in a location (driver temp dir) that will be deleted on Spark application shutdown.
   * @param prefix
   *   prefix string of temporary directory name
   * @return
   *   absolute path of temporary directory
   */
  def createTemporaryDir(prefix: String): String = {
    // SparkFiles.getRootDirectory() will be deleted on spark application shutdown
    Files.createTempDirectory(Paths.get(SparkFiles.getRootDirectory()), prefix).toAbsolutePath.toString
  }

  // https://issues.apache.org/jira/browse/SPARK-40588
  private[spark] def writePartitionedByRequiresCaching[T](ds: Dataset[T]): Boolean = {
    ds.sparkSession.conf
      .get(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key,
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.defaultValue.getOrElse(true).toString
      )
      .equalsIgnoreCase("true") && Some(ds.sparkSession.version).exists(ver =>
      Set("3.0.", "3.1.", "3.2.0", "3.2.1", "3.2.2", "3.3.0", "3.3.1").exists(pat =>
        if (pat.endsWith(".")) { ver.startsWith(pat) }
        else { ver.equals(pat) || ver.startsWith(pat + "-") }
      )
    )
  }

  private[spark] def info(msg: String): Unit = logInfo(msg)
  private[spark] def warning(msg: String): Unit = logWarning(msg)

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
   * @param string
   *   a string
   * @param strings
   *   more strings
   */
  @scala.annotation.varargs
  def backticks(string: String, strings: String*): String =
    Backticks.column_name(string, strings: _*)

  /**
   * Aggregate function: returns the number of items in a group that are not null.
   */
  def count_null(e: Column): Column = count(when(e.isNull, lit(1)))

  private val nanoSecondsPerDotNetTick: Long = 100
  private val dotNetTicksPerSecond: Long = 10000000
  private val unixEpochDotNetTicks: Long = 621355968000000000L

  /**
   * Convert a .Net `DateTime.Ticks` timestamp to a Spark timestamp. The input column must be convertible to a number
   * (e.g. string, int, long). The Spark timestamp type does not support nanoseconds, so the the last digit of the
   * timestamp (1/10 of a microsecond) is lost.
   *
   * Example:
   * {{{
   *   df.select($"ticks", dotNetTicksToTimestamp($"ticks").as("timestamp")).show(false)
   * }}}
   *
   * | ticks              | timestamp                  |
   * |:-------------------|:---------------------------|
   * | 638155413748959318 | 2023-03-27 21:16:14.895931 |
   *
   * Note: the example timestamp lacks the 8/10 of a microsecond. Use `dotNetTicksToUnixEpoch` to preserve the full
   * precision of the tick timestamp.
   *
   * https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks
   *
   * @param tickColumn
   *   column with a tick value
   * @return
   *   result timestamp column
   */
  def dotNetTicksToTimestamp(tickColumn: Column): Column =
    dotNetTicksToUnixEpoch(tickColumn).cast(TimestampType)

  /**
   * Convert a .Net `DateTime.Ticks` timestamp to a Spark timestamp. The input column must be convertible to a number
   * (e.g. string, int, long). The Spark timestamp type does not support nanoseconds, so the the last digit of the
   * timestamp (1/10 of a microsecond) is lost.
   *
   * {{{
   *   df.select($"ticks", dotNetTicksToTimestamp("ticks").as("timestamp")).show(false)
   * }}}
   *
   * | ticks              | timestamp                  |
   * |:-------------------|:---------------------------|
   * | 638155413748959318 | 2023-03-27 21:16:14.895931 |
   *
   * Note: the example timestamp lacks the 8/10 of a microsecond. Use `dotNetTicksToUnixEpoch` to preserve the full
   * precision of the tick timestamp.
   *
   * https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks
   *
   * @param tickColumnName
   *   name of a column with a tick value
   * @return
   *   result timestamp column
   */
  def dotNetTicksToTimestamp(tickColumnName: String): Column = dotNetTicksToTimestamp(col(tickColumnName))

  /**
   * Convert a .Net `DateTime.Ticks` timestamp to a Unix epoch decimal. The input column must be convertible to a number
   * (e.g. string, int, long). The full precision of the tick timestamp is preserved (1/10 of a microsecond).
   *
   * Example:
   * {{{
   *   df.select($"ticks", dotNetTicksToUnixEpoch($"ticks").as("timestamp")).show(false)
   * }}}
   *
   * | ticks              | timestamp            |
   * |:-------------------|:---------------------|
   * | 638155413748959318 | 1679944574.895931800 |
   *
   * https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks
   *
   * @param tickColumn
   *   column with a tick value
   * @return
   *   result unix epoch seconds column as decimal
   */
  def dotNetTicksToUnixEpoch(tickColumn: Column): Column =
    (tickColumn.cast(DecimalType(19, 0)) - unixEpochDotNetTicks) / dotNetTicksPerSecond

  /**
   * Convert a .Net `DateTime.Ticks` timestamp to a Unix epoch seconds. The input column must be convertible to a number
   * (e.g. string, int, long). The full precision of the tick timestamp is preserved (1/10 of a microsecond).
   *
   * Example:
   * {{{
   *   df.select($"ticks", dotNetTicksToUnixEpoch("ticks").as("timestamp")).show(false)
   * }}}
   *
   * | ticks              | timestamp            |
   * |:-------------------|:---------------------|
   * | 638155413748959318 | 1679944574.895931800 |
   *
   * https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks
   *
   * @param tickColumnName
   *   name of column with a tick value
   * @return
   *   result unix epoch seconds column as decimal
   */
  def dotNetTicksToUnixEpoch(tickColumnName: String): Column = dotNetTicksToUnixEpoch(col(tickColumnName))

  /**
   * Convert a .Net `DateTime.Ticks` timestamp to a Unix epoch seconds. The input column must be convertible to a number
   * (e.g. string, int, long). The full precision of the tick timestamp is preserved (1/10 of a microsecond).
   *
   * Example:
   * {{{
   *   df.select($"ticks", dotNetTicksToUnixEpochNanos($"ticks").as("timestamp")).show(false)
   * }}}
   *
   * | ticks              | timestamp           |
   * |:-------------------|:--------------------|
   * | 638155413748959318 | 1679944574895931800 |
   *
   * https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks
   *
   * @param tickColumn
   *   column with a tick value
   * @return
   *   result unix epoch nanoseconds column as long
   */
  def dotNetTicksToUnixEpochNanos(tickColumn: Column): Column = {
    when(
      tickColumn <= 713589688368547758L,
      (tickColumn.cast(LongType) - unixEpochDotNetTicks) * nanoSecondsPerDotNetTick
    )
  }

  /**
   * Convert a .Net `DateTime.Ticks` timestamp to a Unix epoch nanoseconds. The input column must be convertible to a
   * number (e.g. string, int, long). The full precision of the tick timestamp is preserved (1/10 of a microsecond).
   *
   * Example:
   * {{{
   *   df.select($"ticks", dotNetTicksToUnixEpochNanos("ticks").as("timestamp")).show(false)
   * }}}
   *
   * | ticks              | timestamp           |
   * |:-------------------|:--------------------|
   * | 638155413748959318 | 1679944574895931800 |
   *
   * https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks
   *
   * @param tickColumnName
   *   name of column with a tick value
   * @return
   *   result unix epoch nanoseconds column as long
   */
  def dotNetTicksToUnixEpochNanos(tickColumnName: String): Column = dotNetTicksToUnixEpochNanos(col(tickColumnName))

  /**
   * Convert a Spark timestamp to a .Net `DateTime.Ticks` timestamp. The input column must be of TimestampType.
   *
   * Example:
   * {{{
   *   df.select($"timestamp", timestampToDotNetTicks($"timestamp").as("ticks")).show(false)
   * }}}
   *
   * | timestamp                  | ticks              |
   * |:---------------------------|:-------------------|
   * | 2023-03-27 21:16:14.895931 | 638155413748959310 |
   *
   * https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks
   *
   * @param timestampColumn
   *   column with a timestamp value
   * @return
   *   result tick value column
   */
  def timestampToDotNetTicks(timestampColumn: Column): Column =
    unixEpochTenthMicrosToDotNetTicks(UnixMicros.unixMicros(timestampColumn.expr).toColumn * 10)

  /**
   * Convert a Spark timestamp to a .Net `DateTime.Ticks` timestamp. The input column must be of TimestampType.
   *
   * Example:
   * {{{
   *   df.select($"timestamp", timestampToDotNetTicks("timestamp").as("ticks")).show(false)
   * }}}
   *
   * | timestamp                  | ticks              |
   * |:---------------------------|:-------------------|
   * | 2023-03-27 21:16:14.895931 | 638155413748959310 |
   *
   * https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks
   *
   * @param timestampColumnName
   *   name of column with a timestamp value
   * @return
   *   result tick value column
   */
  def timestampToDotNetTicks(timestampColumnName: String): Column = timestampToDotNetTicks(col(timestampColumnName))

  /**
   * Convert a Unix epoch timestamp to a .Net `DateTime.Ticks` timestamp. The input column must represent a numerical
   * unix epoch timestamp, e.g. long, double, string or decimal. The input must not be of TimestampType, as that may be
   * interpreted incorrectly. Use `timestampToDotNetTicks` for TimestampType columns instead.
   *
   * Example:
   * {{{
   *   df.select($"unix", unixEpochToDotNetTicks($"unix").as("ticks")).show(false)
   * }}}
   *
   * | unix                          | ticks              |
   * |:------------------------------|:-------------------|
   * | 1679944574.895931234000000000 | 638155413748959312 |
   *
   * https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks
   *
   * @param unixTimeColumn
   *   column with a unix epoch timestamp value
   * @return
   *   result tick value column
   */
  def unixEpochToDotNetTicks(unixTimeColumn: Column): Column = unixEpochTenthMicrosToDotNetTicks(
    unixTimeColumn.cast(DecimalType(19, 7)) * 10000000
  )

  /**
   * Convert a Unix epoch timestamp to a .Net `DateTime.Ticks` timestamp. The input column must represent a numerical
   * unix epoch timestamp, e.g. long, double, string or decimal. The input must not be of TimestampType, as that may be
   * interpreted incorrectly. Use `timestampToDotNetTicks` for TimestampType columns instead.
   *
   * Example:
   * {{{
   *   df.select($"unix", unixEpochToDotNetTicks("unix").as("ticks")).show(false)
   * }}}
   *
   * | unix                          | ticks              |
   * |:------------------------------|:-------------------|
   * | 1679944574.895931234000000000 | 638155413748959312 |
   *
   * https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks
   *
   * @param unixTimeColumnName
   *   name of column with a unix epoch timestamp value
   * @return
   *   result tick value column
   */
  def unixEpochToDotNetTicks(unixTimeColumnName: String): Column = unixEpochToDotNetTicks(col(unixTimeColumnName))

  /**
   * Convert a Unix epoch nanosecond timestamp to a .Net `DateTime.Ticks` timestamp. The .Net ticks timestamp does not
   * support the two lowest nanosecond digits, so only a 1/10 of a microsecond is the smallest resolution. The input
   * column must represent a numerical unix epoch nanoseconds timestamp, e.g. long, double, string or decimal.
   *
   * Example:
   * {{{
   *   df.select($"unix_nanos", unixEpochNanosToDotNetTicks($"unix_nanos").as("ticks")).show(false)
   * }}}
   *
   * | unix_nanos          | ticks              |
   * |:--------------------|:-------------------|
   * | 1679944574895931234 | 638155413748959312 |
   *
   * Note: the example timestamp lacks the two lower nanosecond digits as this precision is not supported by .Net ticks.
   *
   * https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks
   *
   * @param unixNanosColumn
   *   column with a unix epoch timestamp value
   * @return
   *   result tick value column
   */
  def unixEpochNanosToDotNetTicks(unixNanosColumn: Column): Column = unixEpochTenthMicrosToDotNetTicks(
    unixNanosColumn.cast(DecimalType(21, 0)) / nanoSecondsPerDotNetTick
  )

  /**
   * Convert a Unix epoch nanosecond timestamp to a .Net `DateTime.Ticks` timestamp. The .Net ticks timestamp does not
   * support the two lowest nanosecond digits, so only a 1/10 of a microsecond is the smallest resolution. The input
   * column must represent a numerical unix epoch nanoseconds timestamp, e.g. long, double, string or decimal.
   *
   * Example:
   * {{{
   *   df.select($"unix_nanos", unixEpochNanosToDotNetTicks($"unix_nanos").as("ticks")).show(false)
   * }}}
   *
   * | unix_nanos          | ticks              |
   * |:--------------------|:-------------------|
   * | 1679944574895931234 | 638155413748959312 |
   *
   * Note: the example timestamp lacks the two lower nanosecond digits as this precision is not supported by .Net ticks.
   *
   * https://learn.microsoft.com/de-de/dotnet/api/system.datetime.ticks
   *
   * @param unixNanosColumnName
   *   name of column with a unix epoch timestamp value
   * @return
   *   result tick value column
   */
  def unixEpochNanosToDotNetTicks(unixNanosColumnName: String): Column = unixEpochNanosToDotNetTicks(
    col(unixNanosColumnName)
  )

  private def unixEpochTenthMicrosToDotNetTicks(unixNanosColumn: Column): Column =
    unixNanosColumn.cast(LongType) + unixEpochDotNetTicks

  /**
   * Set the job description and return the earlier description. Only set the description if it is not set.
   *
   * @param description
   *   job description
   * @param ifNotSet
   *   job description is only set if no description is set yet
   * @param context
   *   spark context
   * @return
   */
  def setJobDescription(description: String, ifNotSet: Boolean = false)(implicit context: SparkContext): String = {
    val earlierDescriptionOption = Option(context.getLocalProperty("spark.job.description"))
    if (earlierDescriptionOption.isEmpty || !ifNotSet) {
      context.setJobDescription(description)
    }
    earlierDescriptionOption.orNull
  }

  /**
   * Adds a job description to all Spark jobs started within the given function. The current Job description is restored
   * after exit of the function.
   *
   * Usage example:
   *
   * {{{
   *   import uk.co.gresearch.spark._
   *
   *   implicit val session: SparkSession = spark
   *
   *   val count = withJobDescription("parquet file") {
   *     val df = spark.read.parquet("data.parquet")
   *     df.count
   *   }
   * }}}
   *
   * With `ifNotSet == true`, the description is only set if no job description is set yet.
   *
   * Any modification to the job description during execution of the function is reverted, even if `ifNotSet == true`.
   *
   * @param description
   *   job description
   * @param ifNotSet
   *   job description is only set if no description is set yet
   * @param func
   *   code to execute while job description is set
   * @param session
   *   spark session
   * @tparam T
   *   return type of func
   */
  def withJobDescription[T](description: String, ifNotSet: Boolean = false)(
      func: => T
  )(implicit session: SparkSession): T = {
    val earlierDescription = setJobDescription(description, ifNotSet)(session.sparkContext)
    try {
      func
    } finally {
      setJobDescription(earlierDescription)(session.sparkContext)
    }
  }

  /**
   * Append the job description and return the earlier description.
   *
   * @param extraDescription
   *   job description
   * @param separator
   *   separator to join exiting and extra description with
   * @param context
   *   spark context
   * @return
   */
  def appendJobDescription(extraDescription: String, separator: String, context: SparkContext): String = {
    val earlierDescriptionOption = Option(context.getLocalProperty("spark.job.description"))
    val description = earlierDescriptionOption.map(_ + separator + extraDescription).getOrElse(extraDescription)
    context.setJobDescription(description)
    earlierDescriptionOption.orNull
  }

  /**
   * Appends a job description to all Spark jobs started within the given function. The current Job description is
   * extended by the separator and the extra description on entering the function, and restored after exit of the
   * function.
   *
   * Usage example:
   *
   * {{{
   *   import uk.co.gresearch.spark._
   *
   *   implicit val session: SparkSession = spark
   *
   *   val count = appendJobDescription("parquet file") {
   *     val df = spark.read.parquet("data.parquet")
   *     appendJobDescription("count") {
   *       df.count
   *     }
   *   }
   * }}}
   *
   * Any modification to the job description during execution of the function is reverted.
   *
   * @param extraDescription
   *   job description to be appended
   * @param separator
   *   separator used when appending description
   * @param func
   *   code to execute while job description is set
   * @param session
   *   spark session
   * @tparam T
   *   return type of func
   */
  def appendJobDescription[T](extraDescription: String, separator: String = " - ")(
      func: => T
  )(implicit session: SparkSession): T = {
    val earlierDescription = appendJobDescription(extraDescription, separator, session.sparkContext)
    try {
      func
    } finally {
      setJobDescription(earlierDescription)(session.sparkContext)
    }
  }

  /**
   * Class to extend a Spark Dataset.
   *
   * @param ds
   *   dataset
   * @tparam V
   *   inner type of dataset
   */
  @deprecated(
    "Constructor with encoder is deprecated, the encoder argument is ignored, ds.encoder is used instead.",
    since = "2.9.0"
  )
  class ExtendedDataset[V](ds: Dataset[V], encoder: Encoder[V]) {
    private val eds = ExtendedDatasetV2[V](ds)

    def histogram[T: Ordering](thresholds: Seq[T], valueColumn: Column, aggregateColumns: Column*): DataFrame =
      eds.histogram(thresholds, valueColumn, aggregateColumns: _*)

    def writePartitionedBy(
        partitionColumns: Seq[Column],
        moreFileColumns: Seq[Column] = Seq.empty,
        moreFileOrder: Seq[Column] = Seq.empty,
        partitions: Option[Int] = None,
        writtenProjection: Option[Seq[Column]] = None,
        unpersistHandle: Option[UnpersistHandle] = None
    ): DataFrameWriter[Row] =
      eds.writePartitionedBy(
        partitionColumns,
        moreFileColumns,
        moreFileOrder,
        partitions,
        writtenProjection,
        unpersistHandle
      )

    def groupBySorted[K: Ordering: Encoder](cols: Column*)(order: Column*): SortedGroupByDataset[K, V] =
      eds.groupBySorted(cols: _*)(order: _*)

    def groupBySorted[K: Ordering: Encoder](partitions: Int)(cols: Column*)(
        order: Column*
    ): SortedGroupByDataset[K, V] =
      eds.groupBySorted(partitions)(cols: _*)(order: _*)

    def groupByKeySorted[K: Ordering: Encoder, O: Encoder](key: V => K, partitions: Int)(
        order: V => O
    ): SortedGroupByDataset[K, V] =
      eds.groupByKeySorted(key, Some(partitions))(order)

    def groupByKeySorted[K: Ordering: Encoder, O: Encoder](key: V => K, partitions: Int)(
        order: V => O,
        reverse: Boolean
    ): SortedGroupByDataset[K, V] =
      eds.groupByKeySorted(key, Some(partitions))(order, reverse)

    def groupByKeySorted[K: Ordering: Encoder, O: Encoder](key: V => K, partitions: Option[Int] = None)(
        order: V => O,
        reverse: Boolean = false
    ): SortedGroupByDataset[K, V] =
      eds.groupByKeySorted(key, partitions)(order, reverse)

    def withRowNumbers(order: Column*): DataFrame =
      eds.withRowNumbers(order: _*)

    def withRowNumbers(rowNumberColumnName: String, order: Column*): DataFrame =
      eds.withRowNumbers(rowNumberColumnName, order: _*)

    def withRowNumbers(storageLevel: StorageLevel, order: Column*): DataFrame =
      eds.withRowNumbers(storageLevel, order: _*)

    def withRowNumbers(unpersistHandle: UnpersistHandle, order: Column*): DataFrame =
      eds.withRowNumbers(unpersistHandle, order: _*)

    def withRowNumbers(rowNumberColumnName: String, storageLevel: StorageLevel, order: Column*): DataFrame =
      eds.withRowNumbers(rowNumberColumnName, storageLevel, order: _*)

    def withRowNumbers(rowNumberColumnName: String, unpersistHandle: UnpersistHandle, order: Column*): DataFrame =
      eds.withRowNumbers(rowNumberColumnName, unpersistHandle, order: _*)

    def withRowNumbers(storageLevel: StorageLevel, unpersistHandle: UnpersistHandle, order: Column*): DataFrame =
      eds.withRowNumbers(storageLevel, unpersistHandle, order: _*)

    def withRowNumbers(
        rowNumberColumnName: String,
        storageLevel: StorageLevel,
        unpersistHandle: UnpersistHandle,
        order: Column*
    ): DataFrame =
      eds.withRowNumbers(rowNumberColumnName, storageLevel, unpersistHandle, order: _*)
  }

  /**
   * Class to extend a Spark Dataset.
   *
   * @param ds
   *   dataset
   * @tparam V
   *   inner type of dataset
   */
  def ExtendedDataset[V](ds: Dataset[V], encoder: Encoder[V]): ExtendedDataset[V] = new ExtendedDataset(ds, encoder)

  /**
   * Implicit class to extend a Spark Dataset.
   *
   * @param ds
   *   dataset
   * @tparam V
   *   inner type of dataset
   */
  implicit class ExtendedDatasetV2[V](ds: Dataset[V]) {
    private implicit val encoder: Encoder[V] = ds.encoder

    /**
     * Compute the histogram of a column when aggregated by aggregate columns. Thresholds are expected to be provided in
     * ascending order. The result dataframe contains the aggregate and histogram columns only. For each threshold value
     * in thresholds, there will be a column named s"≤threshold". There will also be a final column called
     * s">last_threshold", that counts the remaining values that exceed the last threshold.
     *
     * @param thresholds
     *   sequence of thresholds, must implement <= and > operators w.r.t. valueColumn
     * @param valueColumn
     *   histogram is computed for values of this column
     * @param aggregateColumns
     *   histogram is computed against these columns
     * @tparam T
     *   type of histogram thresholds
     * @return
     *   dataframe with aggregate and histogram columns
     */
    def histogram[T: Ordering](thresholds: Seq[T], valueColumn: Column, aggregateColumns: Column*): DataFrame =
      Histogram.of(ds, thresholds, valueColumn, aggregateColumns: _*)

    /**
     * Writes the Dataset / DataFrame via DataFrameWriter.partitionBy. In addition to partitionBy, this method sorts the
     * data to improve partition file size. Small partitions will contain few files, large partitions contain more
     * files. Partition ids are contained in a single partition file per `partitionBy` partition only. Rows within the
     * partition files are also sorted, if partitionOrder is defined.
     *
     * Note: With Spark 3.0, 3.1, 3.2 before 3.2.3, 3.3 before 3.3.2, and AQE enabled, an intermediate DataFrame is
     * being cached in order to guarantee sorted output files. See https://issues.apache.org/jira/browse/SPARK-40588.
     * That cached DataFrame can be unpersisted via an optional [[UnpersistHandle]] provided to this method.
     *
     * Calling:
     * {{{
     *   val unpersist = UnpersistHandle()
     *   val writer = df.writePartitionedBy(Seq("a"), Seq("b"), Seq("c"), Some(10), Seq($"a", concat($"b", $"c")), unpersist)
     *   writer.parquet("data.parquet")
     *   unpersist()
     * }}}
     *
     * is equivalent to:
     * {{{
     *   val cached =
     *     df.repartitionByRange(10, $"a", $"b")
     *       .sortWithinPartitions($"a", $"b", $"c")
     *       .cache
     *
     *   val writer =
     *     cached
     *       .select($"a", concat($"b", $"c"))
     *       .write
     *       .partitionBy("a")
     *
     *   writer.parquet("data.parquet")
     *
     *   cached.unpersist
     * }}}
     *
     * @param partitionColumns
     *   columns used for partitioning
     * @param moreFileColumns
     *   columns where individual values are written to a single file
     * @param moreFileOrder
     *   additional columns to sort partition files
     * @param partitions
     *   optional number of partition files
     * @param writtenProjection
     *   additional transformation to be applied before calling write
     * @param unpersistHandle
     *   handle to unpersist internally created DataFrame after writing
     * @return
     *   configured DataFrameWriter
     */
    def writePartitionedBy(
        partitionColumns: Seq[Column],
        moreFileColumns: Seq[Column] = Seq.empty,
        moreFileOrder: Seq[Column] = Seq.empty,
        partitions: Option[Int] = None,
        writtenProjection: Option[Seq[Column]] = None,
        unpersistHandle: Option[UnpersistHandle] = None
    ): DataFrameWriter[Row] = {
      if (partitionColumns.isEmpty)
        throw new IllegalArgumentException(s"partition columns must not be empty")

      if (partitionColumns.exists(!_.expr.isInstanceOf[NamedExpression]))
        throw new IllegalArgumentException(s"partition columns must be named: ${partitionColumns.mkString(",")}")

      val requiresCaching = writePartitionedByRequiresCaching(ds)
      (requiresCaching, unpersistHandle.isDefined) match {
        case (true, false) =>
          warning(
            "Partitioned-writing with AQE enabled and Spark 3.0, 3.1, 3.2 below 3.2.3, " +
              "and 3.3 below 3.3.2 requires caching an intermediate DataFrame, " +
              "which calling code has to unpersist once writing is done. " +
              "Please provide an UnpersistHandle to DataFrame.writePartitionedBy, or UnpersistHandle.Noop. " +
              "See https://issues.apache.org/jira/browse/SPARK-40588"
          )
        case (false, true) if !unpersistHandle.get.isInstanceOf[NoopUnpersistHandle] =>
          info(
            "UnpersistHandle provided to DataFrame.writePartitionedBy is not needed as " +
              "partitioned-writing with AQE disabled or Spark 3.2.3, 3.3.2 or 3.4 and above " +
              "does not require caching intermediate DataFrame."
          )
          unpersistHandle.get.setDataFrame(ds.sparkSession.emptyDataFrame)
        case _ =>
      }

      val partitionColumnsMap = partitionColumns.map(c => c.expr.asInstanceOf[NamedExpression].name -> c).toMap
      val partitionColumnNames = partitionColumnsMap.keys.map(col).toSeq
      val rangeColumns = partitionColumnNames ++ moreFileColumns
      val sortColumns = partitionColumnNames ++ moreFileColumns ++ moreFileOrder
      ds.toDF
        .call(ds => partitionColumnsMap.foldLeft(ds) { case (ds, (name, col)) => ds.withColumn(name, col) })
        .when(partitions.isEmpty)
        .call(_.repartitionByRange(rangeColumns: _*))
        .when(partitions.isDefined)
        .call(_.repartitionByRange(partitions.get, rangeColumns: _*))
        .sortWithinPartitions(sortColumns: _*)
        .when(writtenProjection.isDefined)
        .call(_.select(writtenProjection.get: _*))
        .when(requiresCaching && unpersistHandle.isDefined)
        .call(unpersistHandle.get.setDataFrame(_))
        .write
        .partitionBy(partitionColumnsMap.keys.toSeq: _*)
    }

    /**
     * (Scala-specific) Returns a [[KeyValueGroupedDataset]] where the data is grouped by the given key columns.
     *
     * @see
     *   `org.apache.spark.sql.Dataset.groupByKey(T => K)`
     *
     * @note
     *   Calling this method should be preferred to `groupByKey(T => K)` because the Catalyst query planner cannot
     *   exploit existing partitioning and ordering of this Dataset with that function.
     *
     * {{{
     *   ds.groupByKey[Int]($"age").flatMapGroups(...)
     *   ds.groupByKey[(String, String)]($"department", $"gender").flatMapGroups(...)
     * }}}
     */
    def groupByKey[K: Encoder](column: Column, columns: Column*): KeyValueGroupedDataset[K, V] =
      ds.groupBy(column +: columns: _*).as[K, V]

    /**
     * (Scala-specific) Returns a [[KeyValueGroupedDataset]] where the data is grouped by the given key columns.
     *
     * @see
     *   `org.apache.spark.sql.Dataset.groupByKey(T => K)`
     *
     * @note
     *   Calling this method should be preferred to `groupByKey(T => K)` because the Catalyst query planner cannot
     *   exploit existing partitioning and ordering of this Dataset with that function.
     *
     * {{{
     *   ds.groupByKey[Int]($"age").flatMapGroups(...)
     *   ds.groupByKey[(String, String)]($"department", $"gender").flatMapGroups(...)
     * }}}
     */
    def groupByKey[K: Encoder](column: String, columns: String*): KeyValueGroupedDataset[K, V] =
      ds.groupBy(column, columns: _*).as[K, V]

    /**
     * Groups the Dataset and sorts the groups using the specified columns, so we can run further process the sorted
     * groups. See [[uk.co.gresearch.spark.group.SortedGroupByDataset]] for all the available functions.
     *
     * {{{
     *   // Enumerate elements in the sorted group
     *   ds.groupBySorted($"department")($"salery")
     *     .flatMapSortedGroups((key, it) => it.zipWithIndex)
     * }}}
     *
     * @param cols
     *   grouping columns
     * @param order
     *   sort columns
     */
    def groupBySorted[K: Ordering: Encoder](cols: Column*)(order: Column*): SortedGroupByDataset[K, V] = {
      SortedGroupByDataset(ds, cols, order, None)
    }

    /**
     * Groups the Dataset and sorts the groups using the specified columns, so we can run further process the sorted
     * groups. See [[uk.co.gresearch.spark.group.SortedGroupByDataset]] for all the available functions.
     *
     * {{{
     *   // Enumerate elements in the sorted group
     *   ds.groupBySorted(10)($"department")($"salery")
     *     .flatMapSortedGroups((key, it) => it.zipWithIndex)
     * }}}
     *
     * @param partitions
     *   number of partitions
     * @param cols
     *   grouping columns
     * @param order
     *   sort columns
     */
    def groupBySorted[K: Ordering: Encoder](
        partitions: Int
    )(cols: Column*)(order: Column*): SortedGroupByDataset[K, V] = {
      SortedGroupByDataset(ds, cols, order, Some(partitions))
    }

    /**
     * Groups the Dataset and sorts the groups using the specified columns, so we can run further process the sorted
     * groups. See [[uk.co.gresearch.spark.group.SortedGroupByDataset]] for all the available functions.
     *
     * {{{
     *   // Enumerate elements in the sorted group
     *   ds.groupByKeySorted(row => row.getInt(0), 10)(row => row.getInt(1))
     *     .flatMapSortedGroups((key, it) => it.zipWithIndex)
     * }}}
     *
     * @param partitions
     *   number of partitions
     * @param key
     *   grouping key
     * @param order
     *   sort key
     */
    def groupByKeySorted[K: Ordering: Encoder, O: Encoder](key: V => K, partitions: Int)(
        order: V => O
    ): SortedGroupByDataset[K, V] =
      groupByKeySorted(key, Some(partitions))(order)

    /**
     * Groups the Dataset and sorts the groups using the specified columns, so we can run further process the sorted
     * groups. See [[uk.co.gresearch.spark.group.SortedGroupByDataset]] for all the available functions.
     *
     * {{{
     *   // Enumerate elements in the sorted group
     *   ds.groupByKeySorted(row => row.getInt(0), 10)(row => row.getInt(1), true)
     *     .flatMapSortedGroups((key, it) => it.zipWithIndex)
     * }}}
     *
     * @param partitions
     *   number of partitions
     * @param key
     *   grouping key
     * @param order
     *   sort key
     * @param reverse
     *   sort reverse order
     */
    def groupByKeySorted[K: Ordering: Encoder, O: Encoder](key: V => K, partitions: Int)(
        order: V => O,
        reverse: Boolean
    ): SortedGroupByDataset[K, V] =
      groupByKeySorted(key, Some(partitions))(order, reverse)

    /**
     * Groups the Dataset and sorts the groups using the specified columns, so we can run further process the sorted
     * groups. See [[uk.co.gresearch.spark.group.SortedGroupByDataset]] for all the available functions.
     *
     * {{{
     *   // Enumerate elements in the sorted group
     *   ds.groupByKeySorted(row => row.getInt(0))(row => row.getInt(1), true)
     *     .flatMapSortedGroups((key, it) => it.zipWithIndex)
     * }}}
     *
     * @param partitions
     *   optional number of partitions
     * @param key
     *   grouping key
     * @param order
     *   sort key
     * @param reverse
     *   sort reverse order
     */
    def groupByKeySorted[K: Ordering: Encoder, O: Encoder](
        key: V => K,
        partitions: Option[Int] = None
    )(order: V => O, reverse: Boolean = false): SortedGroupByDataset[K, V] = {
      SortedGroupByDataset(ds, key, order, partitions, reverse)
    }

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[withRowNumbers(String,StorageLevel,UnpersistHandle,Column...)]] for details.
     */
    def withRowNumbers(order: Column*): DataFrame =
      RowNumbers.withOrderColumns(order: _*).of(ds)

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[withRowNumbers(String,StorageLevel,UnpersistHandle,Column...)]] for details.
     */
    def withRowNumbers(rowNumberColumnName: String, order: Column*): DataFrame =
      RowNumbers.withRowNumberColumnName(rowNumberColumnName).withOrderColumns(order).of(ds)

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[withRowNumbers(String,StorageLevel,UnpersistHandle,Column...)]] for details.
     */
    def withRowNumbers(storageLevel: StorageLevel, order: Column*): DataFrame =
      RowNumbers.withStorageLevel(storageLevel).withOrderColumns(order).of(ds)

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[withRowNumbers(String,StorageLevel,UnpersistHandle,Column...)]] for details.
     */
    def withRowNumbers(unpersistHandle: UnpersistHandle, order: Column*): DataFrame =
      RowNumbers.withUnpersistHandle(unpersistHandle).withOrderColumns(order).of(ds)

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[withRowNumbers(String,StorageLevel,UnpersistHandle,Column...)]] for details.
     */
    def withRowNumbers(rowNumberColumnName: String, storageLevel: StorageLevel, order: Column*): DataFrame =
      RowNumbers
        .withRowNumberColumnName(rowNumberColumnName)
        .withStorageLevel(storageLevel)
        .withOrderColumns(order)
        .of(ds)

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[withRowNumbers(String,StorageLevel,UnpersistHandle,Column...)]] for details.
     */
    def withRowNumbers(rowNumberColumnName: String, unpersistHandle: UnpersistHandle, order: Column*): DataFrame =
      RowNumbers
        .withRowNumberColumnName(rowNumberColumnName)
        .withUnpersistHandle(unpersistHandle)
        .withOrderColumns(order)
        .of(ds)

    /**
     * Adds a global continuous row number starting at 1.
     *
     * See [[withRowNumbers(String,StorageLevel,UnpersistHandle,Column...)]] for details.
     */
    def withRowNumbers(storageLevel: StorageLevel, unpersistHandle: UnpersistHandle, order: Column*): DataFrame =
      RowNumbers.withStorageLevel(storageLevel).withUnpersistHandle(unpersistHandle).withOrderColumns(order).of(ds)

    /**
     * Adds a global continuous row number starting at 1, after sorting rows by the given columns. When no columns are
     * given, the existing order is used.
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
     * `Dataset.persist(StorageLevel)` on an intermediate DataFrame. The storage level of that cached DataFrame can be
     * set via `storageLevel`, where the default is `StorageLevel.MEMORY_AND_DISK`.
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
     * @param rowNumberColumnName
     *   name of the row number column
     * @param storageLevel
     *   storage level of the cached intermediate DataFrame
     * @param unpersistHandle
     *   handle to un-persist intermediate DataFrame
     * @param order
     *   columns to order dataframe before assigning row numbers
     * @return
     *   dataframe with row numbers
     */
    def withRowNumbers(
        rowNumberColumnName: String,
        storageLevel: StorageLevel,
        unpersistHandle: UnpersistHandle,
        order: Column*
    ): DataFrame =
      RowNumbers
        .withRowNumberColumnName(rowNumberColumnName)
        .withStorageLevel(storageLevel)
        .withUnpersistHandle(unpersistHandle)
        .withOrderColumns(order)
        .of(ds)
  }

  /**
   * Class to extend a Spark Dataframe.
   *
   * @param df
   *   dataframe
   */
  @deprecated("Implicit class ExtendedDataframe is deprecated, please recompile your source code.", since = "2.9.0")
  class ExtendedDataframe(df: DataFrame) extends ExtendedDataset[Row](df, df.encoder)

  /**
   * Class to extend a Spark Dataframe.
   *
   * @param df
   *   dataframe
   */
  def ExtendedDataframe(df: DataFrame): ExtendedDataframe = new ExtendedDataframe(df)

  /**
   * Implicit class to extend a Spark Dataframe, which is a Dataset[Row].
   *
   * @param df
   *   dataframe
   */
  implicit class ExtendedDataframeV2(df: DataFrame) extends ExtendedDatasetV2[Row](df)

}
