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
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions.col

package object spark {

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
  def backticks(string: String, strings: String*): String = (string +: strings)
    .map(s => if (s.contains(".") && !s.startsWith("`") && !s.endsWith("`")) s"`$s`" else s).mkString(".")

  trait WhenTransformation[T] {
    /**
     * Executes the given transformation.
     *
     * @param transformation transformation
     * @return transformation result
     */
    def call[R](transformation: Dataset[T] => Dataset[R]): DataFrame
  }

  case class ThenTransformation[T](t: Dataset[T]) extends WhenTransformation[T] {
    override def call[R](transformation: Dataset[T] => Dataset[R]): DataFrame = t.call(transformation)
  }

  case class OtherwiseTransformation[T](t: Dataset[T]) extends WhenTransformation[T] {
    override def call[R](transformation: Dataset[T] => Dataset[R]): DataFrame = t.toDF
  }

  /**
   * Implicit class to extend a Spark Dataset.
   *
   * @param df dataset or dataframe
   * @tparam D inner type of dataset
   */
  implicit class ExtendedDataset[D](df: Dataset[D]) {
    /**
     * Compute the histogram of a column when aggregated by aggregate columns.
     * Thresholds are expected to be provided in ascending order.
     * The result dataframe contains the aggregate and histogram columns only.
     * For each threshold value in thresholds, there will be a column named s"≤$threshold".
     * There will also be a final column called s">${last_threshold}", that counts the remaining
     * values that exceed the last threshold.
     *
     * @param thresholds       sequence of thresholds, must implement <= and > operators w.r.t. valueColumn
     * @param valueColumn      histogram is computed for values of this column
     * @param aggregateColumns histogram is computed against these columns
     * @tparam T type of histogram thresholds
     * @return dataframe with aggregate and histogram columns
     */
    def histogram[T: Ordering](thresholds: Seq[T], valueColumn: Column, aggregateColumns: Column*): DataFrame =
      Histogram.of(df, thresholds, valueColumn, aggregateColumns: _*)

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

     * is equivalent to:
     * {{{
     *   df.repartitionByRange(10, $"a", $"b")
     *     .sortWithinPartitions($"a", $"b", $"c")
     *     .select($"a", concat($"b", $"c"))
     *     .write
     *     .partitionBy("a")
     * }}}
     *
     * @param partitionColumns columns used for partitioning
     * @param moreFileColumns columns where individual values are written to a single file
     * @param moreFileOrder additional columns to sort partition files
     * @param partitions optional number of partition files
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
      df.toDF
        .call(ds => partitionColumnsMap.foldLeft(ds) { case (ds, (name, col)) => ds.withColumn(name, col) })
        .when(partitions.isEmpty).call(_.repartitionByRange(rangeColumns: _*))
        .when(partitions.isDefined).call(_.repartitionByRange(partitions.get, rangeColumns: _*))
        .sortWithinPartitions(sortColumns: _*)
        .when(writtenProjection.isDefined).call(_.select(writtenProjection.get: _*))
        .write
        .partitionBy(partitionColumnsMap.keys.toSeq: _*)
    }

    /**
     * Executes the given transformation on the decorated instance.
     *
     * This allows writing fluent code like
     *
     * {{{
     * i.doThis()
     *  .doThat()
     *  .call(transformation)
     *  .doMore()
     * }}}
     *
     * rather than
     *
     * {{{
     * transformation(
     *   i.doThis()
     *    .doThat()
     * ).doMore()
     * }}}
     *
     * where the effective sequence of operations is not clear.
     *
     * @param transformation transformation
     * @return the transformation result
     */
    def call[R](transformation: Dataset[D] => Dataset[R]): DataFrame = transformation(df).toDF

    /**
     * Allows to perform a transformation fluently only if the given condition is true:
     *
     * {{{
     *   a.when(true).call(_.action())
     * }}}
     *
     * This allows to write elegant code like
     *
     * {{{
     * i.doThis()
     *  .doThat()
     *  .when(condition).call(transformation)
     *  .doMore()
     * }}}
     *
     * rather than
     *
     * {{{
     * val intermediate1 =
     *   i.doThis()
     *    .doThat()
     * val intermediate2 =
     *   if (condition) transformation(intermediate1) else indermediate1
     * intermediate2.doMore()
     * }}}
     *
     * @param condition condition
     * @return WhenTransformation
     */
    def when(condition: Boolean): WhenTransformation[D] =
      if (condition) ThenTransformation(df) else OtherwiseTransformation(df)

  }

}
