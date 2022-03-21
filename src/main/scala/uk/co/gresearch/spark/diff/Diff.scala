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

package uk.co.gresearch.spark.diff

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder}
import uk.co.gresearch.spark.backticks
import uk.co.gresearch.spark.diff.DiffMode.DiffMode

import scala.collection.JavaConverters

/**
 * Differ class to diff two Datasets. See Differ.of(…) for details.
 * @param options options for the diffing process
 */
class Differ(options: DiffOptions) {

  private[diff] def checkSchema[T](left: Dataset[T], right: Dataset[T], idColumns: String*): Unit = {
    require(left.columns.length == left.columns.toSet.size &&
      right.columns.length == right.columns.toSet.size,
      "The datasets have duplicate columns.\n" +
        s"Left column names: ${left.columns.mkString(", ")}\n" +
        s"Right column names: ${right.columns.mkString(", ")}")

    require(left.columns.length == right.columns.length,
      "The number of columns doesn't match.\n" +
        s"Left column names (${left.columns.length}): ${left.columns.mkString(", ")}\n" +
        s"Right column names (${right.columns.length}): ${right.columns.mkString(", ")}")

    require(left.columns.length > 0, "The schema must not be empty")

    // column types must match but we ignore the nullability of columns
    val leftFields = left.schema.fields.map(f => handleConfiguredCaseSensitivity(f.name) -> f.dataType)
    val rightFields = right.schema.fields.map(f => handleConfiguredCaseSensitivity(f.name) -> f.dataType)
    val leftExtraSchema = leftFields.diff(rightFields)
    val rightExtraSchema = rightFields.diff(leftFields)
    require(leftExtraSchema.isEmpty && rightExtraSchema.isEmpty,
      "The datasets do not have the same schema.\n" +
        s"Left extra columns: ${leftExtraSchema.map(t => s"${t._1} (${t._2})").mkString(", ")}\n" +
        s"Right extra columns: ${rightExtraSchema.map(t => s"${t._1} (${t._2})").mkString(", ")}")

    val columns = left.columns.map(handleConfiguredCaseSensitivity)
    val pkColumns = if (idColumns.isEmpty) columns.toList else idColumns.map(handleConfiguredCaseSensitivity)
    val nonPkColumns = columns.diff(pkColumns)
    val missingIdColumns = pkColumns.diff(columns)
    require(missingIdColumns.isEmpty,
      s"Some id columns do not exist: ${missingIdColumns.mkString(", ")} missing among ${columns.mkString(", ")}")

    require(!pkColumns.contains(handleConfiguredCaseSensitivity(options.diffColumn)),
      s"The id columns must not contain the diff column name '${options.diffColumn}': " +
        s"${pkColumns.mkString(", ")}")
    if(Set(DiffMode.LeftSide, DiffMode.RightSide).contains(options.diffMode))
      require(!nonPkColumns.contains(options.diffColumn),
        s"The non-id columns must not contain the diff column name '${options.diffColumn}': ${nonPkColumns.mkString((", "))}")

    require(!options.changeColumn.exists(pkColumns.contains),
      s"The id columns must not contain the change column name '${options.changeColumn.get}': ${pkColumns.mkString((", "))}")
    if(Set(DiffMode.LeftSide, DiffMode.RightSide).contains(options.diffMode))
      require(!options.changeColumn.exists(nonPkColumns.contains),
        s"The non-id columns must not contain the change column name '${options.changeColumn.get}': ${nonPkColumns.mkString((", "))}")

    val nonIdColumns = columns.diff(pkColumns)
    val diffValueColumns = getDiffValueColumns(nonIdColumns, options.diffMode)

    require(!diffValueColumns.contains(handleConfiguredCaseSensitivity(options.diffColumn)),
      s"The column prefixes '${options.leftColumnPrefix}' and '${options.rightColumnPrefix}', " +
        s"together with these non-id columns " +
        s"must not produce the diff column name '${options.diffColumn}': " +
        s"${nonIdColumns.mkString(", ")}")

    options.changeColumn.foreach( changeColumn =>
      require(!diffValueColumns.contains(handleConfiguredCaseSensitivity(changeColumn)),
        s"The column prefixes '${options.leftColumnPrefix}' and '${options.rightColumnPrefix}', " +
          s"together with these non-id columns " +
          s"must not produce the change column name '${changeColumn}': " +
          s"${nonIdColumns.mkString(", ")}")
    )

    require(diffValueColumns.forall(column => !pkColumns.contains(column)),
      s"The column prefixes '${options.leftColumnPrefix}' and '${options.rightColumnPrefix}', " +
        s"together with these non-id columns " +
        s"must not produce any id column name '${pkColumns.mkString("', '")}': " +
        s"${nonIdColumns.mkString(", ")}")
  }

  /**
   * Produces the left and right value columns (non-id columns).
   * @param nonIdColumns value column names
   * @return left and right diff value column names
   */
  private[diff] def getDiffValueColumns(nonIdColumns: Seq[String], diffMode: DiffMode): Seq[String] = {
    def prefixColumns(columns: Seq[String])(prefix: String): Seq[String] =
      columns.map(column => s"${prefix}_$column")

    diffMode match {
      case DiffMode.ColumnByColumn =>
        Seq(options.leftColumnPrefix, options.rightColumnPrefix)
          .flatMap(prefixColumns(nonIdColumns))
          .map(handleConfiguredCaseSensitivity)

      case DiffMode.SideBySide =>
        prefixColumns(nonIdColumns)(options.leftColumnPrefix) ++
          prefixColumns(nonIdColumns)(options.rightColumnPrefix)
            .map(handleConfiguredCaseSensitivity)

      case DiffMode.LeftSide | DiffMode.RightSide =>
        nonIdColumns
    }
  }

  private def getChangeColumn(existsColumnName: String, valueColumns: Seq[String], left: Dataset[_], right: Dataset[_]): Option[Column] = {
    options.changeColumn
      .map(changeColumn =>
        when(left(existsColumnName).isNull || right(existsColumnName).isNull, lit(null)).
          otherwise(
            Some(valueColumns.toSeq)
              .filter(_.nonEmpty)
              .map(columns =>
                concat(
                  columns.map(c =>
                    when(left(backticks(c)) <=> right(backticks(c)), array()).otherwise(array(lit(c)))
                  ): _*
                )
              ).getOrElse(
              array().cast(ArrayType(StringType, containsNull = false))
            )
          ).
          as(changeColumn)
      )
  }

  private[diff] def getDiffColumns[T](pkColumns: Seq[String], otherColumns: Seq[String],
                                      left: Dataset[T], right: Dataset[T]): Seq[Column] = {
    val idColumns = pkColumns.map(c => coalesce(left(backticks(c)), right(backticks(c))).as(c))

    val (leftValues, rightValues) = if (options.sparseMode) {
      (
        otherColumns.map(c => (c, if (options.sparseMode) when(not(left(backticks(c)) <=> right(backticks(c))), left(backticks(c))) else left(backticks(c)))).toMap,
        otherColumns.map(c => (c, if (options.sparseMode) when(not(left(backticks(c)) <=> right(backticks(c))), right(backticks(c))) else right(backticks(c)))).toMap
      )
    } else {
      (
        otherColumns.map(c => (c, left(backticks(c)))).toMap,
        otherColumns.map(c => (c, right(backticks(c)))).toMap
      )
    }

    val valueColumns = options.diffMode match {
      case DiffMode.ColumnByColumn =>
        otherColumns.flatMap(c =>
          Seq(
            leftValues(c).as(s"${options.leftColumnPrefix}_$c"),
            rightValues(c).as(s"${options.rightColumnPrefix}_$c")
          )
        )

      case DiffMode.SideBySide =>
        otherColumns.map(c => leftValues(c).as(s"${options.leftColumnPrefix}_$c")) ++
          otherColumns.map(c => rightValues(c).as(s"${options.rightColumnPrefix}_$c"))

      case DiffMode.LeftSide | DiffMode.RightSide =>
        otherColumns.map(c =>
          if (options.diffMode == DiffMode.LeftSide) leftValues(c).as(c) else rightValues(c).as(c)
        )
    }
    idColumns ++ valueColumns
  }

  /**
   * Returns a new DataFrame that contains the differences between the two Datasets
   * of the same type `T`. Both Datasets must contain the same set of column names and data types.
   * The order of columns in the two Datasets is not important as columns are compared based on the
   * name, not the the position.
   *
   * Optional `id` columns are used to uniquely identify rows to compare. If values in any non-id
   * column are differing between the two Datasets, then that row is marked as `"C"`hange
   * and `"N"`o-change otherwise. Rows of the right Dataset, that do not exist in the left Dataset
   * (w.r.t. the values in the id columns) are marked as `"I"`nsert. And rows of the left Dataset,
   * that do not exist in the right Dataset are marked as `"D"`elete.
   *
   * If no id columns are given, all columns are considered id columns. Then, no `"C"`hange rows
   * will appear, as all changes will exists as respective `"D"`elete and `"I"`nsert.
   *
   * The returned DataFrame has the `diff` column as the first column. This holds the `"N"`, `"C"`,
   * `"I"` or `"D"` strings. The id columns follow, then the non-id columns (all remaining columns).
   *
   * {{{
   *   val df1 = Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "value")
   *   val df2 = Seq((1, "one"), (2, "Two"), (4, "four")).toDF("id", "value")
   *
   *   differ.diff(df1, df2).show()
   *
   *   // output:
   *   // +----+---+-----+
   *   // |diff| id|value|
   *   // +----+---+-----+
   *   // |   N|  1|  one|
   *   // |   D|  2|  two|
   *   // |   I|  2|  Two|
   *   // |   D|  3|three|
   *   // |   I|  4| four|
   *   // +----+---+-----+
   *
   *   differ.diff(df1, df2, "id").show()
   *
   *   // output:
   *   // +----+---+----------+-----------+
   *   // |diff| id|left_value|right_value|
   *   // +----+---+----------+-----------+
   *   // |   N|  1|       one|        one|
   *   // |   C|  2|       two|        Two|
   *   // |   D|  3|     three|       null|
   *   // |   I|  4|      null|       four|
   *   // +----+---+----------+-----------+
   *
   * }}}
   *
   * The id columns are in order as given to the method. If no id columns are given then all
   * columns of this Dataset are id columns and appear in the same order. The remaining non-id
   * columns are in the order of this Dataset.
   */
  @scala.annotation.varargs
  def diff[T](left: Dataset[T], right: Dataset[T], idColumns: String*): DataFrame = {
    diff(left, right, idColumns)
  }

  /**
   * Returns a new DataFrame that contains the differences between the two Datasets
   * of the same type `T`. Both Datasets must contain the same set of column names and data types.
   * The order of columns in the two Datasets is not important as columns are compared based on the
   * name, not the the position.
   *
   * Optional id columns are used to uniquely identify rows to compare. If values in any non-id
   * column are differing between the two Datasets, then that row is marked as `"C"`hange
   * and `"N"`o-change otherwise. Rows of the right Dataset, that do not exist in the left Dataset
   * (w.r.t. the values in the id columns) are marked as `"I"`nsert. And rows of the left Dataset,
   * that do not exist in the right Dataset are marked as `"D"`elete.
   *
   * If no id columns are given, all columns are considered id columns. Then, no `"C"`hange rows
   * will appear, as all changes will exists as respective `"D"`elete and `"I"`nsert.
   *
   * Values in optional ignore columns are not compared but included in the output DataFrame.
   *
   * The returned DataFrame has the `diff` column as the first column. This holds the `"N"`, `"C"`,
   * `"I"` or `"D"` strings. The id columns follow, then the non-id columns (all remaining columns).
   *
   * {{{
   *   val df1 = Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "value")
   *   val df2 = Seq((1, "one"), (2, "Two"), (4, "four")).toDF("id", "value")
   *
   *   differ.diff(df1, df2).show()
   *
   *   // output:
   *   // +----+---+-----+
   *   // |diff| id|value|
   *   // +----+---+-----+
   *   // |   N|  1|  one|
   *   // |   D|  2|  two|
   *   // |   I|  2|  Two|
   *   // |   D|  3|three|
   *   // |   I|  4| four|
   *   // +----+---+-----+
   *
   *   differ.diff(df1, df2, Seq("id")).show()
   *
   *   // output:
   *   // +----+---+----------+-----------+
   *   // |diff| id|left_value|right_value|
   *   // +----+---+----------+-----------+
   *   // |   N|  1|       one|        one|
   *   // |   C|  2|       two|        Two|
   *   // |   D|  3|     three|       null|
   *   // |   I|  4|      null|       four|
   *   // +----+---+----------+-----------+
   *
   * }}}
   *
   * The id columns are in order as given to the method. If no id columns are given then all
   * columns of this Dataset are id columns and appear in the same order. The remaining non-id
   * columns are in the order of this Dataset.
   */
  def diff[T](left: Dataset[T], right: Dataset[T], idColumns: Seq[String], ignoreColumns: Seq[String] = Seq.empty): DataFrame = {
    checkSchema(left, right, idColumns: _*)

    val pkColumns = if (idColumns.isEmpty) left.columns.toList else idColumns
    val pkColumnsCs = pkColumns.map(handleConfiguredCaseSensitivity).toSet
    val nonPkColumns = left.columns.filter(col => !pkColumnsCs.contains(handleConfiguredCaseSensitivity(col)))

    val ignoreColumnsCs = ignoreColumns.map(handleConfiguredCaseSensitivity).toSet
    val valueColumns = nonPkColumns.filter(col => !ignoreColumnsCs(handleConfiguredCaseSensitivity(col)))

    val existsColumnName = Diff.distinctStringNameFor(left.columns)
    val leftWithExists = left.withColumn(existsColumnName, lit(1))
    val rightWithExists = right.withColumn(existsColumnName, lit(1))
    val comparator = options.comparator
    val joinCondition = pkColumns.map(c => comparator(leftWithExists(backticks(c)), rightWithExists(backticks(c)))).reduce(_ && _)
    val unChanged = valueColumns.map(c => comparator(leftWithExists(backticks(c)), rightWithExists(backticks(c)))).reduceOption(_ && _)
    val changeCondition = not(unChanged.getOrElse(lit(true)))

    val diffActionColumn =
      when(leftWithExists(existsColumnName).isNull, lit(options.insertDiffValue)).
        when(rightWithExists(existsColumnName).isNull, lit(options.deleteDiffValue)).
        when(changeCondition, lit(options.changeDiffValue)).
        otherwise(lit(options.nochangeDiffValue)).
        as(options.diffColumn)

    val diffColumns = getDiffColumns(pkColumns, nonPkColumns, left, right)
    val changeColumn = getChangeColumn(existsColumnName, valueColumns, leftWithExists, rightWithExists)
        // turn this column into a sequence of one or none column so we can easily concat it below with diffActionColumn and diffColumns
        .map(Seq(_))
        .getOrElse(Seq.empty[Column])

    leftWithExists.join(rightWithExists, joinCondition, "fullouter")
      .select((diffActionColumn +: changeColumn) ++ diffColumns: _*)
  }

  /**
   * Returns a new DataFrame that contains the differences between the two Datasets
   * of the same type `T`. Both Datasets must contain the same set of column names and data types.
   * The order of columns in the two Datasets is not important as columns are compared based on the
   * name, not the the position.
   *
   * Optional id columns are used to uniquely identify rows to compare. If values in any non-id
   * column are differing between the two Datasets, then that row is marked as `"C"`hange
   * and `"N"`o-change otherwise. Rows of the right Dataset, that do not exist in the left Dataset
   * (w.r.t. the values in the id columns) are marked as `"I"`nsert. And rows of the left Dataset,
   * that do not exist in the right Dataset are marked as `"D"`elete.
   *
   * If no id columns are given, all columns are considered id columns. Then, no `"C"`hange rows
   * will appear, as all changes will exists as respective `"D"`elete and `"I"`nsert.
   *
   * Values in optional ignore columns are not compared but included in the output DataFrame.
   *
   * The returned DataFrame has the `diff` column as the first column. This holds the `"N"`, `"C"`,
   * `"I"` or `"D"` strings. The id columns follow, then the non-id columns (all remaining columns).
   *
   * {{{
   *   val df1 = Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "value")
   *   val df2 = Seq((1, "one"), (2, "Two"), (4, "four")).toDF("id", "value")
   *
   *   differ.diff(df1, df2).show()
   *
   *   // output:
   *   // +----+---+-----+
   *   // |diff| id|value|
   *   // +----+---+-----+
   *   // |   N|  1|  one|
   *   // |   D|  2|  two|
   *   // |   I|  2|  Two|
   *   // |   D|  3|three|
   *   // |   I|  4| four|
   *   // +----+---+-----+
   *
   *   differ.diff(df1, df2, Seq("id")).show()
   *
   *   // output:
   *   // +----+---+----------+-----------+
   *   // |diff| id|left_value|right_value|
   *   // +----+---+----------+-----------+
   *   // |   N|  1|       one|        one|
   *   // |   C|  2|       two|        Two|
   *   // |   D|  3|     three|       null|
   *   // |   I|  4|      null|       four|
   *   // +----+---+----------+-----------+
   *
   * }}}
   *
   * The id columns are in order as given to the method. If no id columns are given then all
   * columns of this Dataset are id columns and appear in the same order. The remaining non-id
   * columns are in the order of this Dataset.
   */
  def diff[T](left: Dataset[T], right: Dataset[T], idColumns: java.util.List[String], ignoreColumns: java.util.List[String]): DataFrame = {
    diff(left, right, JavaConverters.iterableAsScalaIterable(idColumns).toSeq, JavaConverters.iterableAsScalaIterable(ignoreColumns).toSeq)
  }

  /**
   * Returns a new Dataset that contains the differences between the two Datasets of the same type `T`.
   *
   * See `diff(Dataset[T], Dataset[T], String*)`.
   *
   * This requires an additional implicit `Encoder[U]` for the return type `Dataset[U]`.
   */
  // no @scala.annotation.varargs here as implicit arguments are explicit in Java
  // this signature is redundant to the other diffAs method in Java
  def diffAs[T, U](left: Dataset[T], right: Dataset[T], idColumns: String*)
                  (implicit diffEncoder: Encoder[U]): Dataset[U] = {
    diffAs(left, right, diffEncoder, idColumns: _*)
  }

  /**
   * Returns a new Dataset that contains the differences between the two Datasets of the same type `T`.
   *
   * See `diff(Dataset[T], Dataset[T], Seq[String], Seq[String])`.
   *
   * This requires an additional implicit `Encoder[U]` for the return type `Dataset[U]`.
   */
  def diffAs[T, U](left: Dataset[T], right: Dataset[T], idColumns: Seq[String], ignoreColumns: Seq[String] = Seq.empty)
                  (implicit diffEncoder: Encoder[U]): Dataset[U] = {
    diffAs(left, right, diffEncoder, idColumns, ignoreColumns)
  }

  /**
   * Returns a new Dataset that contains the differences between the two Datasets of the same type `T`.
   *
   * See `diff(Dataset[T], Dataset[T], String*)`.
   *
   * This requires an additional explicit `Encoder[U]` for the return type `Dataset[U]`.
   */
  @scala.annotation.varargs
  def diffAs[T, U](left: Dataset[T], right: Dataset[T],
                   diffEncoder: Encoder[U], idColumns: String*): Dataset[U] = {
    diffAs(left, right, diffEncoder, idColumns, Seq.empty)
  }

  /**
   * Returns a new Dataset that contains the differences between the two Datasets of the same type `T`.
   *
   * See `of(Dataset[T], Dataset[T], Seq[String], Seq[String])`.
   *
   * This requires an additional explicit `Encoder[U]` for the return type `Dataset[U]`.
   */
  def diffAs[T, U](left: Dataset[T], right: Dataset[T],
                   diffEncoder: Encoder[U], idColumns: Seq[String], ignoreColumns: Seq[String]): Dataset[U] = {
    val nonIdColumns = left.columns.diff(if (idColumns.isEmpty) left.columns.toList else idColumns)
    val encColumns = diffEncoder.schema.fields.map(_.name)
    val diffColumns = Seq(options.diffColumn) ++ idColumns ++ getDiffValueColumns(nonIdColumns, options.diffMode)
    val extraColumns = encColumns.diff(diffColumns)

    require(extraColumns.isEmpty,
      s"Diff encoder's columns must be part of the diff result schema, " +
        s"these columns are unexpected: ${extraColumns.mkString(", ")}")

    diff(left, right, idColumns, ignoreColumns).as[U](diffEncoder)
  }

  /**
   * Returns a new Dataset that contains the differences between the two Datasets of the same type `T`.
   *
   * See `of(Dataset[T], Dataset[T], Seq[String], Seq[String])`.
   *
   * This requires an additional explicit `Encoder[U]` for the return type `Dataset[U]`.
   */
  def diffAs[T, U](left: Dataset[T], right: Dataset[T], diffEncoder: Encoder[U],
                   idColumns: java.util.List[String], ignoreColumns: java.util.List[String]): Dataset[U] = {
    diffAs(left, right, diffEncoder,
      JavaConverters.iterableAsScalaIterable(idColumns).toSeq, JavaConverters.iterableAsScalaIterable(ignoreColumns).toSeq)
  }

}

/**
 * Diffing singleton with default diffing options.
 */
object Diff {
  val default = new Differ(DiffOptions.default)

  /**
   * Provides a string  that is distinct w.r.t. the given strings.
   * @param existing strings
   * @return distinct string w.r.t. existing
   */
  private[diff] def distinctStringNameFor(existing: Seq[String]): String = {
    "_" * (existing.map(_.length).reduceOption(_ max _).getOrElse(0) + 1)
  }

  /**
   * Returns a new DataFrame that contains the differences between the two Datasets
   * of the same type `T`. Both Datasets must contain the same set of column names and data types.
   * The order of columns in the two Datasets is not important as columns are compared based on the
   * name, not the the position.
   *
   * Optional id columns are used to uniquely identify rows to compare. If values in any non-id
   * column are differing between the two Datasets, then that row is marked as `"C"`hange
   * and `"N"`o-change otherwise. Rows of the right Dataset, that do not exist in the left Dataset
   * (w.r.t. the values in the id columns) are marked as `"I"`nsert. And rows of the left Dataset,
   * that do not exist in the right Dataset are marked as `"D"`elete.
   *
   * If no id columns are given, all columns are considered id columns. Then, no `"C"`hange rows
   * will appear, as all changes will exists as respective `"D"`elete and `"I"`nsert.
   *
   * The returned DataFrame has the `diff` column as the first column. This holds the `"N"`, `"C"`,
   * `"I"` or `"D"` strings. The id columns follow, then the non-id columns (all remaining columns).
   *
   * {{{
   *   val df1 = Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "value")
   *   val df2 = Seq((1, "one"), (2, "Two"), (4, "four")).toDF("id", "value")
   *
   *   Diff.of(df1, df2).show()
   *
   *   // output:
   *   // +----+---+-----+
   *   // |diff| id|value|
   *   // +----+---+-----+
   *   // |   N|  1|  one|
   *   // |   D|  2|  two|
   *   // |   I|  2|  Two|
   *   // |   D|  3|three|
   *   // |   I|  4| four|
   *   // +----+---+-----+
   *
   *   Diff.of(df1, df2, "id").show()
   *
   *   // output:
   *   // +----+---+----------+-----------+
   *   // |diff| id|left_value|right_value|
   *   // +----+---+----------+-----------+
   *   // |   N|  1|       one|        one|
   *   // |   C|  2|       two|        Two|
   *   // |   D|  3|     three|       null|
   *   // |   I|  4|      null|       four|
   *   // +----+---+----------+-----------+
   *
   * }}}
   *
   * The id columns are in order as given to the method. If no id columns are given then all
   * columns of this Dataset are id columns and appear in the same order. The remaining non-id
   * columns are in the order of this Dataset.
   */
  @scala.annotation.varargs
  def of[T](left: Dataset[T], right: Dataset[T], idColumns: String*): DataFrame =
    default.diff(left, right, idColumns: _*)

  /**
   * Returns a new DataFrame that contains the differences between the two Datasets
   * of the same type `T`. Both Datasets must contain the same set of column names and data types.
   * The order of columns in the two Datasets is not important as columns are compared based on the
   * name, not the the position.
   *
   * Optional id columns are used to uniquely identify rows to compare. If values in any non-id
   * column are differing between the two Datasets, then that row is marked as `"C"`hange
   * and `"N"`o-change otherwise. Rows of the right Dataset, that do not exist in the left Dataset
   * (w.r.t. the values in the id columns) are marked as `"I"`nsert. And rows of the left Dataset,
   * that do not exist in the right Dataset are marked as `"D"`elete.
   *
   * If no id columns are given, all columns are considered id columns. Then, no `"C"`hange rows
   * will appear, as all changes will exists as respective `"D"`elete and `"I"`nsert.
   *
   * Values in optional ignore columns are not compared but included in the output DataFrame.
   *
   * The returned DataFrame has the `diff` column as the first column. This holds the `"N"`, `"C"`,
   * `"I"` or `"D"` strings. The id columns follow, then the non-id columns (all remaining columns).
   *
   * {{{
   *   val df1 = Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "value")
   *   val df2 = Seq((1, "one"), (2, "Two"), (4, "four")).toDF("id", "value")
   *
   *   Diff.of(df2).show()
   *
   *   // output:
   *   // +----+---+-----+
   *   // |diff| id|value|
   *   // +----+---+-----+
   *   // |   N|  1|  one|
   *   // |   D|  2|  two|
   *   // |   I|  2|  Two|
   *   // |   D|  3|three|
   *   // |   I|  4| four|
   *   // +----+---+-----+
   *
   *   Diff.of(df2, "id").show()
   *
   *   // output:
   *   // +----+---+----------+-----------+
   *   // |diff| id|left_value|right_value|
   *   // +----+---+----------+-----------+
   *   // |   N|  1|       one|        one|
   *   // |   C|  2|       two|        Two|
   *   // |   D|  3|     three|       null|
   *   // |   I|  4|      null|       four|
   *   // +----+---+----------+-----------+
   *
   * }}}
   *
   * The id columns are in order as given to the method. If no id columns are given then all
   * columns of this Dataset are id columns and appear in the same order. The remaining non-id
   * columns are in the order of this Dataset.
   */
  def of[T](left: Dataset[T], right: Dataset[T], idColumns: Seq[String], ignoreColumns: Seq[String] = Seq.empty): DataFrame =
    default.diff(left, right, idColumns, ignoreColumns)

  /**
   * Returns a new DataFrame that contains the differences between the two Datasets
   * of the same type `T`. Both Datasets must contain the same set of column names and data types.
   * The order of columns in the two Datasets is not important as columns are compared based on the
   * name, not the the position.
   *
   * Optional id columns are used to uniquely identify rows to compare. If values in any non-id
   * column are differing between the two Datasets, then that row is marked as `"C"`hange
   * and `"N"`o-change otherwise. Rows of the right Dataset, that do not exist in the left Dataset
   * (w.r.t. the values in the id columns) are marked as `"I"`nsert. And rows of the left Dataset,
   * that do not exist in the right Dataset are marked as `"D"`elete.
   *
   * If no id columns are given, all columns are considered id columns. Then, no `"C"`hange rows
   * will appear, as all changes will exists as respective `"D"`elete and `"I"`nsert.
   *
   * Values in optional ignore columns are not compared but included in the output DataFrame.
   *
   * The returned DataFrame has the `diff` column as the first column. This holds the `"N"`, `"C"`,
   * `"I"` or `"D"` strings. The id columns follow, then the non-id columns (all remaining columns).
   *
   * {{{
   *   val df1 = Seq((1, "one"), (2, "two"), (3, "three")).toDF("id", "value")
   *   val df2 = Seq((1, "one"), (2, "Two"), (4, "four")).toDF("id", "value")
   *
   *   Diff.of(df2).show()
   *
   *   // output:
   *   // +----+---+-----+
   *   // |diff| id|value|
   *   // +----+---+-----+
   *   // |   N|  1|  one|
   *   // |   D|  2|  two|
   *   // |   I|  2|  Two|
   *   // |   D|  3|three|
   *   // |   I|  4| four|
   *   // +----+---+-----+
   *
   *   Diff.of(df2, "id").show()
   *
   *   // output:
   *   // +----+---+----------+-----------+
   *   // |diff| id|left_value|right_value|
   *   // +----+---+----------+-----------+
   *   // |   N|  1|       one|        one|
   *   // |   C|  2|       two|        Two|
   *   // |   D|  3|     three|       null|
   *   // |   I|  4|      null|       four|
   *   // +----+---+----------+-----------+
   *
   * }}}
   *
   * The id columns are in order as given to the method. If no id columns are given then all
   * columns of this Dataset are id columns and appear in the same order. The remaining non-id
   * columns are in the order of this Dataset.
   */
  def of[T](left: Dataset[T], right: Dataset[T], idColumns: java.util.List[String], ignoreColumns: java.util.List[String]): DataFrame =
    default.diff(left, right, JavaConverters.iterableAsScalaIterable(idColumns).toSeq, JavaConverters.iterableAsScalaIterable(ignoreColumns).toSeq)

  /**
   * Returns a new Dataset that contains the differences between the two Datasets of the same type `T`.
   *
   * See `of(Dataset[T], Dataset[T], String*)`.
   *
   * This requires an additional implicit `Encoder[U]` for the return type `Dataset[U]`.
   */
  // no @scala.annotation.varargs here as implicit arguments are explicit in Java
  // this signature is redundant to the other ofAs method in Java
  def ofAs[T, U](left: Dataset[T], right: Dataset[T], idColumns: String*)
                (implicit diffEncoder: Encoder[U]): Dataset[U] =
    default.diffAs(left, right, idColumns: _*)

  /**
   * Returns a new Dataset that contains the differences between the two Datasets of the same type `T`.
   *
   * See `of(Dataset[T], Dataset[T], Seq[String], Seq[String])`.
   *
   * This requires an additional implicit `Encoder[U]` for the return type `Dataset[U]`.
   */
  def ofAs[T, U](left: Dataset[T], right: Dataset[T], idColumns: Seq[String], ignoreColumns: Seq[String] = Seq.empty)
                (implicit diffEncoder: Encoder[U]): Dataset[U] =
    default.diffAs(left, right, idColumns, ignoreColumns)

  /**
   * Returns a new Dataset that contains the differences between the two Datasets of the same type `T`.
   *
   * See `of(Dataset[T], Dataset[T], String*)`.
   *
   * This requires an additional explicit `Encoder[U]` for the return type `Dataset[U]`.
   */
  @scala.annotation.varargs
  def ofAs[T, U](left: Dataset[T], right: Dataset[T],
                 diffEncoder: Encoder[U], idColumns: String*): Dataset[U] =
    default.diffAs(left, right, diffEncoder, idColumns: _*)

  /**
   * Returns a new Dataset that contains the differences between the two Datasets of the same type `T`.
   *
   * See `of(Dataset[T], Dataset[T], Seq[String], Seq[String])`.
   *
   * This requires an additional explicit `Encoder[U]` for the return type `Dataset[U]`.
   */
  def ofAs[T, U](left: Dataset[T], right: Dataset[T],
                 diffEncoder: Encoder[U], idColumns: Seq[String], ignoreColumns: Seq[String]): Dataset[U] =
    default.diffAs(left, right, diffEncoder, idColumns, ignoreColumns)

  /**
   * Returns a new Dataset that contains the differences between the two Datasets of the same type `T`.
   *
   * See `of(Dataset[T], Dataset[T], Seq[String], Seq[String])`.
   *
   * This requires an additional explicit `Encoder[U]` for the return type `Dataset[U]`.
   */
  def ofAs[T, U](left: Dataset[T], right: Dataset[T], diffEncoder: Encoder[U],
                 idColumns: java.util.List[String], ignoreColumns: java.util.List[String]): Dataset[U] =
    default.diffAs(left, right, diffEncoder,
      JavaConverters.iterableAsScalaIterable(idColumns).toSeq, JavaConverters.iterableAsScalaIterable(ignoreColumns).toSeq)

}
