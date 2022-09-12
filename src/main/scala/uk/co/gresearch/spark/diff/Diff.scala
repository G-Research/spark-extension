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

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}
import uk.co.gresearch.spark.{backticks, distinctPrefixFor}

import scala.collection.JavaConverters

/**
 * Differ class to diff two Datasets. See Differ.of(…) for details.
 * @param options options for the diffing process
 */
class Differ(options: DiffOptions) {

  private[diff] def checkSchema[T, U](left: Dataset[T], right: Dataset[U], idColumns: Seq[String], ignoreColumns: Seq[String]): Unit = {
    require(left.columns.length == left.columns.toSet.size &&
      right.columns.length == right.columns.toSet.size,
      "The datasets have duplicate columns.\n" +
        s"Left column names: ${left.columns.mkString(", ")}\n" +
        s"Right column names: ${right.columns.mkString(", ")}")

    val ignoreColumnsCs = ignoreColumns.map(handleConfiguredCaseSensitivity).toSet
    def isIgnoredColumn(column: String): Boolean = !ignoreColumnsCs.contains(handleConfiguredCaseSensitivity(column))
    val leftNonIgnored = left.columns.filter(isIgnoredColumn)
    val rightNonIgnored = right.columns.filter(isIgnoredColumn)

    def notInWithCaseSensitivity(columns: Seq[String])(column: String): Boolean =
      !columns.map(handleConfiguredCaseSensitivity).contains(handleConfiguredCaseSensitivity(column))

    val exceptIgnoredColumnsMsg = if (ignoreColumns.nonEmpty) " except ignored columns" else ""

    require(leftNonIgnored.length == rightNonIgnored.length,
      "The number of columns doesn't match.\n" +
        s"Left column names$exceptIgnoredColumnsMsg (${leftNonIgnored.length}): ${leftNonIgnored.mkString(", ")}\n" +
        s"Right column names$exceptIgnoredColumnsMsg (${rightNonIgnored.length}): ${rightNonIgnored.mkString(", ")}")

    require(leftNonIgnored.length > 0, s"The schema$exceptIgnoredColumnsMsg must not be empty")

    // column types must match but we ignore the nullability of columns
    val leftFields = left.schema.fields.filter(f => isIgnoredColumn(f.name)).map(f => handleConfiguredCaseSensitivity(f.name) -> f.dataType)
    val rightFields = right.schema.fields.filter(f => isIgnoredColumn(f.name)).map(f => handleConfiguredCaseSensitivity(f.name) -> f.dataType)
    val leftExtraSchema = leftFields.diff(rightFields)
    val rightExtraSchema = rightFields.diff(leftFields)
    require(leftExtraSchema.isEmpty && rightExtraSchema.isEmpty,
      "The datasets do not have the same schema.\n" +
        s"Left extra columns: ${leftExtraSchema.map(t => s"${t._1} (${t._2})").mkString(", ")}\n" +
        s"Right extra columns: ${rightExtraSchema.map(t => s"${t._1} (${t._2})").mkString(", ")}")

    val columns = leftNonIgnored
    val pkColumns = if (idColumns.isEmpty) columns.toList else idColumns
    val nonPkColumns = columns.filter(notInWithCaseSensitivity(pkColumns))
    val missingIdColumns = pkColumns.filter(notInWithCaseSensitivity(columns))
    require(missingIdColumns.isEmpty,
      s"Some id columns do not exist: ${missingIdColumns.mkString(", ")} missing among ${columns.mkString(", ")}")

    val missingIgnoreColumns = ignoreColumns.diffCaseSensitivity(left.columns).diffCaseSensitivity(right.columns)
    require(missingIgnoreColumns.isEmpty,
      s"Some ignore columns do not exist: ${missingIgnoreColumns.mkString(", ")} " +
        s"missing among ${(leftNonIgnored ++ rightNonIgnored).distinct.sorted.mkString(", ")}")

    require(notInWithCaseSensitivity(pkColumns)(options.diffColumn),
      s"The id columns must not contain the diff column name '${options.diffColumn}': " +
        s"${pkColumns.mkString(", ")}")
    if(Set(DiffMode.LeftSide, DiffMode.RightSide).contains(options.diffMode))
      require(notInWithCaseSensitivity(nonPkColumns)(options.diffColumn),
        s"The non-id columns must not contain the diff column name '${options.diffColumn}': ${nonPkColumns.mkString((", "))}")

    require(options.changeColumn.forall(notInWithCaseSensitivity(pkColumns)),
      s"The id columns must not contain the change column name '${options.changeColumn.get}': ${pkColumns.mkString((", "))}")
    if(Set(DiffMode.LeftSide, DiffMode.RightSide).contains(options.diffMode))
      require(!options.changeColumn.exists(notInWithCaseSensitivity(nonPkColumns)),
        s"The non-id columns must not contain the change column name '${options.changeColumn.get}': ${nonPkColumns.mkString((", "))}")

    val diffValueColumns = getDiffColumns(pkColumns, nonPkColumns, left, right, ignoreColumns).map(_._1).diff(pkColumns)

    if (Seq(DiffMode.LeftSide, DiffMode.RightSide).contains(options.diffMode)) {
      require(notInWithCaseSensitivity(diffValueColumns)(options.diffColumn),
        s"The ${if (options.diffMode == DiffMode.LeftSide) "left" else "right"} " +
          s"non-id columns must not contain the diff column name '${options.diffColumn}': " +
          s"${(if (options.diffMode == DiffMode.LeftSide) left else right).columns.diffCaseSensitivity(idColumns).mkString(", ")}")

      options.changeColumn.foreach( changeColumn =>
        require(notInWithCaseSensitivity(diffValueColumns)(changeColumn),
          s"The ${if (options.diffMode == DiffMode.LeftSide) "left" else "right"} " +
            s"non-id columns must not contain the change column name '${options.changeColumn.get}': " +
            s"${(if (options.diffMode == DiffMode.LeftSide) left else right).columns.diffCaseSensitivity(idColumns).mkString(", ")}")
      )
    } else {
      require(notInWithCaseSensitivity(diffValueColumns)(options.diffColumn),
        s"The column prefixes '${options.leftColumnPrefix}' and '${options.rightColumnPrefix}', " +
          s"together with these non-id columns " +
          s"must not produce the diff column name '${options.diffColumn}': " +
          s"${nonPkColumns.mkString(", ")}")

      options.changeColumn.foreach( changeColumn =>
        require(notInWithCaseSensitivity(diffValueColumns)(changeColumn),
          s"The column prefixes '${options.leftColumnPrefix}' and '${options.rightColumnPrefix}', " +
            s"together with these non-id columns " +
            s"must not produce the change column name '${changeColumn}': " +
            s"${nonPkColumns.mkString(", ")}")
      )

      require(diffValueColumns.forall(column => notInWithCaseSensitivity(pkColumns)(column)),
        s"The column prefixes '${options.leftColumnPrefix}' and '${options.rightColumnPrefix}', " +
          s"together with these non-id columns " +
          s"must not produce any id column name '${pkColumns.mkString("', '")}': " +
          s"${nonPkColumns.mkString(", ")}")
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

  private[diff] def getDiffColumns[T, U](pkColumns: Seq[String], valueColumns: Seq[String],
                                         left: Dataset[T], right: Dataset[U],
                                         ignoreColumns: Seq[String]): Seq[(String, Column)] = {
    val idColumns = pkColumns.map(c => c -> coalesce(left(backticks(c)), right(backticks(c))).as(c))

    val leftValueColumns = left.columns.filterIsInCaseSensitivity(valueColumns)
    val rightValueColumns = right.columns.filterIsInCaseSensitivity(valueColumns)

    val leftNonPkColumns = left.columns.diffCaseSensitivity(pkColumns)
    val rightNonPkColumns = right.columns.diffCaseSensitivity(pkColumns)

    val leftIgnoredColumns = left.columns.filterIsInCaseSensitivity(ignoreColumns)
    val rightIgnoredColumns = right.columns.filterIsInCaseSensitivity(ignoreColumns)

    val (leftValues, rightValues) = if (options.sparseMode) {
      (
        leftNonPkColumns.map(c => (handleConfiguredCaseSensitivity(c), c -> (if (options.sparseMode) when(not(left(backticks(c)) <=> right(backticks(c))), left(backticks(c))) else left(backticks(c))))).toMap,
        rightNonPkColumns.map(c => (handleConfiguredCaseSensitivity(c), c -> (if (options.sparseMode) when(not(left(backticks(c)) <=> right(backticks(c))), right(backticks(c))) else right(backticks(c))))).toMap
      )
    } else {
      (
        leftNonPkColumns.map(c => (handleConfiguredCaseSensitivity(c), c -> left(backticks(c)))).toMap,
        rightNonPkColumns.map(c => (handleConfiguredCaseSensitivity(c), c -> right(backticks(c)))).toMap,
      )
    }

    def alias(prefix: Option[String], values: Map[String, (String, Column)])(name: String): (String, Column) = {
      values(handleConfiguredCaseSensitivity(name)) match {
        case (name, column) =>
          val alias = prefix.map(p => s"${p}_$name").getOrElse(name)
          alias -> column.as(alias)
      }
    }

    def aliasLeft(name: String): (String, Column) = alias(Some(options.leftColumnPrefix), leftValues)(name)

    def aliasRight(name: String): (String, Column) = alias(Some(options.rightColumnPrefix), rightValues)(name)

    val prefixedLeftIgnoredColumns = leftIgnoredColumns.map(c => aliasLeft(c))
    val prefixedRightIgnoredColumns = rightIgnoredColumns.map(c => aliasRight(c))

    val nonIdColumns = options.diffMode match {
      case DiffMode.ColumnByColumn =>
        valueColumns.flatMap(c =>
          Seq(
            aliasLeft(c),
            aliasRight(c)
          )
        ) ++ ignoreColumns.flatMap(c =>
          (if (leftIgnoredColumns.containsCaseSensitivity(c)) Seq(aliasLeft(c)) else Seq.empty) ++
            (if (rightIgnoredColumns.containsCaseSensitivity(c)) Seq(aliasRight(c)) else Seq.empty)
        )

      case DiffMode.SideBySide =>
        leftValueColumns.toSeq.map(c => aliasLeft(c)) ++ prefixedLeftIgnoredColumns ++
          rightValueColumns.toSeq.map(c => aliasRight(c)) ++ prefixedRightIgnoredColumns

      case DiffMode.LeftSide | DiffMode.RightSide =>
        // in left-side / right-side mode, we do not prefix columns
        (
          if (options.diffMode == DiffMode.LeftSide) valueColumns.map(alias(None, leftValues)) else valueColumns.map(alias(None, rightValues))
          ) ++ (
          if (options.diffMode == DiffMode.LeftSide) leftIgnoredColumns.map(alias(None, leftValues)) else rightIgnoredColumns.map(alias(None, rightValues))
          )
    }
    idColumns ++ nonIdColumns
  }

  private def doDiff[T, U](left: Dataset[T], right: Dataset[U], idColumns: Seq[String], ignoreColumns: Seq[String] = Seq.empty): DataFrame = {
    checkSchema(left, right, idColumns, ignoreColumns)

    val ignoreColumnsCs = ignoreColumns.map(handleConfiguredCaseSensitivity).toSet

    val columns = left.columns.filter(c => !ignoreColumnsCs.contains(handleConfiguredCaseSensitivity(c))).toList
    val pkColumns = if (idColumns.isEmpty) columns else idColumns
    val pkColumnsCs = pkColumns.map(handleConfiguredCaseSensitivity).toSet
    val valueColumns = columns.filter(col => !pkColumnsCs.contains(handleConfiguredCaseSensitivity(col)))

    val existsColumnName = distinctPrefixFor(left.columns) + "exists"
    val leftWithExists = left.withColumn(existsColumnName, lit(1))
    val rightWithExists = right.withColumn(existsColumnName, lit(1))
    val joinCondition = pkColumns.map(c => leftWithExists(backticks(c)) <=> rightWithExists(backticks(c))).reduce(_ && _)
    val unChanged = valueColumns.map(c => leftWithExists(backticks(c)) <=> rightWithExists(backticks(c))).reduceOption(_ && _)
    val changeCondition = not(unChanged.getOrElse(lit(true)))

    val diffActionColumn =
      when(leftWithExists(existsColumnName).isNull, lit(options.insertDiffValue)).
        when(rightWithExists(existsColumnName).isNull, lit(options.deleteDiffValue)).
        when(changeCondition, lit(options.changeDiffValue)).
        otherwise(lit(options.nochangeDiffValue)).
        as(options.diffColumn)

    val diffColumns = getDiffColumns(pkColumns, valueColumns, left, right, ignoreColumns).map(_._2)
    val changeColumn = getChangeColumn(existsColumnName, valueColumns, leftWithExists, rightWithExists)
      // turn this column into a sequence of one or none column so we can easily concat it below with diffActionColumn and diffColumns
      .map(Seq(_))
      .getOrElse(Seq.empty[Column])

    leftWithExists.join(rightWithExists, joinCondition, "fullouter")
      .select((diffActionColumn +: changeColumn) ++ diffColumns: _*)
  }

  /**
   * Returns a new DataFrame that contains the differences between two Datasets of
   * the same type `T`. Both Datasets must contain the same set of column names and data types.
   * The order of columns in the two Datasets is not relevant as columns are compared based on the
   * name, not the the position.
   *
   * Optional `id` columns are used to uniquely identify rows to compare. If values in any non-id
   * column are differing between two Datasets, then that row is marked as `"C"`hange
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
   * Returns a new DataFrame that contains the differences between two Datasets of
   * similar types `T` and `U`. Both Datasets must contain the same set of column names and data types,
   * except for the columns in `ignoreColumns`. The order of columns in the two Datasets is not relevant as
   * columns are compared based on the name, not the the position.
   *
   * Optional id columns are used to uniquely identify rows to compare. If values in any non-id
   * column are differing between two Datasets, then that row is marked as `"C"`hange
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
  def diff[T, U](left: Dataset[T], right: Dataset[U], idColumns: Seq[String], ignoreColumns: Seq[String] = Seq.empty): DataFrame =
    doDiff(left, right, idColumns, ignoreColumns)

  /**
   * Returns a new DataFrame that contains the differences between two Datasets of
   * similar types `T` and `U`. Both Datasets must contain the same set of column names and data types,
   * except for the columns in `ignoreColumns`. The order of columns in the two Datasets is not relevant as
   * columns are compared based on the name, not the the position.
   *
   * Optional id columns are used to uniquely identify rows to compare. If values in any non-id
   * column are differing between two Datasets, then that row is marked as `"C"`hange
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
  def diff[T, U](left: Dataset[T], right: Dataset[U], idColumns: java.util.List[String], ignoreColumns: java.util.List[String]): DataFrame = {
    diff(left, right, JavaConverters.iterableAsScalaIterable(idColumns).toSeq, JavaConverters.iterableAsScalaIterable(ignoreColumns).toSeq)
  }

  /**
   * Returns a new Dataset that contains the differences between two Datasets of the same type `T`.
   *
   * See `diff(Dataset[T], Dataset[U], String*)`.
   *
   * This requires an additional implicit `Encoder[V]` for the return type `Dataset[V]`.
   */
  // no @scala.annotation.varargs here as implicit arguments are explicit in Java
  // this signature is redundant to the other diffAs method in Java
  def diffAs[T, U, V](left: Dataset[T], right: Dataset[T], idColumns: String*)
                     (implicit diffEncoder: Encoder[V]): Dataset[V] = {
    diffAs(left, right, diffEncoder, idColumns: _*)
  }

  /**
   * Returns a new Dataset that contains the differences between two Datasets of
   * similar types `T` and `U`.
   *
   * See `diff(Dataset[T], Dataset[U], Seq[String], Seq[String])`.
   *
   * This requires an additional implicit `Encoder[V]` for the return type `Dataset[V]`.
   */
  def diffAs[T, U, V](left: Dataset[T], right: Dataset[U], idColumns: Seq[String], ignoreColumns: Seq[String] = Seq.empty)
                     (implicit diffEncoder: Encoder[V]): Dataset[V] = {
    diffAs(left, right, diffEncoder, idColumns, ignoreColumns)
  }

  /**
   * Returns a new Dataset that contains the differences between two Datasets of the same type `T`.
   *
   * See `diff(Dataset[T], Dataset[T], String*)`.
   *
   * This requires an additional explicit `Encoder[V]` for the return type `Dataset[V]`.
   */
  @scala.annotation.varargs
  def diffAs[T, V](left: Dataset[T], right: Dataset[T],
                   diffEncoder: Encoder[V], idColumns: String*): Dataset[V] = {
    diffAs(left, right, diffEncoder, idColumns, Seq.empty)
  }

  /**
   * Returns a new Dataset that contains the differences between two Datasets of
   * similar types `T` and `U`.
   *
   * See `of(Dataset[T], Dataset[U], Seq[String], Seq[String])`.
   *
   * This requires an additional explicit `Encoder[V]` for the return type `Dataset[V]`.
   */
  def diffAs[T, U, V](left: Dataset[T], right: Dataset[U],
                      diffEncoder: Encoder[V], idColumns: Seq[String], ignoreColumns: Seq[String]): Dataset[V] = {
    val nonIdColumns = if (idColumns.isEmpty) Seq.empty else left.columns.diffCaseSensitivity(idColumns).diffCaseSensitivity(ignoreColumns).toSeq
    val encColumns = diffEncoder.schema.fields.map(_.name)
    val diffColumns = Seq(options.diffColumn) ++ getDiffColumns(idColumns, nonIdColumns, left, right, ignoreColumns).map(_._1)
    val extraColumns = encColumns.diffCaseSensitivity(diffColumns)

    require(extraColumns.isEmpty,
      s"Diff encoder's columns must be part of the diff result schema, " +
        s"these columns are unexpected: ${extraColumns.mkString(", ")}")

    diff(left, right, idColumns, ignoreColumns).as[V](diffEncoder)
  }

  /**
   * Returns a new Dataset that contains the differences between two Datasets of
   * similar types `T` and `U`.
   *
   * See `of(Dataset[T], Dataset[U], Seq[String], Seq[String])`.
   *
   * This requires an additional explicit `Encoder[V]` for the return type `Dataset[V]`.
   */
  def diffAs[T, U, V](left: Dataset[T], right: Dataset[U], diffEncoder: Encoder[V],
                      idColumns: java.util.List[String], ignoreColumns: java.util.List[String]): Dataset[V] = {
    diffAs(left, right, diffEncoder,
      JavaConverters.iterableAsScalaIterable(idColumns).toSeq, JavaConverters.iterableAsScalaIterable(ignoreColumns).toSeq)
  }

  /**
   * Returns a new Dataset that contains the differences between two Dataset of
   * the same type `T` as tuples of type `(String, T, T)`.
   *
   * See `diff(Dataset[T], Dataset[T], String*)`.
   */
  @scala.annotation.varargs
  def diffWith[T](left: Dataset[T], right: Dataset[T], idColumns: String*): Dataset[(String, T, T)] = {
    val df = diff(left, right, idColumns: _*)
    diffWith(df, idColumns: _*)(left.encoder, right.encoder)
  }

  /**
   * Returns a new Dataset that contains the differences between two Dataset of
   * similar types `T` and `U` as tuples of type `(String, T, U)`.
   *
   * See `diff(Dataset[T], Dataset[U], Seq[String], Seq[String])`.
   */
  def diffWith[T, U](left: Dataset[T], right: Dataset[U],
                     idColumns: Seq[String], ignoreColumns: Seq[String]): Dataset[(String, T, U)] = {
    val df = diff(left, right, idColumns, ignoreColumns)
    diffWith(df, idColumns: _*)(left.encoder, right.encoder)
  }

  /**
   * Returns a new Dataset that contains the differences between two Dataset of
   * similar types `T` and `U` as tuples of type `(String, T, U)`.
   *
   * See `diff(Dataset[T], Dataset[U], Seq[String], Seq[String])`.
   */
  def diffWith[T, U](left: Dataset[T], right: Dataset[U],
                     idColumns: java.util.List[String], ignoreColumns: java.util.List[String]): Dataset[(String, T, U)] = {
    diffWith(left, right,
      JavaConverters.iterableAsScalaIterable(idColumns).toSeq, JavaConverters.iterableAsScalaIterable(ignoreColumns).toSeq)
  }

  private def columnsOfSide(df: DataFrame, idColumns: Seq[String], sidePrefix: String): Seq[Column] = {
    val prefix = sidePrefix + "_"
    df.columns
      .filter(c => idColumns.contains(c) || c.startsWith(sidePrefix))
      .map(c => if (idColumns.contains(c)) col(c) else col(c).as(c.replace(prefix, "")))
  }

  private def diffWith[T : Encoder, U : Encoder](diff: DataFrame, idColumns: String*): Dataset[(String, T, U)] = {
    val leftColumns = columnsOfSide(diff, idColumns, options.leftColumnPrefix)
    val rightColumns = columnsOfSide(diff, idColumns, options.rightColumnPrefix)

    val diffColumn = col(options.diffColumn).as("_1")
    val leftStruct = when(col(options.diffColumn) === options.insertDiffValue, lit(null))
      .otherwise(struct(leftColumns: _*))
      .as("_2")
    val rightStruct = when(col(options.diffColumn) === options.deleteDiffValue, lit(null))
      .otherwise(struct(rightColumns: _*))
      .as("_3")

    val plan = diff.select(diffColumn, leftStruct, rightStruct).queryExecution.logical

    val encoder: Encoder[(String, T, U)] = Encoders.tuple(
      Encoders.STRING, implicitly[Encoder[T]], implicitly[Encoder[U]]
    )

    new Dataset(diff.sparkSession, plan, encoder)
  }

}

/**
 * Diffing singleton with default diffing options.
 */
object Diff {
  val default = new Differ(DiffOptions.default)

  /**
   * Returns a new DataFrame that contains the differences between two Datasets
   * of the same type `T`. Both Datasets must contain the same set of column names and data types.
   * The order of columns in the two Datasets is not relevant as columns are compared based on the
   * name, not the the position.
   *
   * Optional id columns are used to uniquely identify rows to compare. If values in any non-id
   * column are differing between two Datasets, then that row is marked as `"C"`hange
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
   * Returns a new DataFrame that contains the differences between two Datasets of
   * similar types `T` and `U`. Both Datasets must contain the same set of column names and data types,
   * except for the columns in `ignoreColumns`. The order of columns in the two Datasets is not relevant as
   * columns are compared based on the name, not the the position.
   *
   * Optional id columns are used to uniquely identify rows to compare. If values in any non-id
   * column are differing between two Datasets, then that row is marked as `"C"`hange
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
  def of[T, U](left: Dataset[T], right: Dataset[U], idColumns: Seq[String], ignoreColumns: Seq[String] = Seq.empty): DataFrame =
    default.diff(left, right, idColumns, ignoreColumns)

  /**
   * Returns a new DataFrame that contains the differences between two Datasets of
   * similar types `T` and `U`. Both Datasets must contain the same set of column names and data types,
   * except for the columns in `ignoreColumns`. The order of columns in the two Datasets is not relevant as
   * columns are compared based on the name, not the the position.
   *
   * Optional id columns are used to uniquely identify rows to compare. If values in any non-id
   * column are differing between two Datasets, then that row is marked as `"C"`hange
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
  def of[T, U](left: Dataset[T], right: Dataset[U], idColumns: java.util.List[String], ignoreColumns: java.util.List[String]): DataFrame =
    default.diff(left, right, idColumns, ignoreColumns)

  /**
   * Returns a new Dataset that contains the differences between two Datasets of
   * the same type `T`.
   *
   * See `of(Dataset[T], Dataset[T], String*)`.
   *
   * This requires an additional implicit `Encoder[V]` for the return type `Dataset[V]`.
   */
  // no @scala.annotation.varargs here as implicit arguments are explicit in Java
  // this signature is redundant to the other ofAs method in Java
  def ofAs[T, V](left: Dataset[T], right: Dataset[T], idColumns: String*)
                (implicit diffEncoder: Encoder[V]): Dataset[V] =
    default.diffAs(left, right, idColumns: _*)

  /**
   * Returns a new DataFrame that contains the differences between two Datasets of
   * similar types `T` and `U`.
   *
   * See `of(Dataset[T], Dataset[U], Seq[String], Seq[String])`.
   *
   * This requires an additional implicit `Encoder[V]` for the return type `Dataset[V]`.
   */
  def ofAs[T, U, V](left: Dataset[T], right: Dataset[U], idColumns: Seq[String], ignoreColumns: Seq[String] = Seq.empty)
                   (implicit diffEncoder: Encoder[V]): Dataset[V] =
    default.diffAs(left, right, idColumns, ignoreColumns)

  /**
   * Returns a new Dataset that contains the differences between two Datasets of
   * the same type `T`.
   *
   * See `of(Dataset[T], Dataset[T], String*)`.
   *
   * This requires an additional explicit `Encoder[V]` for the return type `Dataset[V]`.
   */
  @scala.annotation.varargs
  def ofAs[T, V](left: Dataset[T], right: Dataset[T],
                 diffEncoder: Encoder[V], idColumns: String*): Dataset[V] =
    default.diffAs(left, right, diffEncoder, idColumns: _*)

  /**
   * Returns a new DataFrame that contains the differences between two Datasets of
   * similar types `T` and `U`.
   *
   * See `of(Dataset[T], Dataset[U], Seq[String], Seq[String])`.
   *
   * This requires an additional explicit `Encoder[V]` for the return type `Dataset[V]`.
   */
  def ofAs[T, U, V](left: Dataset[T], right: Dataset[U],
                    diffEncoder: Encoder[V], idColumns: Seq[String], ignoreColumns: Seq[String]): Dataset[V] =
    default.diffAs(left, right, diffEncoder, idColumns, ignoreColumns)

  /**
   * Returns a new DataFrame that contains the differences between two Datasets of
   * similar types `T` and `U`.
   *
   * See `of(Dataset[T], Dataset[U], Seq[String], Seq[String])`.
   *
   * This requires an additional explicit `Encoder[V]` for the return type `Dataset[V]`.
   */
  def ofAs[T, U, V](left: Dataset[T], right: Dataset[U], diffEncoder: Encoder[V],
                    idColumns: java.util.List[String], ignoreColumns: java.util.List[String]): Dataset[V] =
    default.diffAs(left, right, diffEncoder, idColumns, ignoreColumns)

  /**
   * Returns a new Dataset that contains the differences between two Dataset of
   * the same type `T` as tuples of type `(String, T, T)`.
   *
   * See `of(Dataset[T], Dataset[T], String*)`.
   */
  @scala.annotation.varargs
  def ofWith[T](left: Dataset[T], right: Dataset[T], idColumns: String*): Dataset[(String, T, T)] =
    default.diffWith(left, right, idColumns: _*)

  /**
   * Returns a new DataFrame that contains the differences between two Datasets of
   * similar types `T` and `U` as tuples of type `(String, T, U)`.
   *
   * See `of(Dataset[T], Dataset[U], Seq[String], Seq[String])`.
   */
  def ofWith[T, U](left: Dataset[T], right: Dataset[U],
                   idColumns: Seq[String], ignoreColumns: Seq[String]): Dataset[(String, T, U)] =
    default.diffWith(left, right, idColumns, ignoreColumns)

  /**
   * Returns a new DataFrame that contains the differences between two Datasets of
   * similar types `T` and `U` as tuples of type `(String, T, U)`.
   *
   * See `of(Dataset[T], Dataset[U], Seq[String], Seq[String])`.
   */
  def ofWith[T, U](left: Dataset[T], right: Dataset[U],
                   idColumns: java.util.List[String], ignoreColumns: java.util.List[String]): Dataset[(String, T, U)] =
    default.diffWith(left, right, idColumns, ignoreColumns)
}
