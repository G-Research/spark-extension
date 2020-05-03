/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.diff

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}
import uk.co.gresearch.spark.diff.DiffOptions.columnName

import scala.collection.JavaConverters._

/**
 * Differ class to diff two Datasets.
 * @param options options for the diffing process
 */
class Diff(options: DiffOptions) {

  private val changed: String = "_changed_columns_"

  def checkSchema[T](left: Dataset[T], right: Dataset[T], idColumns: String*): Unit = {

    require(left.columns.length > 0, "The schema must not be empty")

    // column types must match but we ignore the nullability of columns
    val leftFields = left.schema.fields.map(f => columnName(f.name) -> f.dataType)
    val rightFields = right.schema.fields.map(f => columnName(f.name) -> f.dataType)
    val leftExtraSchema = leftFields.diff(rightFields)
    val rightExtraSchema = rightFields.diff(leftFields)
    require(leftExtraSchema.isEmpty && rightExtraSchema.isEmpty,
      "The datasets do not have the same schema.\n" +
        s"${options.leftColumnPrefix} extra columns: ${leftExtraSchema.map(t => s"${t._1} (${t._2})").mkString(", ")}\n" +
        s"${options.rightColumnPrefix} extra columns: ${rightExtraSchema.map(t => s"${t._1} (${t._2})").mkString(", ")}")

    val columns = left.columns.map(columnName)
    val pkColumns = if (idColumns.isEmpty) columns.toList else idColumns.map(columnName)
    val missingIdColumns = pkColumns.diff(columns)
    require(missingIdColumns.isEmpty,
      s"Some id columns do not exist: ${missingIdColumns.mkString(", ")}")

    require(!pkColumns.contains(options.diffColumn),
      s"The id columns must not contain the diff column name '${options.diffColumn}': " +
        s"${pkColumns.mkString(", ")}")

    val nonIdColumns = columns.diff(pkColumns)
    val diffValueColumns = getDiffValueColumns(nonIdColumns)

    require(!diffValueColumns.contains(options.diffColumn),
      s"The column prefixes '${options.leftColumnPrefix}' and '${options.rightColumnPrefix}', " +
        s"together with these non-id columns " +
        s"must not produce the diff column name '${options.diffColumn}': " +
        s"${nonIdColumns.mkString(", ")}")

    require(diffValueColumns.forall(column => !pkColumns.contains(column)),
      s"The column prefixes '${options.leftColumnPrefix}' and '${options.rightColumnPrefix}', " +
        s"together with these non-id columns " +
        s"must not produce any id column name '${pkColumns.mkString("', '")}': " +
        s"${nonIdColumns.mkString(", ")}")
  }

  def getDiffValueColumns(nonIdColumns: Seq[String]): Seq[String] =
    Seq(options.leftColumnPrefix, options.rightColumnPrefix)
      .flatMap(prefix => nonIdColumns.map(column => s"${prefix}_$column"))

  def of[T](left: Dataset[T], right: Dataset[T], idColumns: String*): DataFrame = {
    val leftIgnored = left.drop(options.ignoreColumns:_*)
    val rightIgnored = right.drop(options.ignoreColumns:_*)
    checkSchema(leftIgnored, rightIgnored, idColumns: _*)

    val caseSensitiveMap = idColumns.map(id => columnName(id) -> id).toMap
    val pkColumns = if(idColumns.isEmpty){
      leftIgnored.schema.fields.toIndexedSeq
    }
    else{
      idColumns.toIndexedSeq.map(id => leftIgnored.schema.fields.find(sf => columnName(id) == columnName(sf.name))
        .getOrElse(throw new IllegalArgumentException(s"Could not resolve column name $id against ${leftIgnored.columns.mkString(", ")}.")))
    }
    val pkColumnsCs = pkColumns.map(sf => columnName(sf.name)).toSet
    val otherColumns = leftIgnored.schema.fields.filter(sf => !pkColumnsCs.contains(columnName(sf.name)))

    val existsColumnName = Diff.distinctStringNameFor(leftIgnored.columns)
    val l = leftIgnored.withColumn(existsColumnName, lit(1))
    val r = rightIgnored.withColumn(existsColumnName, lit(1))

    val joinCondition = pkColumns.map(c => options.getDiffComparator(c).compareColumns(l(c.name), r(c.name))).reduce(_ && _)
    val unChanged = otherColumns.map(c => {
      when(options.getDiffComparator(c).compareColumns(l(c.name), r(c.name)), typedLit(Array.empty[String]))
      .otherwise(typedLit(Array(c.name)))
    }).reduceOption((a, b) => concat(a, b))
    val changedColumnIndicator = unChanged.getOrElse(typedLit(Array.empty[String]))

    val diffCondition =
      when(l(existsColumnName).isNull, lit(options.insertDiffValue)).
        when(r(existsColumnName).isNull, lit(options.deleteDiffValue)).
        when(size(changedColumnIndicator) =!= 0, lit(options.changeDiffValue)).
        otherwise(lit(options.nochangeDiffValue))

    val diffColumns = pkColumns.map(c => coalesce(l(c.name), r(c.name)).as(caseSensitiveMap.getOrElse(columnName(c.name), c.name))) ++
      otherColumns.flatMap(c => Seq(l(c.name).as(s"${options.leftColumnPrefix}_${c.name}"), r(c.name).as(s"${options.rightColumnPrefix}_${c.name}")))

    val select = List(diffCondition.as(options.diffColumn)) ++ (if(options.nullOutValidData) Seq(changedColumnIndicator.as(changed)) else Seq()) ++ diffColumns
    val joined = l.join(r, joinCondition, "fullouter").select(select: _*)

    if(options.nullOutValidData){   // finally we null out each valid cell if required
      replaceValidCellsWithNull(joined, pkColumnsCs)
    }
    else{
      joined
    }
  }

  /**
   * Will replace cell objects with null if they satisfy the allocated comparator.
   * This will adhance visibility of erroneous data, especially for tables with many columns
   * @param joined - the result of the diff join operation
   * @param primaryColumns - list of all primary keys of this table
   */
  private def replaceValidCellsWithNull(joined: DataFrame, primaryColumns: Set[String]): DataFrame ={
    assert(joined.schema.fields.exists(f => f.name == changed), "Prerequisites for this function call are not satisfied.")
    // we need to single out those attributes, otherwise we would run in TaskNotSerializableExceptions
    val leftPrefix = options.leftColumnPrefix
    val rightPrefix = options.rightColumnPrefix
    val diffColumn = options.diffColumn
    val nullReplaced = joined.map(row => {
      val changedColumns = row.getList[String](1).asScala.flatMap(c => Seq(s"${leftPrefix}_$c", s"${rightPrefix}_$c"))
      val keepTheseColumns = Set(diffColumn) ++ primaryColumns ++ changedColumns
      val newRowSeq = row.toSeq.zip(row.schema.fields).map{
      case (cell: Any, StructField(name, _, nullable, _)) if ! nullable || keepTheseColumns.contains(name) => cell
      case (_, _) => null
    }
      Row.fromSeq(newRowSeq)
    })(RowEncoder(joined.schema))
    nullReplaced.drop(changed)
  }

  def ofAs[T, U](left: Dataset[T], right: Dataset[T], idColumns: String*)
               (implicit diffEncoder: Encoder[U]): Dataset[U] = {
    ofAs(left, right, diffEncoder, idColumns: _*)
  }

  def ofAs[T, U](left: Dataset[T], right: Dataset[T],
                 diffEncoder: Encoder[U], idColumns: String*): Dataset[U] = {
    val nonIdColumns = left.columns.diff(if (idColumns.isEmpty) left.columns.toList else idColumns)
    val encColumns = diffEncoder.schema.fields.map(_.name)
    val diffColumns = Seq(options.diffColumn) ++ idColumns ++ getDiffValueColumns(nonIdColumns)
    val extraColumns = encColumns.diff(diffColumns)

    require(extraColumns.isEmpty,
      s"Diff encoder's columns must be part of the diff result schema, " +
        s"these columns are unexpected: ${extraColumns.mkString(", ")}")

    of(left, right, idColumns: _*).as[U](diffEncoder)
  }
}

/**
 * Diffing singleton with default diffing options.
 */
object Diff {
  val default = new Diff(DiffOptions.default)

  /**
   * Provides a string  that is distinct w.r.t. the given strings.
   * @param existing strings
   * @return distinct string w.r.t. existing
   */
  def distinctStringNameFor(existing: Seq[String]): String = {
    "_" * (existing.map(_.length).reduceOption(_ max _).getOrElse(0) + 1)
  }

  def of[T](left: Dataset[T], right: Dataset[T], idColumns: String*)(implicit diff: Diff = default): DataFrame =
    diff.of(left, right, idColumns: _*)

  def ofAs[T, U](left: Dataset[T], right: Dataset[T], idColumns: String*)
                (implicit diffEncoder: Encoder[U], diff: Diff = default): Dataset[U] =
    diff.ofAs(left, right, idColumns: _*)

}
