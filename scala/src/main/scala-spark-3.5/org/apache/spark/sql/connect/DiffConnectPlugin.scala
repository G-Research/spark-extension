/*
 * Copyright 2024 G-Research
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

package org.apache.spark.sql.connect

import org.sparkproject.connect.protobuf
import org.apache.orc.protobuf.Descriptors.FieldDescriptor
import org.apache.spark.connect.proto.Plan
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.RelationPlugin
import uk.co.gresearch.spark.diff.DatasetDiff
import uk.co.gresearch.spark.connect.diff.DiffOuterClass
import uk.co.gresearch.spark.connect.diff.DiffOuterClass.Comparator.EpsilonComparator
import uk.co.gresearch.spark.connect.diff.DiffOuterClass.Diff
import uk.co.gresearch.spark.diff.{DiffComparators, DiffMode, DiffOptions, Differ}
import uk.co.gresearch.spark.diff.comparator.DiffComparator

import java.util.Optional
import scala.collection.JavaConverters
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class DiffConnectPlugin extends RelationPlugin {
  private def dataset(bytes: Array[Byte], planner: SparkConnectPlanner): DataFrame = {
    val relation = ConnectProtoUtils.parseRelationWithRecursionLimit(bytes, recursionLimit = 1024)
    Dataset.ofRows(planner.sessionHolder.session, planner.transformRelation(relation))
  }

  private def comparator(comparator: AnyRef): DiffComparator = comparator match {
    case e: EpsilonComparator => DiffComparators.epsilon(e.getEpsilon)
  }

  private def toOptions(options: DiffOuterClass.DiffOptions): DiffOptions = {
    options.getAllFields.entrySet().asScala.foldLeft(DiffOptions.default) { (options, option) => option.getKey.getIndex match {
      case DiffOuterClass.DiffOptions.DIFFCOLUMN_FIELD_NUMBER => options.withDiffColumn(option.getValue.asInstanceOf[String])
      case DiffOuterClass.DiffOptions.LEFTCOLUMNPREFIX_FIELD_NUMBER => options.withLeftColumnPrefix(option.getValue.asInstanceOf[String])
      case DiffOuterClass.DiffOptions.RIGHTCOLUMNPREFIX_FIELD_NUMBER => options.withRightColumnPrefix(option.getValue.asInstanceOf[String])
      case DiffOuterClass.DiffOptions.INSERTDIFFVALUE_FIELD_NUMBER => options.withInsertDiffValue(option.getValue.asInstanceOf[String])
      case DiffOuterClass.DiffOptions.CHANGEDIFFVALUE_FIELD_NUMBER => options.withChangeDiffValue(option.getValue.asInstanceOf[String])
      case DiffOuterClass.DiffOptions.DELETEDIFFVALUE_FIELD_NUMBER => options.withDeleteDiffValue(option.getValue.asInstanceOf[String])
      case DiffOuterClass.DiffOptions.NOCHANGEDIFFVALUE_FIELD_NUMBER => options.withNochangeDiffValue(option.getValue.asInstanceOf[String])
      case DiffOuterClass.DiffOptions.DIFFMODE_FIELD_NUMBER => options.withDiffMode(DiffMode.withName(option.getValue.asInstanceOf[String]))
      case DiffOuterClass.DiffOptions.SPARSEMODE_FIELD_NUMBER => options.withSparseMode(option.getValue.asInstanceOf[Boolean])
      case DiffOuterClass.DiffOptions.DEFAULTCOMPARATOR_FIELD_NUMBER => options.withDefaultComparator(comparator(option.getValue))
      // case DiffOuterClass.DiffOptions.DATATYPECOMPARATORS_FIELD_NUMBER => options.withComparator(option.getValue.asInstanceOf[String])
      // case DiffOuterClass.DiffOptions.COLUMNNAMECOMPARATORS_FIELD_NUMBER => options.withComparator(option.getValue.asInstanceOf[String])
    }}
  }

  override def transform(relation : com.google.protobuf.Any, planner : SparkConnectPlanner) : Option[LogicalPlan] = {
    if (!relation.is(classOf[Diff])) {
      return Option.empty
    }

    val diff = relation.unpack(classOf[Diff])
    val left = dataset(diff.getLeft.toByteArray, planner)
    val right  = dataset(diff.getRight.toByteArray, planner)
    val options = if (diff.hasOptions) toOptions(diff.getOptions) else DiffOptions.default
    Option.apply(left.diff(right, options, JavaConverters.iterableAsScalaIterableConverter(diff.getIdColumnList()).asScala.toSeq, JavaConverters.iterableAsScalaIterableConverter(diff.getIgnoreColumnList()).asScala.toSeq).logicalPlan)
  }
}
