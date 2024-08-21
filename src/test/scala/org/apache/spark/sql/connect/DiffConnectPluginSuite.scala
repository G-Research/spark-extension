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

package org.apache.spark.sql.connect

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.{SessionHolder, SparkConnectService}
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.sparkproject.connect.protobuf.{Any, ByteString}
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.connect.diff.DiffOuterClass.{Diff, DiffOptions}

import java.util.UUID

class DiffConnectPluginSuite extends AnyFunSuite with SparkTestSession {

  val leftRows: Seq[InternalRow] = (0 until 4).map(i => i * 2).map { i =>
    InternalRow(i, UTF8String.fromString(s"str-$i"), InternalRow(i))
  }

  val rightRows: Seq[InternalRow] = (0 until 3).map(i => i * 3).map { i =>
    InternalRow(i, UTF8String.fromString(s"str-$i"), InternalRow(i))
  }

  val schema: StructType = StructType(
    Seq(
      StructField("int", IntegerType),
      StructField("str", StringType),
      StructField("struct", StructType(Seq(StructField("inner", IntegerType))))
    )
  )

  val attributes: Seq[AttributeReference] = DataTypeUtils.toAttributes(schema)

  def createLocalRelationProto(
      attrs: Seq[AttributeReference],
      data: Seq[InternalRow],
      timeZoneId: String = "UTC"
  ): proto.Relation = {
    val localRelationBuilder = proto.LocalRelation.newBuilder()

    val bytes = ArrowConverters
      .toBatchWithSchemaIterator(
        data.iterator,
        DataTypeUtils.fromAttributes(attrs.map(_.toAttribute)),
        Long.MaxValue,
        Long.MaxValue,
        timeZoneId,
        true
      )
      .next()

    localRelationBuilder.setData(ByteString.copyFrom(bytes))
    proto.Relation.newBuilder().setLocalRelation(localRelationBuilder.build()).build()
  }

  def createDummySessionHolder(session: SparkSession): SessionHolder = {
    val ret =
      SessionHolder(userId = "testUser", sessionId = UUID.randomUUID().toString, session = session)
    SparkConnectService.sessionManager.putSessionForTesting(ret)
    ret
  }

  test("plugin") {
    val left = createLocalRelationProto(attributes, leftRows)
    val right = createLocalRelationProto(attributes, rightRows)
    val options = DiffOptions.newBuilder().build()
    val diff = Diff
      .newBuilder()
      .setLeft(left.toByteString)
      .setRight(right.toByteString)
      .setOptions(options)
      .addIdColumn("int")
      .addIgnoreColumn("struct")
      .build()

    val planner = new SparkConnectPlanner(createDummySessionHolder(spark))
    val plan = new DiffConnectPlugin().transform(Any.pack(diff).toByteArray, planner)
    assert(plan.isPresent)
    val actual = Dataset.ofRows(spark, plan.get())

    val expected = Seq(
      Row("N", 0, "str-0", "str-0", Row(0), Row(0)),
      Row("D", 2, "str-2", null, Row(2), null),
      Row("I", 3, null, "str-3", null, Row(3)),
      Row("D", 4, "str-4", null, Row(4), null),
      Row("N", 6, "str-6", "str-6", Row(6), Row(6))
    )
    assert(actual.orderBy("int").collect() === expected)

    val expectedSchema = StructType(Seq(
      StructField("diff", StringType, nullable = false),
      StructField("int", IntegerType),
      StructField("left_str", StringType, nullable = true),
      StructField("right_str", StringType, nullable = true),
      StructField("left_struct", StructType(Seq(StructField("inner", IntegerType)))),
      StructField("right_struct", StructType(Seq(StructField("inner", IntegerType)))),
    ))
    assert(actual.schema === expectedSchema)
  }
}
