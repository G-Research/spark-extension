/*
 * Copyright 2022 G-Research
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

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.GroupBySortedSuite.{valueRowToTuple, valueToTuple}
import uk.co.gresearch.spark.group.SortedGroupByDataset

import scala.language.implicitConversions

case class Val(id: Int, seq: Int, value: Double)
case class State(init: Int) {
  var sum: Int = init

  def add(i: Int): Int = {
    sum = sum + i
    sum
  }
}

class GroupBySortedSuite extends AnyFunSpec with SparkTestSession {

  import spark.implicits._

  val ds: Dataset[Val] = Seq(
    Val(1, 1, 1.1),
    Val(1, 2, 1.2),
    Val(1, 3, 1.3),
    Val(1, 3, 1.31),

    Val(2, 1, 2.1),
    Val(2, 2, 2.2),
    Val(2, 3, 2.3),

    Val(3, 1, 3.1),
  ).reverse.toDS().repartition(3).cache()

  describe("test dataset") {
    it("is randomly partitioned") {
      val partitions: Array[List[Val]] = ds.mapPartitions(it => Iterator(it.toList)).collect()
      assert(partitions === Array(
        List(Val(1,3,1.31), Val(1,1,1.1)),
        List(Val(1,2,1.2), Val(1,3,1.3), Val(2,1,2.1)),
        List(Val(3,1,3.1), Val(2,2,2.2), Val(2,3,2.3))
      ))
    }
  }

  describe("ds.groupBySorted") {
    testGroupByIdSortBySeq(ds.groupBySorted($"id")($"seq", $"value"))
    testGroupByIdSortBySeqDesc(ds.groupBySorted($"id")($"seq".desc, $"value".desc))
    testGroupByIdSortBySeqWithPartitionNum(ds.groupBySorted(10)($"id")($"seq", $"value"))
    testGroupByIdSortBySeqDescWithPartitionNum(ds.groupBySorted(10)($"id")($"seq".desc, $"value".desc))
    testGroupByIdSeqSortByValue(ds.groupBySorted($"id", $"seq")($"value"))
  }

  describe("ds.groupByKeySorted") {
    testGroupByIdSortBySeq(ds.groupByKeySorted(v => v.id)(v => (v.seq, v.value)))
    testGroupByIdSortBySeqDesc(ds.groupByKeySorted(v => v.id)(v => (v.seq, v.value), reverse = true))
    testGroupByIdSortBySeqWithPartitionNum(ds.groupByKeySorted(v => v.id, partitions = Some(10))(v => (v.seq, v.value)))
    testGroupByIdSortBySeqDescWithPartitionNum(ds.groupByKeySorted(v => v.id, partitions = Some(10))(v => (v.seq, v.value), reverse = true))
    testGroupByIdSeqSortByValue(ds.groupByKeySorted(v => (v.id, v.seq))(v => v.value))
  }

  val df: DataFrame = ds.toDF()

  describe("df.groupBySorted") {
    testGroupByIdSortBySeq(df.groupBySorted($"id")($"seq", $"value"))
    testGroupByIdSortBySeqDesc(df.groupBySorted($"id")($"seq".desc, $"value".desc))
    testGroupByIdSortBySeqWithPartitionNum(df.groupBySorted(10)($"id")($"seq", $"value"))
    testGroupByIdSortBySeqDescWithPartitionNum(df.groupBySorted(10)($"id")($"seq".desc, $"value".desc))
    testGroupByIdSeqSortByValue(df.groupBySorted($"id", $"seq")($"value"))
  }

  describe("df.groupByKeySorted") {
    testGroupByIdSortBySeq(df.groupByKeySorted(v => v.getInt(0))(v => (v.getInt(1), v.getDouble(2))))
    testGroupByIdSortBySeqDesc(df.groupByKeySorted(v => v.getInt(0))(v => (v.getInt(1), v.getDouble(2)), reverse = true))
    testGroupByIdSortBySeqWithPartitionNum(df.groupByKeySorted(v => v.getInt(0), partitions = Some(10))(v => (v.getInt(1), v.getDouble(2))))
    testGroupByIdSortBySeqDescWithPartitionNum(df.groupByKeySorted(v => v.getInt(0), partitions = Some(10))(v => (v.getInt(1), v.getDouble(2)), reverse = true))
    testGroupByIdSeqSortByValue(df.groupByKeySorted(v => (v.getInt(0), v.getInt(1)))(v => v.getDouble(2)))
  }


  def testGroupByIdSortBySeq[T](ds: SortedGroupByDataset[Int, T])
                               (implicit asTuple: T => (Int, Int, Double)): Unit = {

    it("should flatMapSortedGroups") {
      val actual = ds
        .flatMapSortedGroups((key, it) => it.zipWithIndex.map(v => (key, v._2, asTuple(v._1))))
        .collect()
        .sortBy(v => (v._1, v._2))

      val expected = Seq(
        // (key, group index, value)
        (1, 0, (1, 1, 1.1)),
        (1, 1, (1, 2, 1.2)),
        (1, 2, (1, 3, 1.3)),
        (1, 3, (1, 3, 1.31)),

        (2, 0, (2, 1, 2.1)),
        (2, 1, (2, 2, 2.2)),
        (2, 2, (2, 3, 2.3)),

        (3, 0, (3, 1, 3.1)),
      )

      assert(actual === expected)
    }

    it("should flatMapSortedGroups with state") {
      val actual = ds
        .flatMapSortedGroups(key => State(key))((state, v) => Iterator((asTuple(v), state.add(asTuple(v)._2))))
        .collect()
        .sortBy(v => (v._1._1, v._1._2))

      val expected = Seq(
        // (value, state)
        ((1, 1, 1.1), 1 + 1),
        ((1, 2, 1.2), 1 + 1 + 2),
        ((1, 3, 1.3), 1 + 1 + 2 + 3),
        ((1, 3, 1.31), 1 + 1 + 2 + 3 + 3),

        ((2, 1, 2.1), 2 + 1),
        ((2, 2, 2.2), 2 + 1 + 2),
        ((2, 3, 2.3), 2 + 1 + 2 + 3),

        ((3, 1, 3.1), 3 + 1),
      )

      assert(actual === expected)
    }

  }

  def testGroupByIdSortBySeqDesc[T](ds: SortedGroupByDataset[Int, T])
                                   (implicit asTuple: T => (Int, Int, Double)): Unit = {
    it("should flatMapSortedGroups reverse") {
      val actual = ds
        .flatMapSortedGroups((key, it) => it.zipWithIndex.map(v => (key, v._2, asTuple(v._1))))
        .collect()
        .sortBy(v => (v._1, v._2))

      val expected = Seq(
        // (key, group index, value)
        (1, 0, (1, 3, 1.31)),
        (1, 1, (1, 3, 1.3)),
        (1, 2, (1, 2, 1.2)),
        (1, 3, (1, 1, 1.1)),

        (2, 0, (2, 3, 2.3)),
        (2, 1, (2, 2, 2.2)),
        (2, 2, (2, 1, 2.1)),

        (3, 0, (3, 1, 3.1)),
      )

      assert(actual === expected)
    }

  }

  def testGroupByIdSortBySeqWithPartitionNum[T](ds: SortedGroupByDataset[Int, T], partitions: Int = 10)
                                               (implicit asTuple: T => (Int, Int, Double)): Unit = {

    it("should flatMapSortedGroups with partition num") {
      val grouped = ds
        .flatMapSortedGroups((key, it) => it.zipWithIndex.map(v => (key, v._2, asTuple(v._1))))
      val actual = grouped
        .collect()
        .sortBy(v => (v._1, v._2))

      val expected = Seq(
        // (key, group index, value)
        (1, 0, (1, 1, 1.1)),
        (1, 1, (1, 2, 1.2)),
        (1, 2, (1, 3, 1.3)),
        (1, 3, (1, 3, 1.31)),

        (2, 0, (2, 1, 2.1)),
        (2, 1, (2, 2, 2.2)),
        (2, 2, (2, 3, 2.3)),

        (3, 0, (3, 1, 3.1)),
      )

      val partitionSizes = grouped.mapPartitions(it => Iterator.single(it.length)).collect()
      assert(partitionSizes.length === partitions)
      assert(partitionSizes.sum === actual.length)
      assert(actual === expected)
    }

  }

  def testGroupByIdSortBySeqDescWithPartitionNum[T](ds: SortedGroupByDataset[Int, T], partitions: Int = 10)
                                                   (implicit asTuple: T => (Int, Int, Double)): Unit = {
    it("should flatMapSortedGroups with partition num and reverse") {
      val grouped = ds
        .flatMapSortedGroups((key, it) => it.zipWithIndex.map(v => (key, v._2, asTuple(v._1))))
      val actual = grouped
        .collect()
        .sortBy(v => (v._1, v._2))

      val expected = Seq(
        // (key, group index, value)
        (1, 0, (1, 3, 1.31)),
        (1, 1, (1, 3, 1.3)),
        (1, 2, (1, 2, 1.2)),
        (1, 3, (1, 1, 1.1)),

        (2, 0, (2, 3, 2.3)),
        (2, 1, (2, 2, 2.2)),
        (2, 2, (2, 1, 2.1)),

        (3, 0, (3, 1, 3.1)),
      )

      val partitionSizes = grouped.mapPartitions(it => Iterator.single(it.length)).collect()
      assert(partitionSizes.length === partitions)
      assert(partitionSizes.sum === actual.length)
      assert(actual === expected)
    }
  }

  def testGroupByIdSeqSortByValue[T](ds: SortedGroupByDataset[(Int, Int), T])
                                    (implicit asTuple: T => (Int, Int, Double)): Unit = {

    it("should flatMapSortedGroups with tuple key") {
      val actual = ds
        .flatMapSortedGroups((key, it) => it.zipWithIndex.map(v => (key, v._2, asTuple(v._1))))
        .collect()
        .sortBy(v => (v._1, v._2))

      val expected = Seq(
        // (key, group index, value)
        ((1, 1), 0, (1, 1, 1.1)),

        ((1, 2), 0, (1, 2, 1.2)),

        ((1, 3), 0, (1, 3, 1.3)),
        ((1, 3), 1, (1, 3, 1.31)),

        ((2, 1), 0, (2, 1, 2.1)),

        ((2, 2), 0, (2, 2, 2.2)),

        ((2, 3), 0, (2, 3, 2.3)),

        ((3, 1), 0, (3, 1, 3.1)),
      )

      assert(actual === expected)
    }

    it("should flatMapSortedGroups with tuple key and state") {
      val actual = ds
        .flatMapSortedGroups(key => State(key._1))((state, v) => Iterator((asTuple(v), state.add(asTuple(v)._2))))
        .collect()
        .sortBy(v => (v._1._1, v._1._2))

      val expected = Seq(
        // (value, state)
        ((1, 1, 1.1), 1 + 1),
        ((1, 2, 1.2), 1 + 2),
        ((1, 3, 1.3), 1 + 3),
        ((1, 3, 1.31), 1 + 3 + 3),

        ((2, 1, 2.1), 2 + 1),
        ((2, 2, 2.2), 2 + 2),
        ((2, 3, 2.3), 2 + 3),

        ((3, 1, 3.1), 3 + 1),
      )

      assert(actual === expected)
    }

  }

}


object GroupBySortedSuite {
  implicit def valueToTuple(value: Val): (Int, Int, Double) = (value.id, value.seq, value.value)
  implicit def valueRowToTuple(value: Row): (Int, Int, Double) = (value.getInt(0), value.getInt(1), value.getDouble(2))
}
