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

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.funspec.AnyFunSpec

case class Val(id: Int, seq: Int, value: Double)
case class State(key: Int) {
  var sum: Int = key

  def add(i: Int): Int = {
    sum = sum + i
    sum
  }
}

class GroupByKeySortedSuite extends AnyFunSpec with SparkTestSession {

  import spark.implicits._

  val ds: Dataset[Val] = Seq(
    Val(1, 1, 1.1),
    Val(1, 2, 1.2),
    Val(1, 3, 1.3),

    Val(2, 1, 2.1),
    Val(2, 2, 2.2),
    Val(2, 3, 2.3),

    Val(3, 1, 3.1),
  ).reverse.toDS().repartition(3).cache()

  describe("test dataset") {
    it("is randomly partitioned") {
      val partitions: Array[List[Val]] = ds.mapPartitions(it => Iterator(it.toList)).collect()
      assert(partitions === Array(
        List(Val(1, 3, 1.3), Val(2, 1, 2.1)),
        List(Val(1, 2, 1.2), Val(2, 2, 2.2), Val(2, 3, 2.3)),
        List(Val(3, 1, 3.1), Val(1, 1, 1.1))
      ))
    }
  }

  describe("ds.groupByKeySorted") {

    it("should flatMapSortedGroups") {
      val actual = ds
        .groupByKeySorted(v => v.id)(v => v.seq)
        .flatMapSortedGroups((key, it) => it.zipWithIndex.map(v => (key, v._2, v._1)))
        .collect()
        .sortBy(v => (v._1, v._2))

      val expected = Seq(
        // (key, group index, value)
        (1, 0, Val(1, 1, 1.1)),
        (1, 1, Val(1, 2, 1.2)),
        (1, 2, Val(1, 3, 1.3)),

        (2, 0, Val(2, 1, 2.1)),
        (2, 1, Val(2, 2, 2.2)),
        (2, 2, Val(2, 3, 2.3)),

        (3, 0, Val(3, 1, 3.1)),
      )

      assert(actual === expected)
    }

    it("should flatMapSortedGroups with state") {
      val actual = ds
        .groupByKeySorted(v => v.id)(v => v.seq)
        .flatMapSortedGroups(key => State(key))((state, v) => Iterator((v, state.add(v.seq))))
        .collect()
        .sortBy(v => (v._1.id, v._1.seq))

      val expected = Seq(
        // (value, state)
        (Val(1, 1, 1.1), 1 + 1),
        (Val(1, 2, 1.2), 1 + 1 + 2),
        (Val(1, 3, 1.3), 1 + 1 + 2 + 3),

        (Val(2, 1, 2.1), 2 + 1),
        (Val(2, 2, 2.2), 2 + 1 + 2),
        (Val(2, 3, 2.3), 2 + 1 + 2 + 3),

        (Val(3, 1, 3.1), 3 + 1),
      )

      assert(actual === expected)
    }

    /**
     *  it("should flatMapSortedGroups with partition num") {
     *  val actual = ds
     *  .groupByKeySorted(key = v => v.id, partitions = Some(10))(v => v.seq)
     *  .flatMapSortedGroups((key, it) => it.zipWithIndex.map(v => (key, v._2, v._1)))
     *  .collect()
     *  .sortBy(v => (v._1, v._2))
     *
     *  val expected = Seq(
     *  // (key, group index, value)
     *  (1, 0, Val(1, 1, 1.1)),
     *  (1, 1, Val(1, 2, 1.2)),
     *  (1, 2, Val(1, 3, 1.3)),
     *
     *  (2, 0, Val(2, 1, 2.1)),
     *  (2, 1, Val(2, 2, 2.2)),
     *  (2, 2, Val(2, 3, 2.3)),
     *
     *  (3, 0, Val(3, 1, 3.1)),
     *  )
     *
     *  assert(actual === expected)
     *  }
     */
    it("should flatMapSortedGroups reverse") {
      val actual = ds
        .groupByKeySorted(key = v => v.id)(order = v => v.seq, reverse = true)
        .flatMapSortedGroups((key, it) => it.zipWithIndex.map(v => (key, v._2, v._1)))
        .collect()
        .sortBy(v => (v._1, v._2))

      val expected = Seq(
        // (key, group index, value)
        (1, 0, Val(1, 3, 1.3)),
        (1, 1, Val(1, 2, 1.2)),
        (1, 2, Val(1, 1, 1.1)),

        (2, 0, Val(2, 3, 2.3)),
        (2, 1, Val(2, 2, 2.2)),
        (2, 2, Val(2, 1, 2.1)),

        (3, 0, Val(3, 1, 3.1)),
      )

      assert(actual === expected)
    }

    /**
     *  it("should flatMapSortedGroups with partition num and reverse") {
     *  val actual = ds
     *  .groupByKeySorted(key = v => v.id, partitions = Some(10))(order = v => v.seq, reverse = true)
     *  .flatMapSortedGroups((key, it) => it.zipWithIndex.map(v => (key, v._2, v._1)))
     *  .collect()
     *  .sortBy(v => (v._1, v._2))
     *
     *  val expected = Seq(
     *  // (key, group index, value)
     *  (1, 0, Val(1, 1, 1.1)),
     *  (1, 1, Val(1, 2, 1.2)),
     *  (1, 2, Val(1, 3, 1.3)),
     *
     *  (2, 0, Val(2, 1, 2.1)),
     *  (2, 1, Val(2, 2, 2.2)),
     *  (2, 2, Val(2, 3, 2.3)),
     *
     *  (3, 0, Val(3, 1, 3.1)),
     *  )
     *
     *  assert(actual === expected)
     *  }
     */
  }

  describe("df.groupByKeySorted") {

    val df = ds.toDF()

    def valueRowToTuple(value: Row): (Int, Int, Double) = (value.getInt(0), value.getInt(1), value.getDouble(2))

    it("should flatMapSortedGroups") {
      val actual = df
        .groupByKeySorted(v => v.getInt(0))(v => v.getInt(1))
        .flatMapSortedGroups((key, it) => it.zipWithIndex.map(v => (key, v._2, valueRowToTuple(v._1))))
        .collect()
        .sorted

      val expected = Seq(
        // (key, group index, value)
        (1, 0, (1, 1, 1.1)),
        (1, 1, (1, 2, 1.2)),
        (1, 2, (1, 3, 1.3)),

        (2, 0, (2, 1, 2.1)),
        (2, 1, (2, 2, 2.2)),
        (2, 2, (2, 3, 2.3)),

        (3, 0, (3, 1, 3.1)),
      )

      assert(actual === expected)
    }

    it("should flatMapSortedGroups with state") {
      val actual = df
        .groupByKeySorted(v => v.getInt(0))(v => v.getInt(1))
        .flatMapSortedGroups(key => State(key))((state, v) => Iterator((valueRowToTuple(v), state.add(v.getInt(1)))))
        .collect()
        .sorted

      val expected = Seq(
        // (value, state)
        ((1, 1, 1.1), 1 + 1),
        ((1, 2, 1.2), 1 + 1 + 2),
        ((1, 3, 1.3), 1 + 1 + 2 + 3),

        ((2, 1, 2.1), 2 + 1),
        ((2, 2, 2.2), 2 + 1 + 2),
        ((2, 3, 2.3), 2 + 1 + 2 + 3),

        ((3, 1, 3.1), 3 + 1),
      )

      assert(actual === expected)
    }
  }

}
