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

package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData
import org.apache.spark.sql.types.{DataType, MapType}
import org.apache.spark.sql.{Column, Encoder}

import scala.reflect.ClassTag

case class MapDiffComparator[K, V](private val comparator: EquivDiffComparator[UnsafeMapData]) extends DiffComparator {
  override def equiv(left: Column, right: Column): Column = comparator.equiv(left, right)
}

private case class MapDiffEquiv[K: ClassTag, V](keyType: DataType, valueType: DataType, keyOrderSensitive: Boolean) extends math.Equiv[UnsafeMapData] {
  override def equiv(left: UnsafeMapData, right: UnsafeMapData): Boolean = {

    val leftKeys: Array[K] = left.keyArray().toArray(keyType)
    val rightKeys: Array[K] = right.keyArray().toArray(keyType)

    val leftKeysIndices: Map[K, Int] = leftKeys.zipWithIndex.toMap
    val rightKeysIndices: Map[K, Int] = rightKeys.zipWithIndex.toMap

    val leftValues = left.valueArray()
    val rightValues = right.valueArray()

    // can only be evaluated when right has same keys as left
    lazy val valuesAreEqual = leftKeysIndices
      .map { case (key, index) => index -> rightKeysIndices(key) }
      .map { case (leftIndex, rightIndex) => (leftIndex, rightIndex, leftValues.isNullAt(leftIndex), rightValues.isNullAt(rightIndex)) }
      .map { case (leftIndex, rightIndex, leftIsNull, rightIsNull) =>
        leftIsNull && rightIsNull ||
          !leftIsNull && !rightIsNull && leftValues.get(leftIndex, valueType).equals(rightValues.get(rightIndex, valueType))
      }

    left.numElements() == right.numElements() &&
      (keyOrderSensitive && leftKeys.sameElements(rightKeys) || !keyOrderSensitive && leftKeys.toSet.diff(rightKeys.toSet).isEmpty) &&
      valuesAreEqual.forall(identity)
  }
}

case object MapDiffComparator {
  def apply[K: Encoder, V: Encoder](keyOrderSensitive: Boolean = false): MapDiffComparator[K, V] = {
    val keyType = encoderFor[K].schema.fields(0).dataType
    val valueType = encoderFor[V].schema.fields(0).dataType
    val equiv = MapDiffEquiv(keyType, valueType, keyOrderSensitive)
    val dataType = MapType(keyType, valueType)
    val comparator = InputTypedEquivDiffComparator[UnsafeMapData](equiv, dataType)
    MapDiffComparator[K, V](comparator)
  }
}
