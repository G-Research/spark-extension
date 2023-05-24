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

case class MapDiffComparator[K, V](private val comparator: EquivDiffComparator[UnsafeMapData]) extends DiffComparator {
  override def equiv(left: Column, right: Column): Column = comparator.equiv(left, right)
}

private case class MapDiffEquiv[K, V](keyType: DataType, valueType: DataType) extends math.Equiv[UnsafeMapData] {
  override def equiv(left: UnsafeMapData, right: UnsafeMapData): Boolean = {
    val leftKeys: Map[K, Int] = 0.until(left.keyArray().numElements())
      .map(ordinal => left.keyArray().get(ordinal, keyType).asInstanceOf[K] -> ordinal)
      .toMap
    val rightKeys: Map[K, Int] = 0.until(left.keyArray().numElements())
      .map(ordinal => left.keyArray().get(ordinal, keyType).asInstanceOf[K] -> ordinal)
      .toMap

    val leftValues = left.valueArray()
    val rightValues = right.valueArray()

    val valuesAreEqual = leftKeys
      .map { case (key, ordinal) => ordinal -> rightKeys(key) }
      .map { case (leftOrdinal, rightOrdinal) => (leftOrdinal, rightOrdinal, leftValues.isNullAt(leftOrdinal), rightValues.isNullAt(rightOrdinal)) }
      .map { case (leftOrdinal, rightOrdinal, leftIsNull, rightIsNull) =>
        leftIsNull && rightIsNull ||
          !leftIsNull && !rightIsNull && leftValues.get(leftOrdinal, valueType).equals(rightValues.get(rightOrdinal, valueType))
      }

    left.numElements() == right.numElements() &&
      leftKeys.keySet.diff(rightKeys.keySet).isEmpty &&
      valuesAreEqual.forall(identity)
  }
}

case object MapDiffComparator {
  def apply[K: Encoder, V: Encoder](): MapDiffComparator[K, V] = {
    val keyType = encoderFor[K].schema.fields(0).dataType
    val valueType = encoderFor[V].schema.fields(0).dataType
    val equiv = MapDiffEquiv(keyType, valueType)
    val dataType = MapType(keyType, valueType)
    val comparator = InputTypedEquivDiffComparator[UnsafeMapData](equiv, dataType)
    MapDiffComparator[K, V](comparator)
  }
}
