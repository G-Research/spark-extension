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

package uk.co.gresearch.spark.diff

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types.DataType
import uk.co.gresearch.spark.diff.comparator._

import java.time.Duration

object DiffComparators {

  /**
   * The default comparator used in [[DiffOptions.default.defaultComparator]].
   */
  def default(): DiffComparator = DefaultDiffComparator

  /**
   * A comparator equivalent to `Column <=> Column`. Null values are considered equal.
   */
  def nullSafeEqual(): DiffComparator = NullSafeEqualDiffComparator

  /**
   * Return a comparator that uses the given [[math.Equiv]] to compare values of type [[T]]. The implicit [[Encoder]] of
   * type [[T]] determines the input data type of the comparator. Only columns of that type can be compared.
   */
  def equiv[T: Encoder](equiv: math.Equiv[T]): EquivDiffComparator[T] = EquivDiffComparator(equiv)

  /**
   * Return a comparator that uses the given [[math.Equiv]] to compare values of type [[T]]. Only columns of the given
   * data type `inputType` can be compared.
   */
  def equiv[T](equiv: math.Equiv[T], inputType: DataType): EquivDiffComparator[T] =
    EquivDiffComparator(equiv, inputType)

  /**
   * Return a comparator that uses the given [[math.Equiv]] to compare values of any type.
   */
  def equiv(equiv: math.Equiv[Any]): EquivDiffComparator[Any] = EquivDiffComparator(equiv)

  /**
   * This comparator considers values equal when they are less than `epsilon` apart. It can be configured to use
   * `epsilon` as an absolute (`.asAbsolute()`) threshold, or as relative (`.asRelative()`) to the larger value.
   * Further, the threshold itself can be considered equal (`.asInclusive()`) or not equal (`.asExclusive()`):
   *
   * <ul> <li>`DiffComparator.epsilon(epsilon).asAbsolute().asInclusive()`: `abs(left - right) ≤ epsilon`</li>
   * <li>`DiffComparator.epsilon(epsilon).asAbsolute().asExclusive()`: `abs(left - right) < epsilon`</li>
   * <li>`DiffComparator.epsilon(epsilon).asRelative().asInclusive()`: `abs(left - right) ≤ epsilon * max(abs(left),
   * abs(right))`</li> <li>`DiffComparator.epsilon(epsilon).asRelative().asExclusive()`: `abs(left - right) < epsilon *
   * max(abs(left), abs(right))`</li> </ul>
   *
   * Requires compared column types to implement `-`, `*`, `<`, `==`, and `abs`.
   */
  def epsilon(epsilon: Double): EpsilonDiffComparator = EpsilonDiffComparator(epsilon)

  /**
   * A comparator for string values.
   *
   * With `whitespaceAgnostic` set `true`, differences in white spaces are ignored. This ignores leading and trailing
   * whitespaces as well. With `whitespaceAgnostic` set `false`, this is equal to the default string comparison (see
   * [[default()]]).
   */
  def string(whitespaceAgnostic: Boolean = true): StringDiffComparator =
    if (whitespaceAgnostic) {
      WhitespaceDiffComparator
    } else {
      StringDiffComparator
    }

  /**
   * This comparator considers two `DateType` or `TimestampType` values equal when they are at most `duration` apart.
   * Duration is an instance of `java.time.Duration`.
   *
   * The comparator can be configured to consider `duration` as equal (`.asInclusive()`) or not equal
   * (`.asExclusive()`): <ul> <li>`DiffComparator.duration(duration).asInclusive()`: `left - right ≤ duration`</li>
   * <li>`DiffComparator.duration(duration).asExclusive()`: `left - right < duration`</li> </lu>
   */
  def duration(duration: Duration): DurationDiffComparator = DurationDiffComparator(duration)

  /**
   * This comparator compares two `Map[K,V]` values. They are equal when they match in all their keys and values.
   */
  def map[K: Encoder, V: Encoder](): DiffComparator = MapDiffComparator[K, V](keyOrderSensitive = false)

  /**
   * This comparator compares two `Map[keyType,valueType]` values. They are equal when they match in all their keys and
   * values.
   */
  def map(keyType: DataType, valueType: DataType, keyOrderSensitive: Boolean = false): DiffComparator =
    MapDiffComparator(keyType, valueType, keyOrderSensitive)

  // for backward compatibility to v2.4.0 up to v2.8.0
  // replace with default value in above map when moving to v3
  /**
   * This comparator compares two `Map[K,V]` values. They are equal when they match in all their keys and values.
   *
   * @param keyOrderSensitive
   *   comparator compares key order if true
   */
  def map[K: Encoder, V: Encoder](keyOrderSensitive: Boolean): DiffComparator =
    MapDiffComparator[K, V](keyOrderSensitive)
}
