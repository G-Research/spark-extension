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

package uk.co.gresearch.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, count, lit, when}

package object functions {
  /**
   * Similar to org.apache.spark.sql.functions.count, except that it counts the null values
   * of the given column.
   */
  def count_null(column: Column): Column = count(when(column.isNull, lit(1))).as(f"count_null($column)")

  /**
   * Similar to org.apache.spark.sql.functions.count, except that it counts the null values
   * of the given column.
   */
  def count_null(column: String): Column = count_null(col(column))
}
