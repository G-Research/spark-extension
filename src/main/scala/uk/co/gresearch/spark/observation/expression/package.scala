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

package uk.co.gresearch.spark.observation

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, collect_set, count, lit, when}

package object expression {
  /**
   * A convenience expression to be used with ExtendedDataset.observe.
   */
  trait ObservationExpression {
    def name: String
    def expr: Column
    def namedExpr: Column = expr.as(name)
  }

  /**
   * Turns any column expression to an ObservationExpression compatible with
   * ExtendedDataset.observe(Observation, ObservationExpression, ObservationExpression*).
   */
  case class As(expr: Column, name: String) extends ObservationExpression

  case class Set(column: Column, name: String) extends ObservationExpression {
    override def expr: Column = collect_set(column)
  }

  /**
   * Counts the rows where the column is not null.
   */
  case class NonNulls(column: Column, name: String) extends ObservationExpression {
    override def expr: Column = count(column)
  }

  /**
   * Counts the rows where the column is not null.
   */
  object NonNulls {
    def apply(column: String): NonNulls = NonNulls(column, f"$column-non-nulls")
    def apply(column: String, name: String): NonNulls = NonNulls(col(column), name)
  }

  /**
   * Counts the rows where the column is null.
   */
  case class Nulls(column: Column, name: String) extends ObservationExpression {
    override def expr: Column = count(when(column.isNull, lit(1)))
  }

  /**
   * Counts the rows where the column is not null.
   */
  object Nulls {
    def apply(column: String): Nulls = Nulls(column, f"$column-nulls")
    def apply(column: String, name: String): Nulls = Nulls(col(column), name)
  }

  /**
   * Counts the number of rows.
   */
  case class Rows(name: String = "rows") extends ObservationExpression {
    override def expr: Column = count(lit(1))
  }
}
