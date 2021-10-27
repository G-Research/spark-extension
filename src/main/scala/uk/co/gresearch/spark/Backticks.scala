/*
 * Copyright 2021 G-Research
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

object Backticks {

  /**
   * Encloses the given strings with backticks if needed. Multiple strings will be enclosed individually and
   * concatenated with dots (`.`).
   *
   * This is useful when referencing column names that contain special characters like dots (`.`).
   *
   * Examples:
   * {{{
   *   col("a.column")                                    // this references the field "column" of column "a"
   *   col("`a.column`")                                  // this reference the column with the name "a.column"
   *   col(Backticks.column_name("column"))               // produces "column"
   *   col(Backticks.column_name("a.column"))             // produces "`a.column`"
   *   col(Backticks.column_name("`a.column`"))           // produces "`a.column`"
   *   col(Backticks.column_name("a.column", "a.field"))  // produces "`a.column`.`a.field`"
   * }}}
   *
   * @param string  a string
   * @param strings more strings
   * @return
   */
  @scala.annotation.varargs
  def column_name(string: String, strings: String*): String = (string +: strings)
    .map(s => if (s.contains(".") && !s.startsWith("`") && !s.endsWith("`")) s"`$s`" else s).mkString(".")

}
