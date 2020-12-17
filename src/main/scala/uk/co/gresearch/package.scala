/*
 * Copyright 2020 G-Research
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

package uk.co

package object gresearch {

  trait ConditionalCall[T] {
    def call(f: T => T): T
    def either[R](f: T => R): ConditionalCallOr[T,R]
  }

  trait ConditionalCallOr[T,R] {
    def or(f: T => R): R
  }

  case class TrueCall[T](t: T) extends ConditionalCall[T] {
    override def call(f: T => T): T = f(t)
    override def either[R](f: T => R): ConditionalCallOr[T,R] = TrueCallOr[T,R](f(t))
  }

  case class FalseCall[T](t: T) extends ConditionalCall[T] {
    override def call(f: T => T): T = t
    override def either[R](f: T => R): ConditionalCallOr[T,R] = FalseCallOr[T,R](t)
  }

  case class TrueCallOr[T,R](r: R) extends ConditionalCallOr[T,R] {
    override def or(f: T => R): R = r
  }

  case class FalseCallOr[T,R](t: T) extends ConditionalCallOr[T,R] {
    override def or(f: T => R): R = f(t)
  }

  implicit class ExtendedAny[T](t: T) {

    /**
     * Allows to call a function on the decorated instance conditionally.
     *
     * This allows fluent code like
     *
     * {{{
     * i.doThis()
     *  .doThat()
     *  .on(condition).call(function)
     *  .on(condition).either(function1).or(function2)
     *  .doMore()
     * }}}
     *
     * rather than
     *
     * {{{
     * val temp = i.doThis()
     *             .doThat()
     * val temp2 = if (condition) function(temp) else temp
     * temp2.doMore()
     * }}}
     *
     * which either needs many temporary variables or duplicate code.
     *
     * @param condition condition
     * @return the function result
     */
    def on(condition: Boolean): ConditionalCall[T] = {
      if (condition) TrueCall[T](t) else FalseCall[T](t)
    }

    /**
     * Allows to call a function on the decorated instance conditionally.
     * This is an alias for the `on` function.
     *
     * This allows fluent code like
     *
     * {{{
     * i.doThis()
     *  .doThat()
     *  .when(condition).call(function)
     *  .when(condition).either(function1).or(function2)
     *  .doMore()
     *
     *
     * rather than
     *
     * {{{
     * val temp = i.doThis()
     *             .doThat()
     * val temp2 = if (condition) function(temp) else temp
     * temp2.doMore()
     * }}}
     *
     * which either needs many temporary variables or duplicate code.
     *
     * @param condition condition
     * @return the function result
     */
    def when(condition: Boolean): ConditionalCall[T] = on(condition)

    /**
     * Executes the given function on the decorated instance.
     *
     * This allows writing fluent code like
     *
     * {{{
     * i.doThis()
     *  .doThat()
     *  .call(function)
     *  .doMore()
     * }}}
     *
     * rather than
     *
     * {{{
     * function(
     *   i.doThis()
     *    .doThat()
     * ).doMore()
     * }}}
     *
     * where the effective sequence of operations is not clear.
     *
     * @param f function
     * @return the function result
     */
    def call[R](f: T => R): R = f(t)
  }

}
