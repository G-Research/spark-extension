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

import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Spark version specific trait that back-ports BinaryLike[T].withNewChildrenInternal(T, T)
 * to Spark 3.0 and 3.1. This is empty in Spark 3.2 and beyond.
 */
trait BinaryLikeWithNewChildrenInternal[T <: TreeNode[T]] {
  self: TreeNode[T] =>

  /**
   * Method `withNewChildrenInternal` is required for Spark 3.2 and beyond.
   * Before, `withNewChildren` is called by Spark, which uses `makeCopy`, which
   *   "Must be overridden by child classes that have constructor arguments
   *    that are not present in the productIterator.",
   * which is not true for where BinaryLikeWithNewChildrenInternal is used here.
   * So nothing need to be overridden.
   */
  protected def withNewChildrenInternal(newLeft: T, newRight: T): T
}