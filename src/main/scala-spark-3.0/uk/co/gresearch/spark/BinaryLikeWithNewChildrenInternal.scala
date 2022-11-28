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
