package uk.co.gresearch.spark

/**
 * Spark version specific trait that back-ports BinaryLike[T].withNewChildrenInternal(T, T)
 * to Spark 3.0 and 3.1. This is empty in Spark 3.2 and beyond.
 */
trait BinaryLikeWithNewChildrenInternal[T]
