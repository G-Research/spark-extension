package uk.co.gresearch.spark.diff.comparator

import scala.math.Equiv

trait RelativeEpsilonDiffComparator[T] extends Equiv[T]

case class RelativeEpsilonIntDiffComparator(relativeEpsilon: Double) extends RelativeEpsilonDiffComparator[Int] {
  override def equiv(left: Int, right: Int): Boolean = {
    val delta = math.max(math.abs(left), math.abs(right)) * relativeEpsilon
    math.abs(left - right) <= delta
  }
}

case class RelativeEpsilonLongDiffComparator(relativeEpsilon: Double) extends RelativeEpsilonDiffComparator[Long] {
  override def equiv(left: Long, right: Long): Boolean = {
    val delta = math.max(math.abs(left), math.abs(right)) * relativeEpsilon
    math.abs(left - right) <= delta
  }
}

case class RelativeEpsilonFloatDiffComparator(relativeEpsilon: Double) extends RelativeEpsilonDiffComparator[Float] {
  override def equiv(left: Float, right: Float): Boolean = {
    val delta = math.max(math.abs(left), math.abs(right)) * relativeEpsilon
    math.abs(left - right) <= delta
  }
}

case class RelativeEpsilonDoubleDiffComparator(relativeEpsilon: Double) extends RelativeEpsilonDiffComparator[Double] {
  override def equiv(left: Double, right: Double): Boolean = {
    val delta = math.max(math.abs(left), math.abs(right)) * relativeEpsilon
    math.abs(left - right) <= delta
  }
}

object RelativeEpsilonDiffComparator {
  def forInt(relativeEpsilon: Double): RelativeEpsilonDiffComparator[Int] = RelativeEpsilonIntDiffComparator(relativeEpsilon)
  def forLong(relativeEpsilon: Double): RelativeEpsilonDiffComparator[Long] = RelativeEpsilonLongDiffComparator(relativeEpsilon)
  def forFloat(relativeEpsilon: Double): RelativeEpsilonDiffComparator[Float] = RelativeEpsilonFloatDiffComparator(relativeEpsilon)
  def forDouble(relativeEpsilon: Double): RelativeEpsilonDiffComparator[Double] = RelativeEpsilonDoubleDiffComparator(relativeEpsilon)
}
