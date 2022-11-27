package uk.co.gresearch.spark.diff.comparator

import scala.math.Equiv

trait AbsoluteEpsilonDiffComparator[T] extends Equiv[T]

case class AbsoluteEpsilonIntDiffComparator(absEpsilon: Int) extends AbsoluteEpsilonDiffComparator[Int] {
  override def equiv(left: Int, right: Int): Boolean =
    math.abs(left - right) <= absEpsilon
}

case class AbsoluteEpsilonLongDiffComparator(absEpsilon: Long) extends AbsoluteEpsilonDiffComparator[Long] {
  override def equiv(left: Long, right: Long): Boolean =
    math.abs(left - right) <= absEpsilon
}

case class AbsoluteEpsilonFloatDiffComparator(absEpsilon: Float) extends AbsoluteEpsilonDiffComparator[Float] {
  override def equiv(left: Float, right: Float): Boolean =
    math.abs(left - right) <= absEpsilon
}

case class AbsoluteEpsilonDoubleDiffComparator(absEpsilon: Double) extends AbsoluteEpsilonDiffComparator[Double] {
  override def equiv(left: Double, right: Double): Boolean =
    math.abs(left - right) <= absEpsilon
}

object AbsoluteEpsilonDiffComparator {
  def apply(relativeEpsilon: Int): AbsoluteEpsilonDiffComparator[Int] = AbsoluteEpsilonIntDiffComparator(relativeEpsilon)
  def apply(relativeEpsilon: Long): AbsoluteEpsilonDiffComparator[Long] = AbsoluteEpsilonLongDiffComparator(relativeEpsilon)
  def apply(relativeEpsilon: Float): AbsoluteEpsilonDiffComparator[Float] = AbsoluteEpsilonFloatDiffComparator(relativeEpsilon)
  def apply(relativeEpsilon: Double): AbsoluteEpsilonDiffComparator[Double] = AbsoluteEpsilonDoubleDiffComparator(relativeEpsilon)
}
