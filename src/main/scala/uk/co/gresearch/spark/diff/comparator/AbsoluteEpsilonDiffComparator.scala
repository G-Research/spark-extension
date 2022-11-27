package uk.co.gresearch.spark.diff.comparator

import scala.math.Equiv

case class EpsilonDiffComparator(relativeEpsilon: Double) extends Equiv[Int] {
  override def equiv(left: Int, right: Int): Boolean = {
    val delta = math.max(math.abs(left), math.abs(right)) * relativeEpsilon
    math.abs(left - right) <= delta
  }
}
