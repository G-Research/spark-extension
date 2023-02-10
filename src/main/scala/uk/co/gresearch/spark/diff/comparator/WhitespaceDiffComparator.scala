package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.unsafe.types.UTF8String

case object WhitespaceDiffComparator extends TypedEquivDiffComparatorWithInput[UTF8String] with StringDiffComparator {
  override val equiv: scala.Equiv[UTF8String] = (x: UTF8String, y: UTF8String) =>
    x == null && y == null ||
      x != null && y != null &&
        x.trimAll().toString.replaceAll("\\s+"," ").equals(
          y.trimAll().toString.replaceAll("\\s+"," ")
        )
}
