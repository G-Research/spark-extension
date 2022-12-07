package uk.co.gresearch.spark.diff.comparator

import org.apache.spark.sql.Column

trait DiffComparator {
  def equiv(left: Column, right: Column): Column
}
