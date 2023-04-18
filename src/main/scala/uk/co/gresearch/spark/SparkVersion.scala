package uk.co.gresearch.spark

import org.apache.spark.SPARK_VERSION_SHORT

trait SparkVersion {
  private def SparkVersionSeq: Seq[Int] = SPARK_VERSION_SHORT.split('.').toSeq.map(_.toInt)

  def SparkMajorVersion: Int = SparkVersionSeq.head
  def SparkMinorVersion: Int = SparkVersionSeq(1)
  def SparkPatchVersion: Int = SparkVersionSeq(2)

  def SparkVersion: (Int, Int, Int) = (SparkMajorVersion, SparkMinorVersion, SparkPatchVersion)
  def SparkCompatVersion: (Int, Int) = (SparkMajorVersion, SparkMinorVersion)
  def SparkCompatVersionString: String = SparkVersionSeq.slice(0, 2).mkString(".")
}
