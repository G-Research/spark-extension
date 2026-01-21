package uk.co.gresearch.spark.source

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum
import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.spark.SparkTestSession

class SourceSuite extends AnyFunSuite with SparkTestSession with AdaptiveSparkPlanHelper {
  import spark.implicits._

  private val source = new DefaultSource().getClass.getPackage.getName
  private def df: DataFrameReader = spark.read.format(source)
  private val dfpartitioned = df.option("partitioned", value = true)
  private val dfpartitionedAndSorted = df.option("partitioned", value = true).option("ordered", value = true)
  private val window = Window.partitionBy($"id").orderBy($"time")

  test("show") {
    df.load().show()
  }

  test("groupBy without partition information") {
    assertPlan(
      df.load().groupBy($"id").count(),
      { case e: Exchange => e },
      expected = true
    )
  }

  test("groupBy with partition information") {
    assertPlan(
      dfpartitioned.load().groupBy($"id").count(),
      { case e: Exchange => e },
      expected = false
    )
  }

  test("window function without partition information") {
    val df = this.df.load().select($"id", $"time", sum($"value").over(window))
    assertPlan(df, { case e: Exchange => e }, expected = true)
    assertPlan(df, { case s: SortExec => s }, expected = true)
  }

  test("window function with partition information") {
    val df = this.dfpartitioned.load().select($"id", $"time", sum($"value").over(window))
    assertPlan(df, { case e: Exchange => e }, expected = false)
    assertPlan(df, { case s: SortExec => s }, expected = true)
  }

  test("window function with partition and order information") {
    assertPlan(
      dfpartitionedAndSorted.load().select($"id", $"time", sum($"value").over(window)),
      { case e: Exchange => e; case s: SortExec => s },
      expected = !DefaultSource.supportsReportingOrder
    )
  }

  def assertPlan[T](df: DataFrame, func: PartialFunction[SparkPlan, T], expected: Boolean): Unit = {
    df.explain()
    assert(df.rdd.getNumPartitions === DefaultSource.partitions)
    assert(collectFirst(df.queryExecution.executedPlan)(func).isDefined === expected)
  }
}
