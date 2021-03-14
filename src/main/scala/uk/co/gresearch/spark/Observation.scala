package uk.co.gresearch.spark

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}

/**
 * Observation helper class to simplify calling Dataset.observe().
 * This class is not thread-safe. Use it in a single thread or guard access via a lock.
 *
 * @param name observation name
 * @param expr metric expression
 * @param exprs metric expressions
 */
case class Observation(name: String, expr: Column, exprs: Column*) extends QueryExecutionListener {
  @transient
  var row: Option[Row] = None
  val lock: Lock = new ReentrantLock()
  val completed: Condition = lock.newCondition()

  def observe[T](ds: Dataset[T]): Dataset[T] = {
    register(ds.sparkSession)
    ds.observe(name, expr, exprs: _*)
  }

  def get: Row = row.get

  def option: Option[Row] = row

  def waitAndGet: Row = {
    waitCompleted()
    row.get
  }

  def waitCompleted(time: Option[Long] = None, unit: TimeUnit = TimeUnit.SECONDS): Boolean = {
    lock.lock()
    try {
      if (row.isEmpty) {
        if (time.isDefined) {
          println(s"waiting for signal: ${time.get}")
          completed.await(time.get, unit)
        } else {
          completed.await()
        }
      }
      row.isDefined
    } finally {
      lock.unlock()
    }
  }

  def waitCompleted(time: Long, unit: TimeUnit): Boolean = waitCompleted(Some(time), unit)

  def reset(): Unit = {
    lock.lock()
    try {
      row = None
    } finally {
      lock.unlock()
    }
  }

  private def register(spark: SparkSession): Unit = {
    spark.listenerManager.unregister(this)
    spark.listenerManager.register(this)
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    println(s"success: $funcName $name")
    onFinish(funcName, qe)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    println(s"failure: $funcName $name")
    onFinish(funcName, qe)
  }

  def onFinish(funcName: String, qe: QueryExecution): Unit = {
    lock.lock()
    try {
      this.row = getMetricRow(qe.observedMetrics)
      if (this.row.isDefined) completed.signalAll()
    } finally {
      lock.unlock()
    }
  }

  private def getMetricRow(metrics: Map[String, Row]): Option[Row] =
    metrics
      .find { case (metricName, _) => metricName.equals(name) }
      .map { case (_, row) => row }

}
