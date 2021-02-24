package uk.co.gresearch.spark

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{Column, Row, SparkSession}

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}

case class Observation(name: String, expr: Column, exprs: Column*) extends QueryExecutionListener {
  @transient
  var row: Option[Row] = None
  val lock: Lock = new ReentrantLock()
  val completed: Condition = lock.newCondition()

  def option: Option[Row] = row

  def get: Row = row.get

  def waitAndGet: Row = {
    waitCompleted()
    row.get
  }

  private def getMetricRow(metrics: Map[String, Row]): Option[Row] =
    metrics
      .find { case (metricName, _) => metricName.equals(name) }
      .map { case (_, row) => row }

  private[spark] def register(spark: SparkSession): Unit = {
    spark.listenerManager.unregister(this)
    spark.listenerManager.register(this)
  }

  private def setCompleted(): Unit = {
    lock.lock()
    try {
      completed.signalAll()
    } finally {
      lock.unlock()
    }
  }

  def waitCompleted(time: Option[Long] = None, unit: TimeUnit = TimeUnit.SECONDS): Boolean = {
    lock.lock()
    try {
      if (time.isDefined) {
        completed.await(time.get, unit)
      } else {
        completed.await()
        true
      }
    } finally {
      lock.unlock()
    }
  }

  def waitCompleted(time: Long, unit: TimeUnit): Boolean = waitCompleted(Some(time), unit)

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    this.row = getMetricRow(qe.observedMetrics)
    setCompleted()
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    this.row = getMetricRow(qe.observedMetrics)
    setCompleted()
  }

}
