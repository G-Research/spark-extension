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

  def get: Row = row.get

  def option: Option[Row] = row

  def waitAndGet: Row = {
    waitCompleted()
    row.get
  }

  def waitCompleted(time: Option[Long] = None, unit: TimeUnit = TimeUnit.SECONDS): Boolean = {
    lock.lock()
    try {
      if (row.isEmpty)
        if (time.isDefined) {
          completed.await(time.get, unit)
        } else {
          completed.await()
        }
      row.isDefined
    } finally {
      lock.unlock()
    }
  }

  def waitCompleted(time: Long, unit: TimeUnit): Boolean = waitCompleted(Some(time), unit)

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    onFailureOrSuccess(funcName, qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    onFailureOrSuccess(funcName, qe)

  def onFailureOrSuccess(funcName: String, qe: QueryExecution): Unit = {
    lock.lock()
    try {
      this.row = getMetricRow(qe.observedMetrics)
      completed.signalAll()
    } finally {
      lock.unlock()
    }
  }

  private def getMetricRow(metrics: Map[String, Row]): Option[Row] =
    metrics
      .find { case (metricName, _) => metricName.equals(name) }
      .map { case (_, row) => row }

  private[spark] def register(spark: SparkSession): Unit = {
    spark.listenerManager.unregister(this)
    spark.listenerManager.register(this)
  }

}
