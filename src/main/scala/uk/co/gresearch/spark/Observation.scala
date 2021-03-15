package uk.co.gresearch.spark

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}

/**
 * Observation helper class to simplify calling Dataset.observe().
 *
 * This class is not thread-safe. Use it in a single thread or guard access via a lock.
 *
 * Call `ds.observe(observation)` or `observation.observe(ds)` to use the observation.
 * Stop the observation by calling `observation.close()`.
 *
 * @param name observation name
 * @param expr metric expression
 * @param exprs metric expressions
 */
case class Observation(name: String, expr: Column, exprs: Column*) {

  val listener: ObservationListener = ObservationListener(this)
  var spark: Option[SparkSession] = None
  @transient var row: Option[Row] = None
  val lock: Lock = new ReentrantLock()
  val completed: Condition = lock.newCondition()

  /**
   * Attach this observation to the given Dataset.
   * Remember to call `close()` when the observation is done.
   *
   * @param ds dataset
   * @tparam T dataset type
   * @return observed dataset
   */
  def observe[T](ds: Dataset[T]): Dataset[T] = {
    spark = Some(ds.sparkSession)
    Observation.register(this, spark.get)
    ds.observe(name, expr, exprs: _*)
  }

  /**
   * Get the observation results. Waits for the first action on the observed dataset to complete.
   * After calling `reset()`, waits for completion of the next action on the observed dataset.
   */
  def get: Row = get(None)

  /**
   * Get the observation results. Waits for the first action on the observed dataset to complete.
   * After calling `reset()`, waits for completion of the next action on the observed dataset.
   */
  def option: Option[Row] = option(None)

  /**
   * Wait for the first action on the observed dataset to complete.
   * Without giving the time parameter, this method does not timeout waiting.
   * After calling `reset()`, waits for completion of the next action on the observed dataset.
   *
   * @param time timeout
   * @param unit timeout time unit
   * @return true if action complete within timeout, false on timeout
   */
  def waitCompleted(time: Option[Long] = None, unit: TimeUnit = TimeUnit.MILLISECONDS): Boolean = {
    lock.lock()
    try {
      if (row.isEmpty) {
        if (time.isDefined) {
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

  /**
   * Get the observation results. Waits for the first action on the observed dataset to complete.
   * After calling `reset()`, waits for completion of the next action on the observed dataset.
   *
   * @param time timeout
   * @param unit timeout time unit
   * @return observation row
   */
  def get(time: Long, unit: TimeUnit): Row = get(Some(time), unit)

  /**
   * Get the observation results. Waits for the first action on the observed dataset to complete.
   * After calling `reset()`, waits for completion of the next action on the observed dataset.
   *
   * @param time timeout
   * @param unit timeout time unit
   * @return observation row as an Option, or None on timeout
   */
  def option(time: Long, unit: TimeUnit): Option[Row] = option(Some(time), unit)

  /**
   * Wait for the first action on the observed dataset to complete.
   * After calling `reset()`, waits for completion of the next action on the observed dataset.
   *
   * @param time timeout
   * @param unit timeout time unit
   * @return true if action complete within timeout, false on timeout
   */
  def waitCompleted(time: Long, unit: TimeUnit): Boolean = waitCompleted(Some(time), unit)

  /**
   * Reset the observation. This deletes the observation and allows to wait for completion
   * of the next action called on the observed dataset.
   */
  def reset(): Unit = {
    lock.lock()
    try {
      row = None
    } finally {
      lock.unlock()
    }
  }

  /**
   * Terminates the observation. Subsequent calls to actions on the observed dataset
   * will not update the observation. The current observation persists after calling this method.
   */
  def close(): Unit = {
    if (spark.isDefined) Observation.unregister(this, spark.get)
  }

  private def get(time: Option[Long] = None, unit: TimeUnit = TimeUnit.MILLISECONDS): Row = {
    waitCompleted(time, unit)
    if (row.isEmpty) throw new NoSuchElementException("call an action on the observed dataset first")
    row.get
  }

  private def option(time: Option[Long] = None, unit: TimeUnit = TimeUnit.MILLISECONDS): Option[Row] = {
    waitCompleted(time, unit)
    row
  }

  private[spark] def onFinish(funcName: String, qe: QueryExecution): Unit = {
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

private[spark] case class ObservationListener(observation: Observation) extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    observation.onFinish(funcName, qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    observation.onFinish(funcName, qe)

}

object Observation {

  /**
   * Observation helper class to simplify calling Dataset.observe().
   *
   * This class is not thread-safe. Use it in a single thread or guard access via a lock.
   *
   * @param expr metric expression
   * @param exprs metric expressions
   */
  def apply(expr: Column, exprs: Column*): Observation =
    new Observation(UUID.randomUUID().toString, expr, exprs: _*)

  private def register(observation: Observation, spark: SparkSession): Unit = {
    this.unregister(observation, spark)
    spark.listenerManager.register(observation.listener)
  }

  private def unregister(observation: Observation, spark: SparkSession): Unit =
    spark.listenerManager.unregister(observation.listener)

}
