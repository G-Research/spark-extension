package uk.co.gresearch.spark

import org.apache.spark.sql.DataFrame

/**
 * Handle to call `DataFrame.unpersist` on a `DataFrame` that is not known to the caller. The [[RowNumbers.of]]
 * constructs a `DataFrame` that is based ony an intermediate cached `DataFrame`, for witch `unpersist` must be called.
 * A provided [[UnpersistHandle]] allows to do that in user code.
 */
class UnpersistHandle {
  var df: Option[DataFrame] = None

  private[spark] def setDataFrame(dataframe: DataFrame): Unit = {
    if (df.isDefined) throw new IllegalStateException("DataFrame has been set already. It cannot be reused once used with withRowNumbers.")
    this.df = Some(dataframe)
  }

  def apply(): Unit = {
    this.df.getOrElse(throw new IllegalStateException("DataFrame has to be set first")).unpersist()
  }

  def apply(blocking: Boolean): Unit = {
    this.df.getOrElse(throw new IllegalStateException("DataFrame has to be set first")).unpersist(blocking)
  }
}

class NoopUnpersistHandle extends UnpersistHandle{
  override def setDataFrame(dataframe: DataFrame): Unit = {}
  override def apply(): Unit = {}
  override def apply(blocking: Boolean): Unit = {}
}

object UnpersistHandle {
  def apply(): UnpersistHandle = new UnpersistHandle()
  val Noop = new NoopUnpersistHandle()
}
