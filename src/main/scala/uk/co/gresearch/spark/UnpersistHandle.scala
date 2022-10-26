package uk.co.gresearch.spark

import org.apache.spark.sql.DataFrame

/**
 * Handle to call `DataFrame.unpersist` on a `DataFrame` that is not known to the caller. The [[RowNumbers.of]]
 * constructs a `DataFrame` that is based ony an intermediate cached `DataFrame`, for witch `unpersist` must be called.
 * A provided [[UnpersistHandle]] allows to do that in user code.
 */
class UnpersistHandle {
  var df: Option[DataFrame] = None

  private[spark] def setDataFrame(dataframe: DataFrame): DataFrame = {
    if (df.isDefined) throw new IllegalStateException("DataFrame has been set already, it cannot be reused.")
    this.df = Some(dataframe)
    dataframe
  }

  def apply(): Unit = {
    this.df.getOrElse(throw new IllegalStateException("DataFrame has to be set first")).unpersist()
  }

  def apply(blocking: Boolean): Unit = {
    this.df.getOrElse(throw new IllegalStateException("DataFrame has to be set first")).unpersist(blocking)
  }
}

case class SilentUnpersistHandle() extends UnpersistHandle {
  override def apply(): Unit = {
    this.df.foreach(_.unpersist())
  }

  override def apply(blocking: Boolean): Unit = {
    this.df.foreach(_.unpersist(blocking))
  }
}

case class NoopUnpersistHandle() extends UnpersistHandle{
  override def setDataFrame(dataframe: DataFrame): DataFrame = dataframe
  override def apply(): Unit = {}
  override def apply(blocking: Boolean): Unit = {}
}

object UnpersistHandle {
  val Noop: NoopUnpersistHandle = NoopUnpersistHandle()
  def apply(): UnpersistHandle = new UnpersistHandle()

  def withUnpersist[T](blocking: Boolean = false)(func: UnpersistHandle => T): T = {
    val handle = SilentUnpersistHandle()
    try {
      func(handle)
    } finally {
      handle(blocking)
    }
  }
}
