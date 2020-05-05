package uk.co.gresearch.spark.diff

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}

/**
 * Abstract class for a Comparator factory of a given comparison type.
 * @param symbols - list of symbols referencing the given comparison method
 */
abstract class DiffComparator(val symbols: Seq[String]) {
  
  def description: String

  /**
   * The Compare Expression of two other expressions (usually resolved as instance of [[BinaryExpression]]
   * @param left - the left expression of a binary expression (left Column)
   * @param right - the right expression of a binary expression (right Column)
   */
  def compare(left: Column, right: Column): Expression

  /**
   * The same as expression based compare function but based on left Column compared to right Column
   * @param left - left Column
   * @param right - right Column
   */
  def compareColumns(left: Column, right: Column): Column = new Column(compare(left, right))
}

object DiffComparator{

  case class FuzzyNumberComparator(divergence: Double, datatype: DataType = FloatType) extends DiffComparator(Seq("~~")) {
    // we add a bit more divergence to compensate oscillating differences at the end of the precision range
    private val significantDigitAddon = datatype match{
      case FloatType => 0.000001              // floats has 6 significant digits
      case DoubleType => 0.0000000000000001   // double has 16 significant digits
      case _ => 0                             // otherwise integer numbers...
    }
    assert(divergence >= significantDigitAddon, "The divergence parameter hast to be greater or equal to " + significantDigitAddon + " for datatype " + datatype)
    override def description: String = "Compares two numbers and considers them equal as long as their difference is below the given divergence."
    override def compare(left: Column, right: Column): Expression = {
      Or(And(IsNull(left.expr), IsNull(right.expr)),
      LessThanOrEqual(abs(left - right).expr, lit(divergence + significantDigitAddon).expr))
    }
  }

  /**
   * Fuzzy timestamp comparator allowing for a grace duration before and after the right column values
   * @param leftBeforeRight - number of seconds before the right timestamp in which the left timestamp may occur to be considered equal
   * @param leftAfterRight - number of seconds after the right timestamp in which the left timestamp may occur to be considered equal
   * @param leftTimeZone - the time zone of the left column (by default: UTC)
   * @param rightTimeZone - the time zone of the left column (by default: UTC)
   */
  case class FuzzyDateComparator(
    leftBeforeRight: Int,
    leftAfterRight: Int,
    leftTimeZone: String = "UTC",
    rightTimeZone: String = "UTC"
  ) extends DiffComparator(Seq[String]()){
    assert(leftBeforeRight >= 0 && leftAfterRight >= 0, "Durations below one second are not allowed for FuzzyDateComparator.")
    override def description: String = "For comparing two timestamps allowing for a given diversion before and after "

    private val defaultUnixFormat = Literal("yyyy-MM-dd HH:mm:ss") // is actually not used
    private val leftTz = Option(getTimeZone(leftTimeZone)).getOrElse(throw new IllegalArgumentException("Invalid time zone: " + leftTimeZone))
    private val rightTz = Option(getTimeZone(rightTimeZone)).getOrElse(throw new IllegalArgumentException("Invalid time zone: " + rightTimeZone))

    /**
     * The Compare Expression of two other expressions (usually resolved as instance of [[BinaryExpression]]
     *
     * @param left  - the left expression of a binary expression (left Column)
     * @param right - the right expression of a binary expression (right Column)
     */
    override def compare(left: Column, right: Column): Expression = {
      val leftTimestamp = to_utc_timestamp(left, leftTz.getID)
      val rightTimestamp = to_utc_timestamp(right, rightTz.getID)
      val diffInSeconds = new Column(Subtract(
        UnixTimestamp(leftTimestamp.expr, defaultUnixFormat, Some(leftTz.getID)),
        UnixTimestamp(rightTimestamp.expr, defaultUnixFormat, Some(rightTz.getID))
      ))

      ( leftTimestamp.isNull && rightTimestamp.isNull ||
        diffInSeconds > lit(0) && diffInSeconds <= lit(leftBeforeRight) ||
        diffInSeconds <= lit(0) && abs(diffInSeconds) <= lit(leftAfterRight)).expr
    }
}

  // this is the default comparator
  val EqualNullSafeComparator: DiffComparator = new DiffComparator(Seq("<=>", "eqNullSafe")){
      override def compare(left: Column, right: Column): Expression = EqualNullSafe(left.expr, right.expr)
      override def description: String = "equality comparator which is null safe"
    }
  val EqualToComparator: DiffComparator = new DiffComparator(Seq("=", "===", "eq", "equalTo")){
      override def compare(left: Column, right: Column): Expression = EqualTo(left.expr, right.expr)
      override def description: String = "equals comparator"
    }
  val NotEqualToComparator: DiffComparator = new DiffComparator(Seq("=!=", "!==", "notEqual")){
      override def compare(left: Column, right: Column): Expression = Not(EqualTo(left.expr, right.expr))
      override def description: String = "not equals comparator"
    }
  val GreaterThanComparator: DiffComparator = new DiffComparator(Seq(">", "gt")){
      override def compare(left: Column, right: Column): Expression = GreaterThan(left.expr, right.expr)
      override def description: String = "greater than comparator"
    }
  val LessThanComparator: DiffComparator = new DiffComparator(Seq("<", "lt")){
      override def compare(left: Column, right: Column): Expression = LessThan(left.expr, right.expr)
      override def description: String = "less than comparator"
    }
  val GreaterThanOrEqualComparator: DiffComparator = new DiffComparator(Seq(">=", "geq")){
      override def compare(left: Column, right: Column): Expression = GreaterThanOrEqual(left.expr, right.expr)
      override def description: String = "greater than or equals comparator"
    }
  val LessThanOrEqualComparator: DiffComparator = new DiffComparator(Seq("<=", "leq")){
      override def compare(left: Column, right: Column): Expression = LessThanOrEqual(left.expr, right.expr)
      override def description: String = "less than or equals comparator"
    }
}
