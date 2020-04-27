package uk.co.gresearch.spark.diff

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FunSuite, MustMatchers}

import scala.collection.JavaConverters._

class DiffSuiteWithComparators extends FunSuite with SparkTestSession with MustMatchers {

  private val floatStruct = StructType(Seq(
    StructField("id", IntegerType, false),
    StructField("float", FloatType, false)
  ))
  private val doubleStruct = StructType(Seq(
    StructField("id", IntegerType, false),
    StructField("float", DoubleType, false)
  ))
  private val timestampStruct = StructType(Seq(
    StructField("id", IntegerType, false),
    StructField("timestamp", TimestampType, false)
  ))
  lazy val leftFloats: DataFrame = spark.createDataFrame(
    List(
    Row(1, 9.9f),
    Row(2, 9.9f),
    Row(3, 9.9f)
    ).asJava, floatStruct
  )

  lazy val rightFloats: DataFrame = spark.createDataFrame(
    List(
      Row(1, 9.8f),
      Row(2, 10.0f),
      Row(3, 11.0f)
    ).asJava, floatStruct
  )

  lazy val leftDouble: DataFrame = spark.createDataFrame(
    List(
      Row(1, 9.9),
      Row(2, 9.9),
      Row(3, 9.9)
    ).asJava, doubleStruct
  )

  lazy val rightDoubles: DataFrame = spark.createDataFrame(
    List(
      Row(1, 9.8),
      Row(2, 10.0),
      Row(3, 11.0)
    ).asJava, doubleStruct
  )

  lazy val expectedDiffColumns: Seq[String] =
    Seq("diff", "id", "left_value", "right_value")

  for(divergence <- Seq(0.1f, 0.09f)) {
    val diffWithOptions = new Diff(DiffOptions(
      diffColumn = "diff",
      leftColumnPrefix = "left",
      rightColumnPrefix = "right",
      insertDiffValue = "Inserted",
      changeDiffValue = "Changed",
      deleteDiffValue = "Deleted",
      nochangeDiffValue = "Equal",
      specialComparators = Map(
        "float" -> DiffComparator.FuzzyNumberComparator(divergence)
      )
    ))

    test("FloatFuzzyComparator with divergence: " + divergence) {
      val expected = Seq(
        Seq(if(divergence >= 0.1) "Equal" else "Changed", 1, 9.9f, 9.8f),
        Seq(if(divergence >= 0.1) "Equal" else "Changed", 2, 9.9f, 10.0f),
        Seq("Changed", 3, 9.9f, 11.0f)
      )

      val actual = diffWithOptions.of(leftFloats, rightFloats, "id").orderBy("id", "diff")

      assert(actual.columns === Seq("diff", "id", "left_float", "right_float"))
      actual.collect().map(row => row.toSeq).zip(expected).foreach(x => x._1 mustBe x._2)
    }
  }

  private val leftTimeStamps = spark.createDataFrame(
    List(
      Row(1, Timestamp.valueOf("2020-01-01 12:12:12.000")),
      Row(2, Timestamp.valueOf("2020-01-01 12:12:12.000")),
      Row(3, Timestamp.valueOf("2020-01-01 12:12:12.000")),
      Row(4, Timestamp.valueOf("2020-01-01 13:12:12.000"))
    ).asJava, timestampStruct
  )

  private val rightTimeStamps = spark.createDataFrame(
    List(
      Row(1, Timestamp.valueOf("2020-01-01 12:12:02.000")),
      Row(2, Timestamp.valueOf("2020-01-01 12:12:22.000")),
      Row(3, Timestamp.valueOf("2020-01-01 12:12:32.000")),
      Row(4, Timestamp.valueOf("2020-01-01 12:12:13.000"))
    ).asJava, timestampStruct
  )

  for(duration <- Seq(10, 20)) {
    val diffWithOptions = new Diff(DiffOptions(
      diffColumn = "diff",
      leftColumnPrefix = "left",
      rightColumnPrefix = "right",
      insertDiffValue = "Inserted",
      changeDiffValue = "Changed",
      deleteDiffValue = "Deleted",
      nochangeDiffValue = "Equal",
      specialComparators = Map(
        "timestamp" -> DiffComparator.FuzzyDateComparator(10, duration)
      )
    ))

    test("FuzzyDateComparator with duration: " + duration) {
      val expected = Seq(
        Seq("Equal", 1, Timestamp.valueOf("2020-01-01 12:12:12.0"), Timestamp.valueOf("2020-01-01 12:12:02.0")),
        Seq("Equal", 2, Timestamp.valueOf("2020-01-01 12:12:12.0"), Timestamp.valueOf("2020-01-01 12:12:22.0")),
        Seq(if(duration >= 20) "Equal" else "Changed", 3, Timestamp.valueOf("2020-01-01 12:12:12.0"), Timestamp.valueOf("2020-01-01 12:12:32.0")),
        Seq("Changed", 4, Timestamp.valueOf("2020-01-01 13:12:12.0"), Timestamp.valueOf("2020-01-01 12:12:13.0"))
      )

      val actual = diffWithOptions.of(leftTimeStamps, rightTimeStamps, "id").orderBy("id", "diff")

      assert(actual.columns === Seq("diff", "id", "left_timestamp", "right_timestamp"))
      actual.collect().map(row => row.toSeq).zip(expected).foreach(x => x._1 mustBe x._2)
    }
  }

  for(tz <- Seq("UTC", "CET")){

    val diffWithTimezones = new Diff(DiffOptions(
      diffColumn = "diff",
      leftColumnPrefix = "left",
      rightColumnPrefix = "right",
      insertDiffValue = "Inserted",
      changeDiffValue = "Changed",
      deleteDiffValue = "Deleted",
      nochangeDiffValue = "Equal",
      specialComparators = Map(
        "timestamp" -> DiffComparator.FuzzyDateComparator(10, 10, tz)
      )
    ))

    test("FuzzyDateComparator with time zone: " + tz) {
      val expected = Seq(
        Seq(if(tz == "UTC") "Equal" else "Changed", 1, Timestamp.valueOf("2020-01-01 12:12:12.0"), Timestamp.valueOf("2020-01-01 12:12:02.0")),
        Seq(if(tz == "UTC") "Equal" else "Changed", 2, Timestamp.valueOf("2020-01-01 12:12:12.0"), Timestamp.valueOf("2020-01-01 12:12:22.0")),
        Seq("Changed", 3, Timestamp.valueOf("2020-01-01 12:12:12.0"), Timestamp.valueOf("2020-01-01 12:12:32.0")),
        Seq(if(tz == "CET") "Equal" else "Changed", 4, Timestamp.valueOf("2020-01-01 13:12:12.0"), Timestamp.valueOf("2020-01-01 12:12:13.0"))
      )

      val actual = diffWithTimezones.of(leftTimeStamps, rightTimeStamps, "id").orderBy("id", "diff")

      assert(actual.columns === Seq("diff", "id", "left_timestamp", "right_timestamp"))
      actual.collect().map(row => row.toSeq).zip(expected).foreach(x => x._1 mustBe x._2)
    }
  }
}
