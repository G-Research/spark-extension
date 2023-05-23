/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Descending, SortOrder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.{DISK_ONLY, MEMORY_AND_DISK, MEMORY_ONLY, NONE}
import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.ExtendedAny
import uk.co.gresearch.spark.SparkSuite.Value

import java.sql.Timestamp
import java.time.Instant

class SparkSuite extends AnyFunSuite with SparkTestSession {

  import spark.implicits._

  val emptyDataset: Dataset[Value] = spark.emptyDataset[Value]
  val emptyDataFrame: DataFrame = spark.createDataFrame(Seq.empty[Value])

  test("Get Spark version") {
    assert(VersionString.contains(s"-$BuildSparkCompatVersionString-") || VersionString.endsWith(s"-$BuildSparkCompatVersionString"))

    assert(spark.version.startsWith(s"$BuildSparkCompatVersionString."))
    assert(SparkVersion === BuildSparkVersion)
    assert(SparkCompatVersion === BuildSparkCompatVersion)
    assert(SparkCompatVersionString === BuildSparkCompatVersionString)
    assert(SparkMajorVersion === BuildSparkMajorVersion)
    assert(SparkMinorVersion === BuildSparkMinorVersion)
    assert(SparkPatchVersion === BuildSparkPatchVersion)
  }

  Seq(MEMORY_AND_DISK, MEMORY_ONLY, DISK_ONLY, NONE).foreach { level =>
    Seq(
      ("UnpersistHandle", UnpersistHandle()),
      ("SilentUnpersistHandle", SilentUnpersistHandle())
    ).foreach { case (handleClass, unpersist) =>
      test(s"$handleClass does unpersist set DataFrame with $level") {
        val cacheManager = spark.sharedState.cacheManager
        cacheManager.clearCache()
        assert(cacheManager.isEmpty === true)

        val df = spark.emptyDataFrame
        assert(cacheManager.lookupCachedData(spark.emptyDataFrame).isDefined === false)

        unpersist.setDataFrame(df)
        assert(cacheManager.lookupCachedData(spark.emptyDataFrame).isDefined === false)

        df.cache()
        assert(cacheManager.lookupCachedData(spark.emptyDataFrame).isDefined === true)

        unpersist(blocking = true)
        assert(cacheManager.lookupCachedData(spark.emptyDataFrame).isDefined === false)

        // calling this twice does not throw any errors
        unpersist()
      }
    }
  }

  Seq(MEMORY_AND_DISK, MEMORY_ONLY, DISK_ONLY, NONE).foreach { level =>
    test(s"NoopUnpersistHandle does not unpersist set DataFrame with $level") {
      val cacheManager = spark.sharedState.cacheManager
      cacheManager.clearCache()
      assert(cacheManager.isEmpty === true)

      val df = spark.emptyDataFrame
      assert(cacheManager.lookupCachedData(spark.emptyDataFrame).isDefined === false)

      val unpersist = UnpersistHandle.Noop
      unpersist.setDataFrame(df)
      assert(cacheManager.lookupCachedData(spark.emptyDataFrame).isDefined === false)

      df.cache()
      assert(cacheManager.lookupCachedData(spark.emptyDataFrame).isDefined === true)

      unpersist(blocking = true)
      assert(cacheManager.lookupCachedData(spark.emptyDataFrame).isDefined === true)

      // calling this twice does not throw any errors
      unpersist()
    }
  }

  Seq(
    ("UnpersistHandle", UnpersistHandle()),
    ("SilentUnpersistHandle", SilentUnpersistHandle())
  ).foreach { case (handleClass, unpersist) =>
    test(s"$handleClass throws on setting DataFrame twice") {
      unpersist.setDataFrame(spark.emptyDataFrame)
      assert(intercept[IllegalStateException] {
        unpersist.setDataFrame(spark.emptyDataFrame)
      }.getMessage === s"DataFrame has been set already, it cannot be reused.")
    }
  }

  test("UnpersistHandle throws on unpersist if no DataFrame is set") {
    val unpersist = UnpersistHandle()
    assert(intercept[IllegalStateException] { unpersist() }.getMessage === s"DataFrame has to be set first")
  }

  test("UnpersistHandle throws on unpersist with blocking if no DataFrame is set") {
    val unpersist = UnpersistHandle()
    assert(intercept[IllegalStateException] { unpersist(blocking = true) }.getMessage === s"DataFrame has to be set first")
  }

  test("SilentUnpersistHandle does not throw on unpersist if no DataFrame is set") {
    val unpersist = SilentUnpersistHandle()
    unpersist()
  }

  test("SilentUnpersistHandle does not throw on unpersist with blocking if no DataFrame is set") {
    val unpersist = SilentUnpersistHandle()
    unpersist(blocking = true)
  }

  test("backticks") {
    assert(backticks("column") === "column")
    assert(backticks("a.column") === "`a.column`")
    assert(backticks("`a.column`") === "`a.column`")
    assert(backticks("column", "a.field") === "column.`a.field`")
    assert(backticks("a.column", "a.field") === "`a.column`.`a.field`")
    assert(backticks("the.alias", "a.column", "a.field") === "`the.alias`.`a.column`.`a.field`")
  }

  def assertIsDataset[T](actual: Dataset[T]): Unit = {
    // if calling class compiles, we assert success
    // further we evaluate the dataset to see this works as well
    actual.collect()
  }

  def assertIsGenericType[T](actual: T): Unit = {
    // if calling class compiles, we assert success
  }

  test("call dataset-to-dataset transformation") {
    assertIsDataset[Value](spark.emptyDataset[Value].transform(_.sort()))
    assertIsDataset[Value](spark.emptyDataset[Value].call(_.sort()))
  }

  test("call dataset-to-dataframe transformation") {
    assertIsDataset[Row](spark.emptyDataset[Value].transform(_.drop("string")))
    assertIsDataset[Row](spark.emptyDataset[Value].call(_.drop("string")))
  }

  test("call dataframe-to-dataset transformation") {
    assertIsDataset[Value](spark.createDataFrame(Seq.empty[Value]).transform(_.as[Value]))
    assertIsDataset[Value](spark.createDataFrame(Seq.empty[Value]).call(_.as[Value]))
  }

  test("call dataframe-to-dataframe transformation") {
    assertIsDataset[Row](spark.createDataFrame(Seq.empty[Value]).transform(_.drop("string")))
    assertIsDataset[Value](spark.createDataFrame(Seq.empty[Value]).call(_.as[Value]))
  }


  Seq(true, false).foreach { condition =>
    test(s"call on $condition condition dataset-to-dataset transformation") {
      assertIsGenericType[Dataset[Value]](
        emptyDataset.transform(_.on(condition).call(_.sort()))
      )
      assertIsGenericType[Dataset[Value]](
        emptyDataset.on(condition).call(_.sort())
      )
    }

    test(s"call on $condition condition dataframe-to-dataframe transformation") {
      assertIsGenericType[DataFrame](
        emptyDataFrame.transform(_.on(condition).call(_.drop("string")))
      )
      assertIsGenericType[DataFrame](
        emptyDataFrame.on(condition).call(_.drop("string"))
      )
    }

    test(s"when $condition call dataset-to-dataset transformation") {
      assertIsDataset[Value](
        emptyDataset.transform(_.when(condition).call(_.sort()))
      )
      assertIsDataset[Value](
        emptyDataset.when(condition).call(_.sort())
      )
    }

    test(s"when $condition call dataframe-to-dataframe transformation") {
      assertIsDataset[Row](
        emptyDataFrame.transform(_.when(condition).call(_.drop("string")))
      )
      assertIsDataset[Row](
        emptyDataFrame.when(condition).call(_.drop("string"))
      )
    }


    test(s"call on $condition condition either dataset-to-dataset transformation") {
      assertIsGenericType[Dataset[Value]](
        spark.emptyDataset[Value]
          .transform(
            _.on(condition)
              .either(_.sort())
              .or(_.orderBy())
          )
      )
    }

    test(s"call on $condition condition either dataset-to-dataframe transformation") {
      assertIsGenericType[DataFrame](
        spark.emptyDataset[Value]
          .transform(
            _.on(condition)
              .either(_.drop("string"))
              .or(_.withColumnRenamed("string", "value"))
          )
      )
    }

    test(s"call on $condition condition either dataframe-to-dataset transformation") {
      assertIsGenericType[Dataset[Value]](
        spark.createDataFrame(Seq.empty[Value])
          .transform(
            _.on(condition)
              .either(_.as[Value])
              .or(_.as[Value])
          )
      )
    }

    test(s"call on $condition condition either dataframe-to-dataframe transformation") {
      assertIsGenericType[DataFrame](
        spark.createDataFrame(Seq.empty[Value])
          .transform(
            _.on(condition)
              .either(_.drop("string"))
              .or(_.withColumnRenamed("string", "value"))
          )
      )
    }
  }


  test("on true condition call either writer-to-writer methods") {
    assertIsGenericType[DataFrameWriter[Value]](
      spark
        .emptyDataset[Value]
        .write
        .on(true)
        .either(_.partitionBy("id"))
        .or(_.bucketBy(10, "id"))
        .mode(SaveMode.Overwrite)
    )
  }

  test("on false condition call either writer-to-writer methods") {
    assertIsGenericType[DataFrameWriter[Value]](
      spark
        .emptyDataset[Value]
        .write
        .on(false)
        .either(_.partitionBy("id"))
        .or(_.bucketBy(10, "id"))
        .mode(SaveMode.Overwrite)
    )
  }

  test("on true condition call either writer-to-unit methods") {
    withTempPath { dir =>
      assertIsGenericType[Unit](
        spark
          .emptyDataset[Value]
          .write
          .on(true)
          .either(_.csv(dir.getAbsolutePath))
          .or(_.csv(dir.getAbsolutePath))
      )
    }
  }

  test("on false condition call either writer-to-unit methods") {
    withTempPath { dir =>
      assertIsGenericType[Unit](
        spark
          .emptyDataset[Value]
          .write
          .on(false)
          .either(_.csv(dir.getAbsolutePath))
          .or(_.csv(dir.getAbsolutePath))
      )
    }
  }

  test("global row number preserves order") {
    doTestWithRowNumbers()(){ df =>
      assert(df.columns === Seq("id", "rand", "row_number"))
    }
  }

  test("global row number respects order") {
    doTestWithRowNumbers { df => df.repartition(100) }($"id")()
  }

  test("global row number supports multiple order columns") {
    doTestWithRowNumbers { df => df.repartition(100) }($"id", $"rand", rand())()
  }

  test("global row number allows desc order") {
    doTestWithRowNumbers { df => df.repartition(100) }($"id".desc)()
  }

  Seq(MEMORY_AND_DISK, MEMORY_ONLY, DISK_ONLY, NONE).foreach { level =>
    test(s"global row number with $level") {
      doTestWithRowNumbers(storageLevel = level)($"id")()
    }
  }

  Seq(MEMORY_AND_DISK, MEMORY_ONLY, DISK_ONLY, NONE).foreach { level =>
    test(s"global row number allows to unpersist with $level") {
      val cacheManager = spark.sharedState.cacheManager
      cacheManager.clearCache()
      assert(cacheManager.isEmpty === true)

      val unpersist = UnpersistHandle()
      doTestWithRowNumbers(storageLevel = level, unpersistHandle = unpersist)($"id")()
      assert(cacheManager.isEmpty === false)
      unpersist(true)
      assert(cacheManager.isEmpty === true)
    }
  }

  test("global row number with existing row_number column") {
    // this overwrites the existing column 'row_number' (formerly 'rand') with the row numbers
    doTestWithRowNumbers { df => df.withColumnRenamed("rand", "row_number") }(){ df =>
      assert(df.columns === Seq("id", "row_number"))
    }
  }

  test("global row number with custom row_number column") {
    // this puts the row numbers in the column "row", which is not the default column name
    doTestWithRowNumbers(df => df.withColumnRenamed("rand", "row_number"),
      rowNumberColumnName = "row" )(){ df =>
      assert(df.columns === Seq("id", "row_number", "row"))
    }
  }

  test("global row number with internal column names") {
    val cols = Seq("mono_id", "partition_id", "local_row_number", "max_local_row_number",
      "cum_row_numbers", "partition_offset")
    var prefix: String = null

    doTestWithRowNumbers { df =>
      prefix = distinctPrefixFor(df.columns)
      cols.foldLeft(df){ (df, name) => df.withColumn(prefix + name, rand()) }
    }(){ df =>
      assert(df.columns === Seq("id", "rand") ++ cols.map(prefix + _) :+ "row_number")
    }
  }

  def doTestWithRowNumbers(transform: DataFrame => DataFrame = identity,
                           rowNumberColumnName: String = "row_number",
                           storageLevel: StorageLevel = MEMORY_AND_DISK,
                           unpersistHandle: UnpersistHandle = UnpersistHandle.Noop)
                          (columns: Column*)
                          (handle: DataFrame => Unit = identity[DataFrame]): Unit = {
    val partitions = 10
    val rowsPerPartition = 1000
    val rows = partitions * rowsPerPartition
    assert(partitions > 1)
    assert(rowsPerPartition > 1)

    val df = spark.range(1, rows + 1, 1, partitions)
      .withColumn("rand", rand())
      .transform(transform)
      .withRowNumbers(
        rowNumberColumnName=rowNumberColumnName,
        storageLevel=storageLevel,
        unpersistHandle=unpersistHandle,
        columns: _*)
      .cache()

    try {
      // testing with descending order is only supported for a single column
      val desc = columns.map(_.expr) match {
        case Seq(SortOrder(_, Descending, _, _)) => true
        case _ => false
      }

      // assert row numbers are correct
      assertRowNumbers(df, rows, desc, rowNumberColumnName)
      handle(df)
    } finally {
      // always unpersist
      df.unpersist(true)
    }
  }

  def assertRowNumbers(df: DataFrame, rows: Int, desc: Boolean, rowNumberColumnName: String): Unit = {
    val expect = if (desc) {
      $"id" === (lit(rows) - col(rowNumberColumnName) + 1)
    } else {
      $"id" === col(rowNumberColumnName)
    }

    val correctRowNumbers = df.where(expect).count()
    val incorrectRowNumbers = df.where(! expect).count()
    assert(correctRowNumbers === rows)
    assert(incorrectRowNumbers === 0)
  }

  test(".Net ticks to Spark timestamp / unix epoch seconds / nanoseconds") {
    val df = Seq(
      (1, 599266080000000000L),
      (2, 621355968000000000L),
      (3, 638155413748959308L),
      (4, 638155413748959309L),
      (5, 638155413748959310L),
      // results in largest possible unix epoch nanos
      (6, 713589688368547758L),
      (7, 3155378975999999999L)
    ).toDF("id", "ts")

    val plan = df.select(
      $"id",
      dotNetTicksToTimestamp($"ts"),
      dotNetTicksToTimestamp("ts"),
      dotNetTicksToUnixEpoch($"ts"),
      dotNetTicksToUnixEpoch("ts"),
      dotNetTicksToUnixEpochNanos($"ts"),
      dotNetTicksToUnixEpochNanos("ts")
    ).orderBy($"id")
    assert(plan.schema.fields.map(_.dataType) === Seq(
      IntegerType, TimestampType, TimestampType, DecimalType(29, 9), DecimalType(29, 9), LongType, LongType
    ))

    val actual = plan.collect()

    assert(actual.map(_.getTimestamp(1)) === Seq(
      Timestamp.from(Instant.parse("1900-01-01T00:00:00Z")),
      Timestamp.from(Instant.parse("1970-01-01T00:00:00Z")),
      Timestamp.from(Instant.parse("2023-03-27T19:16:14.89593Z")),
      Timestamp.from(Instant.parse("2023-03-27T19:16:14.89593Z")),
      Timestamp.from(Instant.parse("2023-03-27T19:16:14.895931Z")),
      // largest possible unix epoch nanos
      Timestamp.from(Instant.parse("2262-04-11T23:47:16.854775Z")),
      Timestamp.from(Instant.parse("9999-12-31T23:59:59.999999Z")),
    ))
    assert(actual.map(_.getTimestamp(2)) === actual.map(_.getTimestamp(1)))

    assert(actual.map(_.getDecimal(3)).map(BigDecimal(_)) === Array(
      BigDecimal(-2208988800000000000L, 9),
      BigDecimal(0, 9),
      BigDecimal(1679944574895930800L, 9),
      BigDecimal(1679944574895930900L, 9),
      BigDecimal(1679944574895931000L, 9),
      // largest possible unix epoch nanos
      BigDecimal(9223372036854775800L, 9),
      BigDecimal(2534023007999999999L, 7).setScale(9),
    ))
    assert(actual.map(_.getDecimal(4)) === actual.map(_.getDecimal(3)))

    assert(actual.map(row =>
      if (BigDecimal(row.getDecimal(3)) <= BigDecimal(9223372036854775800L, 9)) row.getLong(5) else null
    ) === actual.map(row =>
      if (BigDecimal(row.getDecimal(3)) <= BigDecimal(9223372036854775800L, 9)) row.getDecimal(3).multiply(new java.math.BigDecimal(1000000000)).longValue() else null
    ))
    assert(actual.map(_.get(6)) === actual.map(_.get(5)))
  }

  test("Spark timestamp to .Net ticks") {
    val df = Seq(
      (1, Timestamp.from(Instant.parse("1900-01-01T00:00:00Z"))),
      (2, Timestamp.from(Instant.parse("1970-01-01T00:00:00Z"))),
      (3, Timestamp.from(Instant.parse("2023-03-27T19:16:14.895931Z"))),
      (4, Timestamp.from(Instant.parse("9999-12-31T23:59:59.999999Z"))),
    ).toDF("id", "ts")

    if (Some(spark.sparkContext.version).exists(_.startsWith("3.0."))) {
      assertThrows[NotImplementedError] {
        df.select(timestampToDotNetTicks($"ts"))
      }
    } else {
      val plan = df.select(
        $"id",
        timestampToDotNetTicks($"ts"),
        timestampToDotNetTicks("ts"),
      ).orderBy($"id")

      assert(plan.schema.fields.map(_.dataType) === Seq(
        IntegerType, LongType, LongType
      ))

      val actual = plan.collect()

      assert(actual.map(_.getLong(1)) === Seq(
        599266080000000000L,
        621355968000000000L,
        638155413748959310L,
        3155378975999999990L
      ))
      assert(actual.map(_.getLong(2)) === actual.map(_.getLong(1)))

      val message = intercept[AnalysisException] {
        Seq(1L).toDF("ts").select(timestampToDotNetTicks($"ts")).collect()
      }.getMessage
      if (Some(spark.sparkContext.version).exists(_.startsWith("3.1."))) {
        assert(message.startsWith("cannot resolve 'unix_micros(`ts`)' due to data type mismatch: argument 1 requires timestamp type, however, '`ts`' is of bigint type.;"))
      } else if (Some(spark.sparkContext.version).exists(_.startsWith("3.4."))) {
        assert(message.startsWith("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"unix_micros(ts)\" due to data type mismatch: Parameter 1 requires the \"TIMESTAMP\" type, however \"ts\" has the type \"BIGINT\".;"))
      } else {
        assert(message.startsWith("cannot resolve 'unix_micros(ts)' due to data type mismatch: argument 1 requires timestamp type, however, 'ts' is of bigint type.;"))
      }
    }
  }

  test("Unix epoch to .Net ticks") {
    def df[T : Encoder](v: T): DataFrame =
      spark.createDataset(Seq(v)).withColumnRenamed("value", "ts")

    Seq(
      df(BigDecimal(1679944574895931234L, 9)),
      df("1679944574.895931234"),
      df(1679944574.895931234),
      df(1679944574L),
      df(1679944574),
    ).foreach { df =>
      this.withClue(df.schema.fields.head.dataType) {
        val plan = df.select(
          unixEpochToDotNetTicks($"ts"),
          unixEpochToDotNetTicks("ts")
        )
        assert(plan.schema.fields.map(_.dataType) === Seq(LongType, LongType))

        val actual = plan.collect()

        assert(actual.length === 1)
        assert(actual.head.isNullAt(0) === false)
        assert(actual.head.isNullAt(1) === false)

        if (Set(IntegerType, LongType).map(_.asInstanceOf[DataType]).contains(df.schema.head.dataType)) {
          // long and integer also works, but without sub-second precision
          assert(actual.head.getLong(0) === 638155413740000000L)
        } else {
          // lowest two nanosecond digits get lost
          assert(actual.head.getLong(0) === 638155413748959312L)
        }
        assert(actual.head.getLong(1) === actual.head.getLong(0))
      }
    }
  }

  test("Unix epoch nanos to .Net ticks") {
    def df[T : Encoder](v: T): DataFrame =
      spark.createDataset(Seq(v)).withColumnRenamed("value", "ts")

    Seq(
      df(BigDecimal(1679944574895931234L)),
      df("1679944574895931234"),
      df(1679944574895931234L),
      df(1679944574895931234.5),
    ).foreach { df =>
      this.withClue(df.schema.fields.head.dataType) {
        val plan = df.select(
          unixEpochNanosToDotNetTicks($"ts"),
          unixEpochNanosToDotNetTicks("ts")
        )
        assert(plan.schema.fields.map(_.dataType) === Seq(LongType, LongType))

        val actual = plan.collect()

        assert(actual.length === 1)
        assert(actual.head.isNullAt(0) === false)
        assert(actual.head.isNullAt(1) === false)
        if (df.schema.fields.head.dataType == DoubleType) {
          // The initial double value can represent the epoch nanos only as 1.67994457489593114E18
          assert(actual.head.getLong(0) === 638155413748959311L)
        } else {
          assert(actual.head.getLong(0) === 638155413748959312L)
        }
        assert(actual.head.getLong(1) === actual.head.getLong(0))
      }
    }
  }
}

object SparkSuite {
  case class Value(id: Int, string: String)
}
