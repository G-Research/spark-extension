/*
 * Copyright 2023 G-Research
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

package uk.co.gresearch.spark.diff

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scopt.OptionParser
import uk.co.gresearch._

object App {
  // define available options
  case class Options(master: Option[String] = None,
                     appName: Option[String] = None,
                     hive: Boolean = false,

                     leftPath: Option[String] = None,
                     rightPath: Option[String] = None,
                     outputPath: Option[String] = None,

                     leftFormat: Option[String] = None,
                     rightFormat: Option[String] = None,
                     outputFormat: Option[String] = None,

                     leftSchema: Option[String] = None,
                     rightSchema: Option[String] = None,

                     leftOptions: Map[String, String] = Map.empty,
                     rightOptions: Map[String, String] = Map.empty,
                     outputOptions: Map[String, String] = Map.empty,

                     ids: Seq[String] = Seq.empty,
                     ignore: Seq[String] = Seq.empty,
                     saveMode: SaveMode = SaveMode.ErrorIfExists,
                     filter: Set[String] = Set.empty,
                     statistics: Boolean = false,
                     diffOptions: DiffOptions = DiffOptions.default)

  // read options from args
  val programName = s"spark-extension_${spark.BuildScalaCompatVersionString}-${spark.VersionString}.jar"
  val scop = s"com.github.scopt:scopt_${spark.BuildScalaCompatVersionString}:4.1.0"
  val sparkSubmit = s"spark-submit --packages $scop $programName"
  val parser: OptionParser[Options] = new scopt.OptionParser[Options](programName) {
    head(s"Spark Diff app (${spark.VersionString})")
    head()

    arg[String]("left")
      .required()
      .valueName("<left-path>")
      .action((x, c) => c.copy(leftPath = Some(x)))
      .text("file path (requires format option) or table name to read left dataframe")

    arg[String]("right")
      .required()
      .valueName("<right-path>")
      .action((x, c) => c.copy(rightPath = Some(x)))
      .text("file path (requires format option) or table name to read right dataframe")

    arg[String]("diff")
      .required()
      .valueName("<diff-path>")
      .action((x, c) => c.copy(outputPath = Some(x)))
      .text("file path (requires format option) or table name to write diff dataframe")

    note("")
    note("Examples:")
    note("")
    note("  - Diff CSV files 'left.csv' and 'right.csv' and write result into CSV file 'diff.csv':")
    note(s"    $sparkSubmit --format csv left.csv right.csv diff.csv")
    note("")
    note("  - Diff CSV file 'left.csv' and Parquet file 'right.parquet' with id column 'id',")
    note("    and write result into Hive table 'diff':")
    note(s"    $sparkSubmit --left-format csv --right-format parquet --hive --id id left.csv right.parquet diff")

    note("")
    note("Spark session")
    opt[String]("master")
      .valueName("<master>")
      .action((x, c) => c.copy(master = Some(x)))
      .text("Spark master (local, yarn, ...), not needed with spark-submit")
    opt[String]("app-name")
      .valueName("<app-name>")
      .action((x, c) => c.copy(appName = Some(x)))
      .text("Spark application name")
      .withFallback(() => "Diff App")
    opt[Unit]("hive")
      .optional()
      .action((_, c) => c.copy(hive = true))
      .text(s"enable Hive support to read from and write to Hive tables")

    note("")
    note("Input and output")
    opt[String]('f', "format")
      .valueName("<format>")
      .action((x, c) => c.copy(
        leftFormat = c.leftFormat.orElse(Some(x)),
        rightFormat = c.rightFormat.orElse(Some(x)),
        outputFormat = c.outputFormat.orElse(Some(x))
      ))
      .text("input and output file format (csv, json, parquet, ...)")
    opt[String]("left-format")
      .valueName("<format>")
      .action((x, c) => c.copy(leftFormat = Some(x)))
      .text("left input file format (csv, json, parquet, ...)")
    opt[String]("right-format")
      .valueName("<format>")
      .action((x, c) => c.copy(rightFormat = Some(x)))
      .text("right input file format (csv, json, parquet, ...)")
    opt[String]("output-format")
      .valueName("<formt>")
      .action((x, c) => c.copy(outputFormat = Some(x)))
      .text("output file format (csv, json, parquet, ...)")

    note("")
    opt[String]('s', "schema")
      .valueName("<schema>")
      .action((x, c) => c.copy(
        leftSchema = c.leftSchema.orElse(Some(x)),
        rightSchema = c.rightSchema.orElse(Some(x))
      ))
      .text("input schema")
    opt[String]("left-schema")
      .valueName("<schema>")
      .action((x, c) => c.copy(leftSchema = Some(x)))
      .text("left input schema")
    opt[String]("right-schema")
      .valueName("<schema>")
      .action((x, c) => c.copy(rightSchema = Some(x)))
      .text("right input schema")

    note("")
    opt[(String, String)]("left-option")
      .unbounded()
      .optional()
      .keyValueName("key", "val")
      .action((x, c) => c.copy(leftOptions = c.leftOptions + (x._1 -> x._2)))
      .text("left input option")
    opt[(String, String)]("right-option")
      .unbounded()
      .optional()
      .keyValueName("key", "val")
      .action((x, c) => c.copy(rightOptions = c.rightOptions + (x._1 -> x._2)))
      .text("right input option")
    opt[(String, String)]("output-option")
      .unbounded()
      .optional()
      .keyValueName("key", "val")
      .action((x, c) => c.copy(outputOptions = c.outputOptions + (x._1 -> x._2)))
      .text("output option")

    note("")
    opt[String]("id")
      .unbounded()
      .valueName("<name>")
      .action((x, c) => c.copy(ids = c.ids :+ x))
      .text(s"id column name")
    opt[String]("ignore")
      .unbounded()
      .valueName("<name>")
      .action((x, c) => c.copy(ignore = c.ignore :+ x))
      .text(s"ignore column name")
    opt[String]("save-mode")
      .optional()
      .valueName("<save-mode>")
      .action((x, c) => c.copy(saveMode = SaveMode.valueOf(x)))
      .text(s"save mode for writing output (${SaveMode.values().mkString(", ")}, default ${Options().saveMode})")
    opt[String]("filter")
      .unbounded()
      .optional()
      .valueName("<filter>")
      .action((x, c) => c.copy(filter = c.filter + x))
      .text(s"Filters for rows with these diff actions, with default diffing options use 'N', 'I', 'D', or 'C' (see 'Diffing options' section)")
    opt[Unit]("statistics")
      .optional()
      .action((_, c) => c.copy(statistics = true))
      .text(s"Only output statistics on how many rows exist per diff action (see 'Diffing options' section)")

    note("")
    note("Diffing options")
    opt[String]("diff-column")
      .optional()
      .valueName("<name>")
      .action((x, c) => c.copy(diffOptions = c.diffOptions.copy(diffColumn = x)))
      .text(s"column name for diff column (default '${DiffOptions.default.diffColumn}')")
    opt[String]("left-prefix")
      .optional()
      .valueName("<prefix>")
      .action((x, c) => c.copy(diffOptions = c.diffOptions.copy(leftColumnPrefix = x)))
      .text(s"prefix for left column names (default '${DiffOptions.default.leftColumnPrefix}')")
    opt[String]("right-prefix")
      .optional()
      .valueName("<prefix>")
      .action((x, c) => c.copy(diffOptions = c.diffOptions.copy(rightColumnPrefix = x)))
      .text(s"prefix for right column names (default '${DiffOptions.default.rightColumnPrefix}')")
    opt[String]("insert-value")
      .optional()
      .valueName("<value>")
      .action((x, c) => c.copy(diffOptions = c.diffOptions.copy(insertDiffValue = x)))
      .text(s"value for insertion (default '${DiffOptions.default.insertDiffValue}')")
    opt[String]("change-value")
      .optional()
      .valueName("<value>")
      .action((x, c) => c.copy(diffOptions = c.diffOptions.copy(changeDiffValue = x)))
      .text(s"value for change (default '${DiffOptions.default.changeDiffValue}')")
    opt[String]("delete-value")
      .optional()
      .valueName("<value>")
      .action((x, c) => c.copy(diffOptions = c.diffOptions.copy(deleteDiffValue = x)))
      .text(s"value for deletion (default '${DiffOptions.default.deleteDiffValue}')")
    opt[String]("no-change-value")
      .optional()
      .valueName("<val>")
      .action((x, c) => c.copy(diffOptions = c.diffOptions.copy(nochangeDiffValue = x)))
      .text(s"value for no change (default '${DiffOptions.default.nochangeDiffValue}')")
    opt[String]("change-column")
      .optional()
      .valueName("<name>")
      .action((x, c) => c.copy(diffOptions = c.diffOptions.copy(changeColumn = Some(x))))
      .text(s"column name for change column (default is no such column)")
    opt[String]("diff-mode")
      .optional()
      .valueName("<mode>")
      .action((x, c) => c.copy(diffOptions = c.diffOptions.copy(diffMode = DiffMode.withName(x))))
      .text(s"diff mode (${DiffMode.values.mkString(", ")}, default ${Options().diffOptions.diffMode})")
    opt[Unit]("sparse")
      .optional()
      .action((_, c) => c.copy(diffOptions = c.diffOptions.copy(sparseMode = true)))
      .text(s"enable sparse diff")

    note("")
    note("General")
    help("help").text("prints this usage text")
  }

  def read(spark: SparkSession, format: Option[String], path: String, schema: Option[String], options: Map[String, String]): DataFrame =
    spark.read
      .when(format.isDefined).call(_.format(format.get))
      .options(options)
      .when(schema.isDefined).call(_.schema(schema.get))
      .when(format.isDefined).either(_.load(path)).or(_.table(path))

  def write(df: DataFrame, format: Option[String], path: String, options: Map[String, String], saveMode: SaveMode, filter: Set[String], saveStats: Boolean, diffOptions: DiffOptions): Unit =
    df.when(filter.nonEmpty).call(_.where(col(diffOptions.diffColumn).isInCollection(filter)))
      .when(saveStats).call(_.groupBy(diffOptions.diffColumn).count.orderBy(diffOptions.diffColumn))
      .write
      .when(format.isDefined).call(_.format(format.get))
      .options(options)
      .mode(saveMode)
      .when(format.isDefined).either(_.save(path)).or(_.saveAsTable(path))

  def main(args: Array[String]): Unit = {
    // parse options
    val options = parser.parse(args, Options()) match {
      case Some(options) => options
      case None => sys.exit(1)
    }
    val unknownFilters = options.filter.filter(filter => !options.diffOptions.diffValues.contains(filter))
    if (unknownFilters.nonEmpty) {
      throw new RuntimeException(s"Filter ${unknownFilters.mkString("'", "', '", "'")} not allowed, " +
        s"these are the configured diff values: ${options.diffOptions.diffValues.mkString("'", "', '", "'")}")
    }

    // create spark session
    val spark = SparkSession.builder()
      .appName(options.appName.get)
      .when(options.hive).call(_.enableHiveSupport())
      .when(options.master.isDefined).call(_.master(options.master.get))
      .getOrCreate()

    // read and write
    val left = read(spark, options.leftFormat, options.leftPath.get, options.leftSchema, options.leftOptions)
    val right = read(spark, options.rightFormat, options.rightPath.get, options.rightSchema, options.rightOptions)
    val diff = left.diff(right, options.diffOptions, options.ids, options.ignore)
    write(diff, options.outputFormat, options.outputPath.get, options.outputOptions, options.saveMode, options.filter, options.statistics, options.diffOptions)
  }
}
