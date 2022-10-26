# Partitioned Writing

If you have ever used `Dataset[T].write.partitionBy`, here is how you can minimize the number of
written files and obtain same-size files.

Spark has two different concepts both referred to as partitioning. Central to Spark is the
concept of how a `Dataset[T]` is split into partitions where a Spark worker processes
a single partition at a time. This is the fundamental concept of how Spark scales with data.

When writing a `Dataset` `ds` to a file-based storage, that output file is actually a directory:

<!--
import java.sql.Timestamp
import java.sql.Timestamp

case class Value(id: Int, ts: Timestamp, property: String, value: String)
val ds = Seq(
  Value(1, Timestamp.valueOf("2020-07-01 12:00:00"), "label", "one"),
  Value(1, Timestamp.valueOf("2020-07-02 12:00:00"), "descr", "number one"),
  Value(1, Timestamp.valueOf("2020-07-03 12:00:00"), "label", "ONE"),
  Value(2, Timestamp.valueOf("2020-07-01 12:00:00"), "label", "two"),
  Value(2, Timestamp.valueOf("2020-07-03 12:00:00"), "label", "TWO"),
  Value(2, Timestamp.valueOf("2020-07-04 12:00:00"), "descr", "number two"),
  Value(3, Timestamp.valueOf("2020-07-03 12:00:00"), "label", "THREE"),
  Value(3, Timestamp.valueOf("2020-07-03 12:00:00"), "descr", "number three"),
  Value(4, Timestamp.valueOf("2020-07-01 12:00:00"), "label", "four"),
  Value(4, Timestamp.valueOf("2020-07-03 12:00:00"), "descr", "number four"),
  Value(5, Timestamp.valueOf("2020-07-01 12:00:00"), "label", "five"),
  Value(5, Timestamp.valueOf("2020-07-03 12:00:00"), "descr", "number five"),
  Value(6, Timestamp.valueOf("2020-07-01 12:00:00"), "label", "six"),
  Value(6, Timestamp.valueOf("2020-07-01 12:00:00"), "descr", "number six"),
).toDS()
-->

```scala
ds.write.csv("file.csv")
```

The directory structure looks like:

    file.csv
    file.csv/part-00000-7d34816f-bb53-4f44-ab9d-a62d570e5de0-c000.csv
    file.csv/part-00001-7d34816f-bb53-4f44-ab9d-a62d570e5de0-c000.csv
    file.csv/part-00002-7d34816f-bb53-4f44-ab9d-a62d570e5de0-c000.csv
    file.csv/part-00003-7d34816f-bb53-4f44-ab9d-a62d570e5de0-c000.csv
    file.csv/part-00004-7d34816f-bb53-4f44-ab9d-a62d570e5de0-c000.csv
    file.csv/_SUCCESS

When writing, the output can be `partitionBy` one or more columns of the `Dataset`.
For each distinct `value` in that column `col` an individual sub-directory is created in your output path.
The name is of the format `col=value`. Inside the sub-directory, multiple partitions exists,
all containing only data where column `col` has value `value`. To remove redundancy, those
files do not contain that column anymore.

    file.csv/property=descr/part-00001-8eb44de1-2c33-4f95-a1be-8d1b4e35eb4a.c000.csv
    file.csv/property=descr/part-00002-8eb44de1-2c33-4f95-a1be-8d1b4e35eb4a.c000.csv
    file.csv/property=descr/part-00003-8eb44de1-2c33-4f95-a1be-8d1b4e35eb4a.c000.csv
    file.csv/property=descr/part-00004-8eb44de1-2c33-4f95-a1be-8d1b4e35eb4a.c000.csv
    file.csv/property=label/part-00001-8eb44de1-2c33-4f95-a1be-8d1b4e35eb4a.c000.csv
    file.csv/property=label/part-00002-8eb44de1-2c33-4f95-a1be-8d1b4e35eb4a.c000.csv
    file.csv/property=label/part-00003-8eb44de1-2c33-4f95-a1be-8d1b4e35eb4a.c000.csv
    file.csv/property=label/part-00004-8eb44de1-2c33-4f95-a1be-8d1b4e35eb4a.c000.csv
    file.csv/_SUCCESS

Data that is mis-organized when written end up with the same number of files
in each of the sub-directories, even if some sub-directories contain only a fraction of
the number of rows than others. What you would like to have is have fewer files in smaller
and more files in larger partition sub-directories. Further, all files should have
roughly the same number of rows.

For this, you have to first range partition the `Dataset` according to your partition columns.

    ds.repartitionByRange($"property", $"id")
      .write
      .partitionBy("property")
      .csv("file.csv")

This organizes the data optimally for partition-writing them by column `property`.

    file.csv/property=descr/part-00000-6317db5e-5161-41f1-8227-ffeaf06a3e41.c000.csv
    file.csv/property=descr/part-00001-6317db5e-5161-41f1-8227-ffeaf06a3e41.c000.csv
    file.csv/property=label/part-00002-6317db5e-5161-41f1-8227-ffeaf06a3e41.c000.csv
    file.csv/property=label/part-00003-6317db5e-5161-41f1-8227-ffeaf06a3e41.c000.csv
    file.csv/property=label/part-00004-6317db5e-5161-41f1-8227-ffeaf06a3e41.c000.csv
    file.csv/_SUCCESS

This brings all rows with the same value in the `property` and `id` column into the same file.

If you need each file to further be sorted by additional columns, e.g. `ts`, then you can do this with `sortWithinPartitions`.

    ds.repartitionByRange($"property", $"id")
      .sortWithinPartitions($"property", $"id", $"ts")
      .cache    // this is needed for Spark 3.0 to 3.3 with AQE enabled: SPARK-40588
      .write
      .partitionBy("property")
      .csv("file.csv")

Sometimes you want to write-partition by some expression that is not a column of your data,
e.g. the date-representation of the `ts` column.

    ds.withColumn("date", $"ts".cast(DateType))
      .repartitionByRange($"date", $"id")
      .sortWithinPartitions($"date", $"id", $"ts")
      .cache    // this is needed for Spark 3.0 to 3.3 with AQE enabled: SPARK-40588
      .write
      .partitionBy("date")
      .csv("file.csv")

All those above constructs can be replaced with a single meaningful operation:

    ds.writePartitionedBy(Seq($"ts".cast(DateType).as("date")), Seq($"id"), Seq($"ts"))
      .csv("file.csv")

For Spark 3.0 to 3.3 with AQE enabled (see [SPARK-40588](https://issues.apache.org/jira/browse/SPARK-40588)),
`writePartitionedBy` has to cache an internally created DataFrame. This can be unpersisted after writing
is finished. Provide an `UnpersistHandle` for this purpose:

    val unpersist = UnpersistHandle()

    ds.writePartitionedBy(…, unpersistHandle = Some(unpersist))
      .csv("file.csv")

    unpersist()

<!--
# Other Approaches

problems with `repartition()` instead of `repartitionByRange()`
problems with `repartitionByRange(cols).write.partitionBy(cols)`
-->