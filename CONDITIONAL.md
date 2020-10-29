# Conditional Transformation

The Spark `DataFrame` API allows for chaining transformations as in the following example:

    df.where($"id" === 1)
      .withColumn("state", lit("new"))
      .orderBy($"timestamp")

If you want to perform any of the transformations only if a condition is true,
then your you have to break that chain and the code becomes harder to read:

    val condition = true

    val filteredDf = df.where($"id" === 1)
    val condDf = if (condition) df.withColumn("state", lit("new")) else df
    val result = df.orderBy($"timestamp")

With the following implicit class you can conditionally call transformations:

    implicit class ConditionalDataFrame[T](df: Dataset[T]) {
      def conditionally(condition: Boolean, method: DataFrame => DataFrame): DataFrame =
        if (condition) method(df.toDF) else df.toDF
    }

    val condition = true

    val result =
      df.where($"id" === 1)
        .conditionally(condition, _.withColumn("state", lit("new")))
        .orderBy($"timestamp")

With an `Option`, this could look like:

    val state: Option[String] = Some("new")

    val result =
      df.where($"id" === 1)
        .conditionally(state.isDefined, _.withColumn("state", lit(state.get)))
        .orderBy($"timestamp")

### Even-sized Partitions for variable-sized Languages

Spark splits `DataFrame`s into partitions to be able to scale out.
   
    implicit class PartitionedDataFrame[T](df: Dataset[T]) {
      // with this range partition and sort you get fewer partitions
      // for smaller languages and order within your partitions
      // the partitionBy allows you to read in a subset of languages efficiently
      def writePartitionedBy(hadoopPartitions: Seq[String],
                             fileIds: Seq[String],
                             fileOrder: Seq[String] = Seq.empty,
                             projection: Option[Seq[Column]] = None): DataFrameWriter[Row] = {
        df
          .repartitionByRange((hadoopPartitions ++ fileIds).map(col): _*)
          .sortWithinPartitions((hadoopPartitions ++ fileIds ++ fileOrder).map(col): _*)
          .conditionally(projection.isDefined, _.select(projection.get: _*))
          .write
          .partitionBy(hadoopPartitions: _*)
      }
    }

equivalent code for `writePartitionedBy`
