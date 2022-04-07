package uk.co.gresearch.spark

import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.{Column, Dataset, Encoder, Encoders}
import uk.co.gresearch.ExtendedAny

package object group {

  /**
   * This is a Dataset of key-value tuples, that provide a flatMap function over the individual groups,
   * while providing a sorted iterator over group values.
   *
   * The key-value Dataset given the constructor has to be partitioned by the key
   * and sorted within partitions by the key and value.
   *
   * @param ds the properly partitioned and sorted dataset
   * @param ordering$K$0 ordering over the keys
   * @param encoder$K$1 encoder for the keys
   * @param encoder$V$0 encoder for the values
   * @tparam K type of the keys
   * @tparam V type of the values
   */
  case class SortedGroupByDataset[K: Ordering : Encoder, V: Encoder] private (ds: Dataset[(K, V)]) {
    def flatMapSortedGroups[W: Encoder](func: (K, Iterator[V]) => TraversableOnce[W]): Dataset[W] =
      ds.mapPartitions(new GroupedIterator(_).flatMap(v => func(v._1, v._2)))

    def flatMapSortedGroups[S, W: Encoder](s: K => S)(func: (S, V) => TraversableOnce[W]): Dataset[W] = {
      ds.mapPartitions(new GroupedIterator(_).flatMap { case (k, it) =>
        val state = s(k)
        it.flatMap(v => func(state, v))
      })
    }
  }

  object SortedGroupByDataset {
    def apply[K: Ordering : Encoder, V: Encoder](ds: Dataset[V],
                                                 groupColumns: Seq[Column],
                                                 orderColumns: Seq[Column],
                                                 partitions: Option[Int]): SortedGroupByDataset[K, V] = {
      // multiple group columns are turned into a tuple,
      // while a single group column is taken as is
      val keyColumn =
      if (groupColumns.length == 1)
        groupColumns.head
      else
        struct(groupColumns: _*)

      // all columns are turned into a single colum as a struct
      val valColumn = struct(col("*"))

      // repartition by group columns with given number of partitions (if given)
      // sort within partitions by group and order columns
      // finally, turn key and value into typed classes
      val grouped = ds
        .on(partitions.isDefined)
        .either(_.repartition(partitions.get, groupColumns: _*))
        .or(_.repartition(groupColumns: _*))
        .sortWithinPartitions(groupColumns ++ orderColumns: _*)
        .select(
          keyColumn.as("key").as[K],
          valColumn.as("value").as[V]
        )

      SortedGroupByDataset(grouped)
    }

    def apply[K: Ordering : Encoder, V: Encoder, O: Encoder](ds: Dataset[V],
                                                             key: V => K,
                                                             order: V => O,
                                                             partitions: Option[Int],
                                                             reverse: Boolean): SortedGroupByDataset[K, V] = {
      implicit val kvEncoder: Encoder[(K, V)] = Encoders.tuple(implicitly[Encoder[K]], implicitly[Encoder[V]])
      implicit val kvoEncoder: Encoder[(K, V, O)] = Encoders.tuple(implicitly[Encoder[K]], implicitly[Encoder[V]], implicitly[Encoder[O]])

      // materialise the key and order class for each value
      val kvo = ds.map(v => (key(v), v, order(v)))

      // sort by key and order column
      def keyColumn = col(kvo.columns.head)

      def orderColumn = if (reverse) col(kvo.columns.last).desc else col(kvo.columns.last)

      // repartition by group columns with given number of partitions (if given)
      // sort within partitions by group and order columns
      // finally, turn key and value into typed classes
      val grouped = kvo
        .on(partitions.isDefined)
        .either(_.repartition(partitions.get, keyColumn))
        .or(_.repartition(keyColumn))
        .sortWithinPartitions(keyColumn, orderColumn)
        .map(v => (v._1, v._2))

      SortedGroupByDataset(grouped)
    }
  }

  class GroupedIterator[K: Ordering, V](iter: Iterator[(K, V)]) extends Iterator[(K, Iterator[V])] {
    private val values = iter.buffered
    private var currentKey: Option[K] = None
    private var currentGroup: Option[Iterator[V]] = None

    override def hasNext: Boolean = {
      if (currentKey.isEmpty) {
        if (currentGroup.isDefined) {
          // consume current group
          val it = currentGroup.get
          while (it.hasNext) it.next
          currentGroup = None
        }

        if (values.hasNext) {
          currentKey = Some(values.head._1)
          currentGroup = Some(new GroupIterator(values))
        }
      }
      currentKey.isDefined
    }

    override def next(): (K, Iterator[V]) = {
      try {
        (currentKey.get, currentGroup.get)
      } finally {
        currentKey = None
      }
    }
  }

  class GroupIterator[K: Ordering, V](iter: BufferedIterator[(K, V)]) extends Iterator[V] {
    private val ordering = implicitly[Ordering[K]]
    private val key = iter.head._1

    private def identicalKeys(one: K, two: K): Boolean =
      one == null && two == null || one != null && two != null && ordering.equiv(one, two)

    override def hasNext: Boolean = iter.hasNext && identicalKeys(iter.head._1, key)

    override def next(): V = iter.next._2
  }

}
