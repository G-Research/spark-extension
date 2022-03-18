package uk.co.gresearch.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoder, Encoders}

package object group {

  case class SortedGroupByKeyValueDataset[K: Ordering : Encoder, V: Encoder, O](kv: Dataset[(K, V, O)],
                                                                                partitions: Option[Int],
                                                                                reverse: Boolean) {
    implicit val encoder: Encoder[(K, V)] = Encoders.tuple(implicitly[Encoder[K]], implicitly[Encoder[V]])

    private def keyColumn = col(kv.columns.head)
    private def sortColumn = if (reverse) col(kv.columns.last).desc else col(kv.columns.last)

    private val partitioned = partitions.map(parts => kv.repartition(parts, keyColumn)).getOrElse(kv.repartition(keyColumn))
    private val ds = partitioned.sortWithinPartitions(keyColumn, sortColumn).map(v => (v._1, v._2))

    def flatMapSortedGroups[W: Encoder](func: (K, Iterator[V]) => TraversableOnce[W]): Dataset[W] =
      ds.mapPartitions(new GroupedIterator(_).flatMap(v => func(v._1, v._2)))

    def flatMapSortedGroupsWithKeys[W: Encoder](func: (K, Iterator[V]) => TraversableOnce[W]): Dataset[(K, W)] = {
      implicit val encoder: Encoder[(K, W)] = Encoders.tuple(implicitly[Encoder[K]], implicitly[Encoder[W]])
      ds.mapPartitions(new GroupedIterator(_).flatMap(v => func(v._1, v._2).map(w => (v._1, w))))
    }

    def flatMapSortedGroups[S, W: Encoder](s: K => S)(f: (S, V) => TraversableOnce[W]): Dataset[W] = {
      ds.mapPartitions(new GroupedIterator(_).flatMap { case (k, it) =>
        val state = s(k)
        it.flatMap(v => f(state, v))
      })
    }

    def flatMapSortedGroupsWithKey[S, W: Encoder](s: K => S)(f: (S, V) => TraversableOnce[W]): Dataset[(K, W)] = {
      implicit val encoder: Encoder[(K, W)] = Encoders.tuple(implicitly[Encoder[K]], implicitly[Encoder[W]])
      ds.mapPartitions(new GroupedIterator(_).flatMap { case (k, it) =>
        val state = s(k)
        it.flatMap(v => f(state, v).map(w => (k, w)))
      })
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
      one == null && two == null || one != null && two != null && ordering.compare(one, two) == 0

    override def hasNext: Boolean = iter.hasNext && identicalKeys(iter.head._1, key)

    override def next(): V = iter.next._2
  }

}
