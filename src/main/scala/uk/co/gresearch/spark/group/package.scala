package uk.co.gresearch.spark

import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedStar}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, encoderFor}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.catalyst.expressions.{Alias, BoundReference, CreateStruct, Expression, GetStructField, If, IsNull, Literal}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.ObjectType
import org.apache.spark.sql.{Column, Dataset, Encoder, Encoders, TypedColumn}

import scala.reflect.classTag

package object group {

  def keyValueEncoder[K: Encoder, V: Encoder](expr: Column*):  ExpressionEncoder[(K,V)] = {
    // serializer takes expr and projects to row that is encode with Encoder[K].serializer, while encoding unprojected row to V, turning both into Tuple
    // deserializer takes _._2 and deserializes it with Encoder[V].deserializer
    val keyEncoder: ExpressionEncoder[K] = encoderFor[K]
    val valueEncoder: ExpressionEncoder[V] = encoderFor[V]

    val clsTag = classTag[(K, V)]
    val cls = clsTag.runtimeClass

    val serializerInput = CreateStruct(expr.map(_.expr))
    //val serializer = CreateStruct(Seq(keyEncoder.objSerializer, valueEncoder.objSerializer))
    //val newSerializerInput = BoundReference(0, ObjectType(cls), nullable = true)
    val serializers = Seq(keyEncoder, valueEncoder).zipWithIndex.map { case (enc, index) =>
      val boundRefs = enc.objSerializer.collect { case b: BoundReference => b }.distinct
      assert(boundRefs.size == 1, "object serializer should have only one bound reference but " +
        s"there are ${boundRefs.size}")

      val originalInputObject = boundRefs.head
      val newInputObject = Invoke(
        serializerInput,
        s"_${index + 1}",
        originalInputObject.dataType,
        returnNullable = originalInputObject.nullable)

      val newSerializer = enc.objSerializer.transformUp {
        case BoundReference(0, _, _) => newInputObject
      }

      Alias(newSerializer, s"_${index + 1}")()
      //Seq(Literal(s"_${index + 1}"), newSerializer)
    }
    val serializer = CreateStruct(serializers)
    //val serializer = CreateNamedStruct(serializers)

    val deserializerInput = GetColumnByOrdinal(0, serializer.dataType)
    val getColExprs = valueEncoder.objDeserializer.collect { case c: GetColumnByOrdinal => c }.distinct
    assert(getColExprs.size == 1, "object deserializer should have only one " +
      s"`GetColumnByOrdinal`, but there are ${getColExprs.size}")

    val input = GetStructField(deserializerInput, 1)
    val deserializer = valueEncoder.objDeserializer.transformUp {
      case GetColumnByOrdinal(0, _) => input
    }
    val newDeserializer = NewInstance(cls, Seq(deserializer), ObjectType(cls), propagateNull = false)

    def nullSafe(input: Expression, result: Expression): Expression = {
      If(IsNull(input), Literal.create(null, result.dataType), result)
    }

    new ExpressionEncoder[(K,V)](
      nullSafe(serializerInput, serializer),
      nullSafe(deserializerInput, newDeserializer),
      clsTag)
  }

  case class SortedGroupByDataset[K: Ordering : Encoder, V: Encoder](ds: Dataset[V],
                                                                     groupColumns: Seq[Column],
                                                                     orderColumns: Seq[Column],
                                                                     partitions: Option[Int]) {
    implicit val encoder: Encoder[(K, V)] = Encoders.tuple(encoderFor[K], encoderFor[V])
    //implicit val encoder: Encoder[(K, V)] = keyValueEncoder[K, V](groupColumns: _*)
//    val groupColumn: TypedColumn[V, K] =
//      if (groupColumns.length == 1)
//        new Column(groupColumns.map(_.expr).head).as[K]
//      else
//        new Column(CreateStruct(groupColumns.map(_.expr))).as("key").as[K]

//    val valueColumn: TypedColumn[V, V] = new Column(CreateStruct(Seq(UnresolvedStar(None)))).as("value").as[V]

    private val partitioned = partitions
      .map(parts => ds.repartition(parts, groupColumns: _*))
      .getOrElse(ds.repartition(groupColumns: _*))

    private val grouped = partitioned.sortWithinPartitions(groupColumns ++ orderColumns : _*)
      .select(
        new Column(CreateStruct(groupColumns.map(_.expr))).as("key"),  //.as[K],
        new Column(CreateStruct(Seq(UnresolvedStar(None)))).as("value")  //.as[V]
      )
      .as[(K, V)]

    def flatMapSortedGroups[W: Encoder](func: (K, Iterator[V]) => TraversableOnce[W]): Dataset[W] =
      grouped.mapPartitions(new GroupedIterator(_).flatMap(v => func(v._1, v._2)))

    def flatMapSortedGroups[S, W: Encoder](s: K => S)(func: (S, V) => TraversableOnce[W]): Dataset[W] = {
      grouped.mapPartitions(new GroupedIterator(_).flatMap { case (k, it) =>
        val state = s(k)
        it.flatMap(v => func(state, v))
      })
    }

  }

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

    def flatMapSortedGroups[S, W: Encoder](s: K => S)(func: (S, V) => TraversableOnce[W]): Dataset[W] = {
      ds.mapPartitions(new GroupedIterator(_).flatMap { case (k, it) =>
        val state = s(k)
        it.flatMap(v => func(state, v))
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
      one == null && two == null || one != null && two != null && ordering.equiv(one, two)

    override def hasNext: Boolean = iter.hasNext && identicalKeys(iter.head._1, key)

    override def next(): V = iter.next._2
  }

}
