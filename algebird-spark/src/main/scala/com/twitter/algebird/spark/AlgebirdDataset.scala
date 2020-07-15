package com.twitter.algebird

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder

class AlgebirdDataset[T](val ds: Dataset[T]) extends AnyVal {

  def aggregateOption[B: Encoder, C](
      agg: Aggregator[T, B, C]
  ): Option[C] = {
    val pr = ds.mapPartitions(data =>
      if (data.isEmpty) Iterator.empty
      else {
        val b = agg.prepare(data.next)
        Iterator(agg.appendAll(b, data))
      }
    )

    val results = pr
      .repartition(1)
      .mapPartitions(it => agg.semigroup.sumOption(it).toIterator)
      .collect

    if (results.isEmpty) None
    else Some(agg.present(results.head))
  }

  def aggregate[B: Encoder, C](agg: Aggregator[T, B, C]): C =
    (aggregateOption[B, C](agg), agg.semigroup) match {
      case (Some(c), _)         => c
      case (None, m: Monoid[B]) => agg.present(m.zero)
      case (None, _)            => None.get // no such element
    }

  def aggregateByKey[K: Encoder, V1: Encoder, U: Encoder, V2: Encoder](
      agg: Aggregator[V1, U, V2]
  )(
      implicit ev: T <:< (K, V1),
      enc1: Encoder[(K, U)],
      enc2: Encoder[(K, V2)]
  ): Dataset[(K, V2)] =
    keyed
      .mapPartitions(it => it.map { case (k, v) => (k, agg.prepare(v)) })
      .groupByKey(_._1)
      .reduceGroups((a: (K, U), b: (K, U)) => (a._1, agg.reduce(a._2, b._2)))
      .map { case (k, (_, v)) => (k, agg.present(v)) }

  private def keyed[K, V](implicit ev: T <:< (K, V)): Dataset[(K, V)] =
    ds.asInstanceOf[Dataset[(K, V)]]

  def sumByKey[K: Encoder, V: Semigroup: Encoder]()(
      implicit ev: T <:< (K, V),
      enc: Encoder[(K, V)]
  ): Dataset[(K, V)] =
    keyed
      .groupByKey(_._1)
      .reduceGroups((a: (K, V), b: (K, V)) => (a._1, implicitly[Semigroup[V]].plus(a._2, b._2)))
      .map { case (k, (_, v)) => (k, v) }

  def sumOption(implicit sg: Semigroup[T], enc1: Encoder[T]): Option[T] = {
    val partialReduce: Dataset[T] =
      ds.mapPartitions(itT => sg.sumOption(itT).toIterator)

    val results = partialReduce
      .repartition(1)
      .mapPartitions(it => sg.sumOption(it).toIterator)
      .collect

    if (results.isEmpty) None
    else Some(results.head)
  }

  def sum(implicit mon: Monoid[T], enc1: Encoder[T]): T =
    sumOption.getOrElse(mon.zero)
}
