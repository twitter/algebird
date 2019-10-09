package com.twitter.algebird

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.Partitioner
import scala.reflect.ClassTag

/**
 * import com.twitter.algebird.spark.ToAlgebird
 * to get the enrichment to do:
 * myRdd.algebird: AlgebirdRDD[T]
 *
 * This adds methods to Spark RDDs to use Algebird
 */
class AlgebirdRDD[T](val rdd: RDD[T]) extends AnyVal {

  /**
   * Apply an Aggregator to return a single value for the whole RDD.
   * If the RDD is empty, None is returned
   * requires a commutative Semigroup. To generalize to non-commutative, we need a sorted partition for
   * T.
   */
  def aggregateOption[B: ClassTag, C](agg: Aggregator[T, B, C]): Option[C] = {
    val pr = rdd.mapPartitions({ data =>
      if (data.isEmpty) Iterator.empty
      else {
        val b = agg.prepare(data.next)
        Iterator(agg.appendAll(b, data))
      }
    }, preservesPartitioning = true)
    pr.repartition(1)
      .mapPartitions(pr => Iterator(agg.semigroup.sumOption(pr)))
      .collect
      .head
      .map(agg.present)
  }

  /**
   * This will throw if you use a non-MonoidAggregator with an empty RDD
   * requires a commutative Semigroup. To generalize to non-commutative, we need a sorted partition for
   * T.
   */
  def aggregate[B: ClassTag, C](agg: Aggregator[T, B, C]): C =
    (aggregateOption[B, C](agg), agg.semigroup) match {
      case (Some(c), _)         => c
      case (None, m: Monoid[B]) => agg.present(m.zero)
      case (None, _)            => None.get // no such element
    }

  /**
   * Apply an Aggregator to the values for each key.
   * requires a commutative Semigroup. To generalize to non-commutative, we need a sorted partition for
   * T.
   */
  def aggregateByKey[K: ClassTag, V1, U: ClassTag, V2](
      agg: Aggregator[V1, U, V2]
  )(implicit ev: T <:< (K, V1), ordK: Priority[Ordering[K], DummyImplicit]): RDD[(K, V2)] =
    aggregateByKey(Partitioner.defaultPartitioner(rdd), agg)

  private def toPair[K: ClassTag, V: ClassTag](
      kv: RDD[(K, V)]
  )(implicit ordK: Priority[Ordering[K], DummyImplicit]) =
    new PairRDDFunctions(kv)(implicitly, implicitly, ordK.getPreferred.orNull)

  /**
   * Apply an Aggregator to the values for each key with a custom Partitioner.
   * requires a commutative Semigroup. To generalize to non-commutative, we need a sorted partition for
   * T.
   */
  def aggregateByKey[K: ClassTag, V1, U: ClassTag, V2](
      part: Partitioner,
      agg: Aggregator[V1, U, V2]
  )(implicit ev: T <:< (K, V1), ordK: Priority[Ordering[K], DummyImplicit]): RDD[(K, V2)] = {
    /*
     * This mapValues implementation allows us to avoid needing the V1 ClassTag, which would
     * be required to use the implementation in PairRDDFunctions
     */
    val prepared = keyed.mapPartitions({ it =>
      it.map { case (k, v) => (k, agg.prepare(v)) }
    }, preservesPartitioning = true)

    toPair(
      toPair(prepared)
        .reduceByKey(part, agg.reduce(_, _))
    ).mapValues(agg.present)
  }

  private def keyed[K, V](implicit ev: T <:< (K, V)): RDD[(K, V)] = rdd.asInstanceOf[RDD[(K, V)]]

  /**
   * Use the implicit semigroup to sum by keys
   * requires a commutative Semigroup. To generalize to non-commutative, we need a sorted partition for
   * T.
   */
  def sumByKey[K: ClassTag, V: ClassTag: Semigroup](
      implicit ev: T <:< (K, V),
      ord: Priority[Ordering[K], DummyImplicit]
  ): RDD[(K, V)] =
    sumByKey(Partitioner.defaultPartitioner(rdd))

  /**
   * Use the implicit semigroup to sum by keys with a custom Partitioner.
   * requires a commutative Semigroup. To generalize to non-commutative, we need a sorted partition for
   * T.
   * Unfortunately we need to use a different name than sumByKey in scala 2.11
   */
  def sumByKey[K: ClassTag, V: ClassTag: Semigroup](
      part: Partitioner
  )(implicit ev: T <:< (K, V), ord: Priority[Ordering[K], DummyImplicit]): RDD[(K, V)] =
    toPair(keyed).reduceByKey(part, implicitly[Semigroup[V]].plus _)

  /**
   * Use the implicit Monoid to sum all items. If RDD is empty, Monoid.zero is returned
   * requires a commutative Monoid. To generalize to non-commutative, we need a sorted partition for
   * T.
   */
  def sum(implicit mon: Monoid[T], ct: ClassTag[T]): T =
    sumOption.getOrElse(mon.zero)

  /**
   * Use the implicit Semigroup to sum all items. If there are no items, None is returned.
   * requires a commutative Monoid. To generalize to non-commutative, we need a sorted partition for
   * T.
   */
  def sumOption(implicit sg: Semigroup[T], ct: ClassTag[T]): Option[T] = {
    val partialReduce: RDD[T] = rdd.mapPartitions({ itT =>
      sg.sumOption(itT).toIterator
    }, preservesPartitioning = true)

    // my reading of the docs is that we do want a shuffle at this stage to
    // to make sure the upstream work is done in parallel.
    val results = partialReduce
      .repartition(1)
      .mapPartitions({ it =>
        Iterator(sg.sumOption(it))
      }, preservesPartitioning = true)
      .collect

    assert(results.size == 1, s"Should only be 1 item: ${results.toList}")
    results.head // there can only be one item now
  }
}
