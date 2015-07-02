package com.twitter.algebird

import com.twitter.algebird._
import org.apache.spark.rdd.{ RDD, PairRDDFunctions }
import scala.reflect.ClassTag
/**
 * import com.twitter.algebird.spark.toAlgebird
 * to get the enrichment to do:
 * myRdd.algebird: AlgebirdRDD[T]
 *
 * This adds methods to Spark RDDs to use Algebird
 */
class AlgebirdRDD[T](val rdd: RDD[T]) extends AnyVal {
  /**
   * Apply an Aggregator to return a single value for the whole RDD.
   * If the RDD is empty, None is returned
   */
  def aggregateOption[B: ClassTag, C](agg: Aggregator[T, B, C]): Option[C] =
    (new AlgebirdRDD(rdd.map(agg.prepare)))
      .sumOption(agg.semigroup, implicitly)
      .map(agg.present)

  /**
   * This will throw if you use a non-MonoidAggregator with an empty RDD
   */
  def aggregate[B: ClassTag, C](agg: Aggregator[T, B, C]): C = (aggregateOption[B, C](agg), agg.semigroup) match {
    case (Some(c), _) => c
    case (None, m: Monoid[B]) => agg.present(m.zero)
    case (None, _) => None.get // no such element
  }

  def aggregateByKey[K: ClassTag, V1, U: ClassTag, V2](agg: Aggregator[V1, U, V2])(implicit ev: T <:< (K, V1), ordK: Ordering[K] = null): RDD[(K, V2)] = {
    val prepared = new PairRDDFunctions(rdd.map { t =>
      /*
       * We use the cast to avoid having to serialize the ev, which could also be applied
       */
      val tupl = t.asInstanceOf[(K, V1)]
      (tupl._1, agg.prepare(tupl._2))
    })
    prepared
      .reduceByKey(agg.reduce)
      .map { case (k, b) => (k, agg.present(b)) }
  }

  private def keyed[K, V](implicit ev: T <:< (K, V)): RDD[(K, V)] = rdd.asInstanceOf[RDD[(K, V)]]

  /**
   * Use the implicit semigroup to sum by keys
   */
  def sumByKey[K: ClassTag, V: ClassTag: Semigroup](implicit ev: T <:< (K, V), ord: Ordering[K] = null): RDD[(K, V)] =
    (new PairRDDFunctions(keyed)).reduceByKey(implicitly[Semigroup[V]].plus)

  /**
   * Use the implicit Monoid to sum all items
   */
  def sum(implicit mon: Monoid[T], ct: ClassTag[T]): T =
    sumOption.getOrElse(mon.zero)

  def sumOption(implicit sg: Semigroup[T], ct: ClassTag[T]): Option[T] = {
    val partialReduce: RDD[T] = rdd.mapPartitions({ itT => sg.sumOption(itT).toIterator },
      preservesPartitioning = true)

    // my reading of the docs is that we do want a shuffle at this stage to
    // to make sure the upstream work is done in parallel.
    val results = partialReduce.coalesce(1, shuffle = true).mapPartitions({ it =>
      Iterator(sg.sumOption(it))
    }, preservesPartitioning = true)
      .collect

    assert(results.size == 1, s"Should only be 1 item: ${results.toList}")
    results.head // there can only be one item now
  }
}
