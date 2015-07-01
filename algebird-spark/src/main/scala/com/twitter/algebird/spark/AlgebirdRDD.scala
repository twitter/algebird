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
   * Apply an Aggregator to return a single value for the whole RDD
   */
  def aggregate[B: ClassTag, C](agg: Aggregator[T, B, C]): C = {
    val partialReduce: RDD[B] = rdd.mapPartitions({ itT =>
      val itB = itT.map(agg.prepare)
      agg.semigroup.sumOption(itB)
        .fold(Iterator.empty: Iterator[B]) { b: B => Iterator(b) }
    }, preservesPartitioning = true)
    // Finish the job
    agg.present(partialReduce.reduce(agg.reduce))
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
  def sumByKey[K: ClassTag, V: ClassTag: Semigroup](implicit ev: T <:< (K, V), ord: Ordering[K] = null): RDD[(K, V)] = {
    (new PairRDDFunctions(keyed)).reduceByKey(implicitly[Semigroup[V]].plus)
  }
  /**
   * Use the implicit semigroup to sum all items
   */
  def sum(implicit sg: Semigroup[T], ct: ClassTag[T]): T =
    rdd.mapPartitions({ itT =>
      sg.sumOption(itT)
        .fold(Iterator.empty: Iterator[T]) { t: T => Iterator(t) }
    }, preservesPartitioning = true).reduce(sg.plus)
}
