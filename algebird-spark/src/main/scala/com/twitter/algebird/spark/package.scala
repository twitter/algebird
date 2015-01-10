package com.twitter.algebird

import org.apache.spark.rdd.{ RDD, PairRDDFunctions }

import scala.reflect.ClassTag
import com.twitter.algebird._

/**
 * To use this, you probably want:
 * import com.twitter.algebird.spark._
 */
package object spark {
  /**
   * This adds methods to Spark RDDs to use Algebird
   */
  implicit class AlgebirdRDD[T](val rdd: RDD[T]) extends AnyVal {
    /**
     * Apply an Aggregator to return a single value for the whole RDD
     */
    def aggregatorOnAll[B: ClassTag, C](agg: Aggregator[T, B, C]): C =
      agg.present(rdd.map(agg.prepare).reduce(agg.reduce))

    def aggregatorByKey[K: ClassTag, A: ClassTag, B: ClassTag, C](agg: Aggregator[A, B, C])(implicit ev: T <:< (K, A), ordK: Ordering[K] = null): RDD[(K, C)] =
      /**
       * We use the cast to avoid having to serialize the ev, which could also be applied
       */
      (new PairRDDFunctions(rdd.map { t =>
        val tupl = t.asInstanceOf[(K, A)]
        (tupl._1, agg.prepare(tupl._2))
      })).reduceByKey(agg.reduce)
        .map { case (k, b) => (k, agg.present(b)) }

    private def keyed[K, V](implicit ev: T <:< (K, V)): RDD[(K, V)] = rdd.asInstanceOf[RDD[(K, V)]]

    /**
     * Use the implicit semigroup to sum by keys
     */
    def semigroupByKey[K: ClassTag, V: ClassTag: Semigroup](implicit ev: T <:< (K, V), ord: Ordering[K] = null): RDD[(K, V)] = {
      (new PairRDDFunctions(keyed)).reduceByKey(implicitly[Semigroup[V]].plus)
    }
    /**
     * Use the implicit semigroup to sum all items
     */
    def semigroupOnAll(implicit sg: Semigroup[T]): T = rdd.reduce(sg.plus)
  }
}
