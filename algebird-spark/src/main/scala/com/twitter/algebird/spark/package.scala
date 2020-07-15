package com.twitter.algebird

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * To use this, you probably want:
 * import com.twitter.algebird.spark._
 */
package object spark {

import org.apache.spark.sql.Dataset

  /**
   * spark exposes an Aggregator type, so this is here to avoid shadowing
   */
  type AlgebirdAggregator[A, B, C] = Aggregator[A, B, C]
  val AlgebirdAggregator = Aggregator

  implicit class ToAlgebirdRDD[T](val rdd: RDD[T]) extends AnyVal {
    def algebird: AlgebirdRDD[T] = new AlgebirdRDD[T](rdd)
  }

  implicit class ToAlgebirdDataset[T](val ds: Dataset[T]) extends AnyVal {
    def algebird: AlgebirdDataset[T] = new AlgebirdDataset[T](ds)
  }

  def rddMonoid[T: ClassTag](sc: SparkContext): Monoid[RDD[T]] = new Monoid[RDD[T]] {
    def zero = sc.emptyRDD[T]
    override def isNonZero(s: RDD[T]) = s.isEmpty
    def plus(a: RDD[T], b: RDD[T]) = a.union(b)
  }

  implicit def rddSemigroup[T]: Semigroup[RDD[T]] = new Semigroup[RDD[T]] {
    def plus(a: RDD[T], b: RDD[T]) = a.union(b)
  }
  // We should be able to make an Applicative[RDD] except that map needs an implicit ClassTag
  // which breaks the Applicative signature. I don't see a way around that.


}
