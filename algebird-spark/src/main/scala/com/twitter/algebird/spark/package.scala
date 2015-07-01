package com.twitter.algebird

import com.twitter.algebird._
import org.apache.spark.rdd.{ RDD, PairRDDFunctions }
import scala.reflect.ClassTag

/**
 * To use this, you probably want:
 * import com.twitter.algebird.spark._
 */
package object spark {
  /**
   * spark exposes an Aggregator type, so this is here to avoid shadowing
   */
  type AlgebirdAggregator[A, B, C] = Aggregator[A, B, C]
  val AlgebirdAggregator = Aggregator

  implicit class ToAlgebird[T](val rdd: RDD[T]) extends AnyVal {
    def algebird: AlgebirdRDD[T] = new AlgebirdRDD[T](rdd)
  }
}
