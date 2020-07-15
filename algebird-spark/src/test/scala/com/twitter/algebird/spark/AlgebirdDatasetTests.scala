package com.twitter.algebird.spark

import com.twitter.algebird.{MapAlgebra, Monoid, Semigroup}
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import com.twitter.algebird.Min
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Dataset
import com.twitter.algebird.BloomFilter
import com.twitter.algebird.BloomFilterAggregator
import com.twitter.algebird.Hash128

package test {
  // not needed in the algebird package, just testing the API

  object DatasetTest {
    def sum[T: Monoid: Encoder](r: Dataset[T]) = r.algebird.sum
  }
}

class AlgebirdDatasetTest extends AnyFunSuite with BeforeAndAfter {

  private var spark: SparkSession = _

  before {
    spark = SparkSession.builder().master("local").getOrCreate()
  }

  after {
    // try spark.stop()
    // catch {
    //   case t: Throwable => ()
    // }
  }

  // Why does scala.math.Equiv suck so much.
  implicit def optEq[V](implicit eq: Equiv[V]): Equiv[Option[V]] = Equiv.fromFunction[Option[V]] { (o1, o2) =>
    (o1, o2) match {
      case (Some(v1), Some(v2)) => eq.equiv(v1, v2)
      case (None, None)         => true
      case _                    => false
    }
  }

  def equiv[V](a: V, b: V)(implicit eq: Equiv[V]): Boolean = eq.equiv(a, b)
  def assertEq[V: Equiv](a: V, b: V): Unit = assert(equiv(a, b))

  def aggregate[T: Encoder, U: Encoder, V: Equiv](
      s: Seq[T],
      agg: AlgebirdAggregator[T, U, V]
  ): Unit =
    assertEq(spark.createDataset(s).algebird.aggregate(agg), agg(s))

  def aggregateByKey[K: Encoder, T: Encoder, U: Encoder, V: Equiv: Encoder](
      s: Seq[(K, T)],
      agg: AlgebirdAggregator[T, U, V]
  )(implicit enc1: Encoder[(K, V)], enc2: Encoder[(K, T)], enc3: Encoder[(K, U)]): Unit = {
    val resMap = spark.createDataset(s).algebird.aggregateByKey[K, T, U, V](agg).collect.toMap
    implicit val sg = agg.semigroup
    val algMap = MapAlgebra.sumByKey(s.map { case (k, t) => k -> agg.prepare(t) }).mapValues(agg.present)
    s.map(_._1).toSet.foreach { k: K => assertEq(resMap.get(k), algMap.get(k)) }
  }

  def sumOption[T: Encoder: Equiv: Semigroup](s: Seq[T]): Unit =
    assertEq(spark.createDataset(s).algebird.sumOption, Semigroup.sumOption(s))

  def sumByKey[K: Encoder, V: Encoder: Semigroup: Equiv](
      s: Seq[(K, V)]
  )(implicit enc: Encoder[(K, V)]): Unit = {
    val resMap = spark.createDataset(s).algebird.sumByKey[K, V].collect.toMap
    val algMap = MapAlgebra.sumByKey(s)
    s.map(_._1).toSet.foreach { k: K => assertEq(resMap.get(k), algMap.get(k)) }
  }

  import com.twitter.algebird.spark.implicits._

  implicit val hash = Hash128.intHash
  /**
   * These tests almost always timeout on Travis. Leaving the
   * above to at least check compilation
   */
  test("aggregate") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    aggregate(0 to 1000, AlgebirdAggregator.fromSemigroup[Int])
    aggregate(0 to 1000, AlgebirdAggregator.min[Int])
    aggregate(0 to 1000, AlgebirdAggregator.sortedTake[Int](3))
    aggregate(0 to 1000, BloomFilterAggregator(1000,1000))

  }
  test("sumOption") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    sumOption(0 to 1000)
    sumOption((0 to 1000).map(Min(_)))
    sumOption((0 to 1000).map(x => (x, x % 3)))
  }
  test("aggregateByKey") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    aggregateByKey((0 to 1000).map(k => (k % 3, k)), AlgebirdAggregator.fromSemigroup[Int])
    aggregateByKey((0 to 1000).map(k => (k % 3, k)), AlgebirdAggregator.min[Int])
    aggregateByKey((0 to 1000).map(k => (k % 3, k)), AlgebirdAggregator.sortedTake[Int](3))
  }
  test("sumByKey") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    sumByKey((0 to 1000).map(k => (k % 3, k)))
    sumByKey((0 to 1000).map(k => (k % 3, Option(k))))
    sumByKey((0 to 1000).map(k => (k % 3, Min(k))))
  }
}
