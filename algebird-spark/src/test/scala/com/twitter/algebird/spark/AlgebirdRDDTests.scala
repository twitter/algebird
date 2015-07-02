package com.twitter.algebird.spark

import com.twitter.algebird.{ MapAlgebra, Semigroup, Monoid, Min }
import org.apache.spark._
import org.apache.spark.rdd._
import org.scalatest._
import org.scalatest.DiagrammedAssertions._
import scala.reflect.ClassTag

class AlgebirdRDDTest extends FunSuite with BeforeAndAfter {
  private val master = "local[2]"
  private val appName = "algebird-rdd-test"

  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  after { if (sc != null) { sc.stop() } }

  // Why does scala.math.Equiv suck so much.
  implicit def optEq[V](implicit eq: Equiv[V]): Equiv[Option[V]] = Equiv.fromFunction[Option[V]] { (o1, o2) =>
    (o1, o2) match {
      case (Some(v1), Some(v2)) => eq.equiv(v1, v2)
      case (None, None) => true
      case _ => false
    }
  }

  def equiv[V](a: V, b: V)(implicit eq: Equiv[V]): Boolean = eq.equiv(a, b)
  def assertEq[V: Equiv](a: V, b: V): Unit = assert(equiv(a, b))

  def aggregate[T: ClassTag, U: ClassTag, V: Equiv](s: Seq[T], agg: AlgebirdAggregator[T, U, V]) {
    assertEq(sc.makeRDD(s).algebird.aggregate(agg), agg(s))
  }

  def aggregateByKey[K: ClassTag, T: ClassTag, U: ClassTag, V: Equiv](s: Seq[(K, T)], agg: AlgebirdAggregator[T, U, V]) {
    val resMap = sc.makeRDD(s).algebird.aggregateByKey[K, T, U, V](agg).collect.toMap
    implicit val sg = agg.semigroup
    val algMap = MapAlgebra.sumByKey(s.map { case (k, t) => k -> agg.prepare(t) }).mapValues(agg.present)
    s.map(_._1).toSet.foreach { k: K =>
      assertEq(resMap.get(k), algMap.get(k))
    }
  }

  def sumOption[T: ClassTag: Equiv: Semigroup](s: Seq[T]) {
    assertEq(sc.makeRDD(s).algebird.sumOption, Semigroup.sumOption(s))
  }

  def sumByKey[K: ClassTag, V: ClassTag: Semigroup: Equiv](s: Seq[(K, V)]) {
    val resMap = sc.makeRDD(s).algebird.sumByKey[K, V].collect.toMap
    val algMap = MapAlgebra.sumByKey(s)
    s.map(_._1).toSet.foreach { k: K =>
      assertEq(resMap.get(k), algMap.get(k))
    }
  }

  test("aggregate") {
    aggregate(0 to 1000, AlgebirdAggregator.fromSemigroup[Int])
    aggregate(0 to 1000, AlgebirdAggregator.min[Int])
    aggregate(0 to 1000, AlgebirdAggregator.sortedTake[Int](3))
  }
  test("sumOption") {
    sumOption(0 to 1000)
    sumOption((0 to 1000).map(Min(_)))
    sumOption((0 to 1000).map(x => (x, x % 3)))
  }
  test("aggregateByKey") {
    aggregateByKey((0 to 1000).map(k => (k % 3, k)), AlgebirdAggregator.fromSemigroup[Int])
    aggregateByKey((0 to 1000).map(k => (k % 3, k)), AlgebirdAggregator.min[Int])
    aggregateByKey((0 to 1000).map(k => (k % 3, k)), AlgebirdAggregator.sortedTake[Int](3))
  }
  test("sumByKey") {
    sumByKey((0 to 1000).map(k => (k % 3, k)))
    sumByKey((0 to 1000).map(k => (k % 3, Option(k))))
    sumByKey((0 to 1000).map(k => (k % 3, Min(k))))
  }
}
