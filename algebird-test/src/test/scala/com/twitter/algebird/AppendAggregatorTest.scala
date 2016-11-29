package com.twitter.algebird

import org.scalatest._

class AppendAggregatorTest extends WordSpec with Matchers {
  val data = Vector.fill(100) { scala.util.Random.nextInt(100) }
  val mpty = Vector.empty[Int]

  // test the methods that appendSemigroup method defines or overrides
  def testMethodsSemigroup[E, M, P](
    agg1: Aggregator[E, M, P],
    agg2: Aggregator[E, M, P],
    data: Seq[E],
    empty: Seq[E]) {

    val n = data.length
    val (half1, half2) = data.splitAt(n / 2)
    val lhs = agg1.appendAll(agg1.prepare(half1.head), half1.tail)

    data.foreach { e =>
      agg1.prepare(e) should be(agg2.prepare(e))
    }

    agg1.present(lhs) should be(agg2.present(lhs))

    agg1(data) should be (agg2(data))

    agg1.applyOption(data) should be(agg2.applyOption(data))
    agg1.applyOption(empty) should be(agg2.applyOption(empty))

    half2.foreach { e =>
      agg1.append(lhs, e) should be(agg2.append(lhs, e))
    }

    agg1.appendAll(lhs, half2) should be(agg2.appendAll(lhs, half2))
  }

  // test the methods that appendMonoid method defines or overrides
  def testMethodsMonoid[E, M, P](
    agg1: MonoidAggregator[E, M, P],
    agg2: MonoidAggregator[E, M, P],
    data: Seq[E],
    empty: Seq[E]) {

    testMethodsSemigroup(agg1, agg2, data, empty)

    agg1(empty) should be (agg2(empty))
    agg1.appendAll(data) should be(agg2.appendAll(data))
  }

  "appendMonoid" should {
    "be equivalent to integer monoid aggregator" in {
      val agg1 = Aggregator.fromMonoid[Int]
      val agg2 = Aggregator.appendMonoid((m: Int, e: Int) => m + e)
      testMethodsMonoid(agg1, agg2, data, mpty)
    }

    "be equivalent to set monoid aggregator" in {
      object setMonoid extends Monoid[Set[Int]] {
        val zero = Set.empty[Int]
        def plus(m1: Set[Int], m2: Set[Int]) = m1 ++ m2
      }

      val agg1 = Aggregator.prepareMonoid((e: Int) => Set(e))(setMonoid)
      val agg2 = Aggregator.appendMonoid((m: Set[Int], e: Int) => m + e)(setMonoid)

      testMethodsMonoid(agg1, agg2, data, mpty)
    }
  }

  "appendSemigroup" should {
    "be equivalent to integer semigroup aggregator" in {
      val agg1 = Aggregator.fromSemigroup[Int]
      val agg2 = Aggregator.appendSemigroup(identity[Int]_, (m: Int, e: Int) => m + e)
      testMethodsSemigroup(agg1, agg2, data, mpty)
    }

    "be equivalent to set semigroup aggregator" in {
      object setSemigroup extends Semigroup[Set[Int]] {
        def plus(m1: Set[Int], m2: Set[Int]) = m1 ++ m2
      }

      val agg1 = Aggregator.prepareSemigroup((e: Int) => Set(e))(setSemigroup)
      val agg2 = Aggregator.appendSemigroup((e: Int) => Set(e), (m: Set[Int], e: Int) => m + e)(setSemigroup)

      testMethodsSemigroup(agg1, agg2, data, mpty)
    }
  }
}
