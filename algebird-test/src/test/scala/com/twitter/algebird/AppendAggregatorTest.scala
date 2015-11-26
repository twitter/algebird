package com.twitter.algebird

import org.scalatest._

class AppendAggregatorTest extends WordSpec with Matchers {
  val data = Vector.fill(100) { scala.util.Random.nextInt(100) }
  val mpty = Vector.empty[Int]

  // test the methods that appendMonoid method defines or overrides
  def testMethods[E, M, P](
    agg1: MonoidAggregator[E, M, P],
    agg2: MonoidAggregator[E, M, P],
    data: Seq[E],
    empty: Seq[E]) {

    val n = data.length
    val (half1, half2) = data.splitAt(n / 2)
    val lhs = agg1.appendAll(half1)

    data.foreach { e =>
      agg1.prepare(e) should be(agg2.prepare(e))
    }

    agg1.present(lhs) should be(agg2.present(lhs))

    agg1(data) should be (agg2(data))
    agg1(empty) should be (agg2(empty))

    agg1.applyOption(data) should be(agg2.applyOption(data))
    agg1.applyOption(empty) should be(agg2.applyOption(empty))

    half2.foreach { e =>
      agg1.append(lhs, e) should be(agg2.append(lhs, e))
    }

    agg1.appendAll(lhs, half2) should be(agg2.appendAll(lhs, half2))

    agg2.appendAll(data) should be(agg2.appendAll(data))
  }

  "appendMonoid" should {
    "be equivalent to integer monoid aggregator" in {
      val agg1 = Aggregator.fromMonoid[Int]
      val agg2 = Aggregator.appendMonoid((m: Int, e: Int) => m + e)
      testMethods(agg1, agg2, data, mpty)
    }

    "be equivalent to set monoid aggregator" in {
      object setMonoid extends Monoid[Set[Int]] {
        val zero = Set.empty[Int]
        def plus(m1: Set[Int], m2: Set[Int]) = m1 ++ m2
      }

      val agg1 = Aggregator.prepareMonoid((e: Int) => Set(e))(setMonoid)
      val agg2 = Aggregator.appendMonoid((m: Set[Int], e: Int) => m + e)(setMonoid)

      testMethods(agg1, agg2, data, mpty)
    }
  }
}
