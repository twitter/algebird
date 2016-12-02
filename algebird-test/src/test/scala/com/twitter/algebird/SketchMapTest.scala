package com.twitter.algebird

import org.scalatest._
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary }

object SketchMapTestImplicits {
  val DELTA = 1E-6
  val EPS = 0.001
  val SEED = 1
  val HEAVY_HITTERS_COUNT = 10
}

class SketchMapLaws extends CheckProperties {
  import BaseProperties._
  import SketchMapTestImplicits._
  import HyperLogLog.int2Bytes

  val params = SketchMapParams[Int](SEED, EPS, 1e-3, HEAVY_HITTERS_COUNT)
  implicit val smMonoid = SketchMap.monoid[Int, Long](params)
  implicit val smGen = Arbitrary {
    for (key: Int <- Gen.choose(0, 10000)) yield (smMonoid.create((key, 1L)))
  }

  // TODO: SketchMap's heavy hitters are not strictly associative
  // (approximately they are)
  implicit def equiv[K, V]: Equiv[SketchMap[K, V]] =
    Equiv.fromFunction { (left, right) =>
      (left.valuesTable == right.valuesTable) &&
        (left.totalValue == right.totalValue)
    }

  property("SketchMap is a commutative monoid") {
    commutativeMonoidLawsEquiv[SketchMap[Int, Long]]
  }
}

class SketchMapTest extends WordSpec with Matchers {
  import SketchMapTestImplicits._
  import HyperLogLog.int2Bytes

  val PARAMS = SketchMapParams[Int](SEED, EPS, DELTA, HEAVY_HITTERS_COUNT)
  val MONOID = SketchMap.monoid[Int, Long](PARAMS)
  val RAND = new scala.util.Random

  "SketchMap" should {
    "count total number of elements in a stream" in {
      val totalCount = 1243
      val range = 234
      val data = (0 to (totalCount - 1)).map { _ => (RAND.nextInt(range), 1L) }
      val sm = MONOID.create(data)
      assert(sm.totalValue == totalCount)
    }

    "exactly compute frequencies in a small stream" in {
      val one = MONOID.create((1, 1L))
      val two = MONOID.create((2, 1L))
      val sm = MONOID.plus(MONOID.plus(one, two), two)
      assert(MONOID.frequency(sm, 0) == 0L)
      assert(MONOID.frequency(sm, 1) == 1L)
      assert(MONOID.frequency(sm, 2) == 2L)

      val three = MONOID.create((1, 3L))
      assert(MONOID.frequency(three, 1) == 3L)
      val four = MONOID.create((1, 4L))
      assert(MONOID.frequency(four, 1) == 4L)
      val sm2 = MONOID.plus(four, three)
      assert(MONOID.frequency(sm2, 1) == 7L)
    }

    "drop old heavy hitters when new heavy hitters replace them" in {
      val params = SketchMapParams[Int](SEED, EPS, DELTA, 1)
      val monoid = SketchMap.monoid[Int, Long](params)

      val sm1 = monoid.create(Seq((1, 5L), (2, 4L)))
      assert(monoid.heavyHitters(sm1) == List((1, 5L)))

      val sm2 = monoid.plus(sm1, monoid.create((2, 2L)))
      assert(monoid.heavyHitters(sm2) == List((2, 6L)))

      val sm3 = monoid.plus(sm2, monoid.create((1, 2L)))
      assert(monoid.heavyHitters(sm3) == List((1, 7L)))

      val sm4 = monoid.plus(sm3, monoid.create((0, 10L)))
      assert(monoid.heavyHitters(sm4) == List((0, 10L)))
    }

    "exactly compute heavy hitters in a small stream" in {
      val data = Seq((1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L))

      val params1 = SketchMapParams[Int](SEED, EPS, DELTA, 5)
      val monoid1 = SketchMap.monoid[Int, Long](params1)
      val sm1 = monoid1.create(data)
      val params2 = SketchMapParams[Int](SEED, EPS, DELTA, 3)
      val monoid2 = SketchMap.monoid[Int, Long](params2)
      val sm2 = monoid2.create(data)
      val params3 = SketchMapParams[Int](SEED, EPS, DELTA, 1)
      val monoid3 = SketchMap.monoid[Int, Long](params3)
      val sm3 = monoid3.create(data)
      val params4 = SketchMapParams[Int](SEED, EPS, DELTA, 0)
      val monoid4 = SketchMap.monoid[Int, Long](params4)
      val sm4 = monoid4.create(data)
      assert(sm1.heavyHitterKeys == List(5, 4, 3, 2, 1))
      assert(sm2.heavyHitterKeys == List(5, 4, 3))
      assert(sm3.heavyHitterKeys == List(5))
      assert(sm4.heavyHitterKeys == List.empty[Int])
    }

    "use custom monoid" in {
      // Monoid that takes the smaller number.
      val smallerMonoid: Monoid[Long] = new Monoid[Long] {
        override def zero: Long = Long.MaxValue
        override def plus(first: Long, second: Long): Long = {
          if (first < second) {
            first
          } else {
            second
          }
        }
      }

      // Ordering that orders from biggest to smallest (so that HeavyHitters
      // are the smallest numbers).
      val smallerOrdering: Ordering[Long] = Ordering.by[Long, Long] { -_ }

      val monoid = SketchMap.monoid[Int, Long](PARAMS)(smallerOrdering, smallerMonoid)

      val sm1 = monoid.create((100, 10L))
      assert(monoid.heavyHitters(sm1) == List((100, 10L)))

      // Summing should yield the smaller number, via smallerMonoid.
      val sm2 = monoid.plus(sm1, monoid.create((100, 5L)))
      assert(monoid.heavyHitters(sm2) == List((100, 5L)))

      // Summing a bigger number should not affect the data structure.
      val sm3 = monoid.plus(sm2, monoid.create((100, 100L)))
      assert(monoid.heavyHitters(sm3) == List((100, 5L)))

      // Try more than one at a time.
      val sm4 = monoid.plus(sm3, monoid.create(Seq((100, 100L), (200, 30L), (200, 20L), (200, 10L))))
      assert(monoid.heavyHitters(sm4) == List((100, 5L), (200, 10L)))
    }

    "work as an Aggregator" in {
      val data = Seq((1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L))

      val params1 = SketchMapParams[Int](SEED, EPS, DELTA, 5)
      val params2 = SketchMapParams[Int](SEED, EPS, DELTA, 3)
      val params3 = SketchMapParams[Int](SEED, EPS, DELTA, 1)
      val params4 = SketchMapParams[Int](SEED, EPS, DELTA, 0)
      val sm1 = SketchMap.aggregator[Int, Long](params1).apply(data)
      val sm2 = SketchMap.aggregator[Int, Long](params2).apply(data)
      val sm3 = SketchMap.aggregator[Int, Long](params3).apply(data)
      val sm4 = SketchMap.aggregator[Int, Long](params4).apply(data)
      assert(sm1.heavyHitterKeys == List(5, 4, 3, 2, 1))
      assert(sm2.heavyHitterKeys == List(5, 4, 3))
      assert(sm3.heavyHitterKeys == List(5))
      assert(sm4.heavyHitterKeys == List.empty[Int])
    }
  }
}
