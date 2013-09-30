package com.twitter.algebird

import org.specs._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose
import org.scalacheck.Prop.forAll

object SketchMapTestImplicits {
  val DELTA = 1E-10
  val EPS = 0.001
  val SEED = 1
  val HEAVY_HITTERS_COUNT = 10
}


object SketchMapLaws extends Properties("SketchMap") {
  import BaseProperties._
  import SketchMapTestImplicits._
  import HyperLogLog.int2Bytes

  implicit val smMonoid = SketchMap.monoid[Int, Long](EPS, DELTA, SEED, HEAVY_HITTERS_COUNT)
  implicit val smGen = Arbitrary {
    for (key: Int <- choose(0, 10000)) yield (smMonoid.create(key, 1L))
  }

  property("SketchMap is a Monoid") = monoidLaws[SketchMap[Int, Long]]
}


class SketchMapTest extends Specification {
  import SketchMapTestImplicits._
  import HyperLogLog.int2Bytes

  noDetailedDiffs()

  val MONOID = SketchMap.monoid[Int, Long](EPS, DELTA, SEED, HEAVY_HITTERS_COUNT)
  val RAND = new scala.util.Random

  "SketchMap" should {
    "count total number of elements in a stream" in {
      val totalCount = 1243
      val range = 234
      val data = (0 to (totalCount - 1)).map { _ => (RAND.nextInt(range), 1L) }
      val sm = MONOID.create(data)

      sm.totalValue must be_==(totalCount)
    }

    "exactly compute frequencies in a small stream" in {
      val one = MONOID.create(1, 1L)
      val two = MONOID.create(2, 1L)
      val sm = MONOID.plus(MONOID.plus(one, two), two)

      sm.frequency(0) must be_==(0L)
      sm.frequency(1) must be_==(1L)
      sm.frequency(2) must be_==(2L)

      val three = MONOID.create(1, 3L)
      three.frequency(1) must be_==(3L)
      val four = MONOID.create(1, 4L)
      four.frequency(1) must be_==(4L)
      val sm2 = MONOID.plus(four, three)
      sm2.frequency(1) must be_==(7L)
    }

    "drop old heavy hitters when new heavy hitters replace them" in {
      val monoid = SketchMap.monoid[Int, Long](EPS, DELTA, SEED, 1)

      val sm1 = monoid.create(Seq((1, 5L), (2, 4L)))
      sm1.heavyHitters must be_==(List((1, 5L)))

      val sm2 = monoid.plus(sm1, monoid.create(2, 2L))
      sm2.heavyHitters must be_==(List((2, 6L)))

      val sm3 = monoid.plus(sm2, monoid.create(1, 2L))
      sm3.heavyHitters must be_==(List((1, 7L)))

      val sm4 = monoid.plus(sm3, monoid.create(0, 10L))
      sm4.heavyHitters must be_==(List((0, 10L)))
    }

    "exactly compute heavy hitters in a small stream" in {
      val data = Seq((1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L))

      val sm1 = SketchMap.monoid[Int, Long](EPS, DELTA, SEED, 5).create(data)
      val sm2 = SketchMap.monoid[Int, Long](EPS, DELTA, SEED, 3).create(data)
      val sm3 = SketchMap.monoid[Int, Long](EPS, DELTA, SEED, 1).create(data)
      val sm4 = SketchMap.monoid[Int, Long](EPS, DELTA, SEED, 0).create(data)

      sm1.heavyHitterKeys must be_==(List(5, 4, 3, 2, 1))
      sm2.heavyHitterKeys must be_==(List(5, 4, 3))
      sm3.heavyHitterKeys must be_==(List(5))
      sm4.heavyHitterKeys must be_==(List.empty[Int])
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

      val monoid = SketchMap.monoid[Int, Long](EPS, DELTA, SEED, 5)(implicitly[Int => Array[Byte]], smallerOrdering, smallerMonoid)

      val sm1 = monoid.create((100, 10L))
      sm1.heavyHitters must be_==(List((100, 10L)))

      // Summing should yield the smaller number, via smallerMonoid.
      val sm2 = monoid.plus(sm1, monoid.create((100, 5L)))
      sm2.heavyHitters must be_==(List((100, 5L)))

      // Summing a bigger number should not affect the data structure.
      val sm3 = monoid.plus(sm2, monoid.create((100, 100L)))
      sm3.heavyHitters must be_==(List((100, 5L)))

      // Try more than one at a time.
      val sm4 = monoid.plus(sm3, monoid.create(Seq((100, 100L), (200, 30L), (200, 20L), (200, 10L))))
      sm4.heavyHitters must be_==(List((100, 5L), (200, 10L)))
    }

    "work as an Aggregator" in {
      val data = Seq((1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L))

      val sm1 = SketchMap.aggregator[Int, Long](EPS, DELTA, SEED, 5).apply(data)
      val sm2 = SketchMap.aggregator[Int, Long](EPS, DELTA, SEED, 3).apply(data)
      val sm3 = SketchMap.aggregator[Int, Long](EPS, DELTA, SEED, 1).apply(data)
      val sm4 = SketchMap.aggregator[Int, Long](EPS, DELTA, SEED, 0).apply(data)

      sm1.heavyHitterKeys must be_==(List(5, 4, 3, 2, 1))
      sm2.heavyHitterKeys must be_==(List(5, 4, 3))
      sm3.heavyHitterKeys must be_==(List(5))
      sm4.heavyHitterKeys must be_==(List.empty[Int])
    }
  }
}
