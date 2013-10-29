package com.twitter.algebird

import org.specs2.mutable._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose
import org.scalacheck.Prop.forAll

object CountMinSketchLaws extends Properties("CountMinSketch") {
  import BaseProperties._

  val DELTA = 1E-8
  val EPS = 0.005
  val SEED = 1

  implicit val cmsMonoid = new CountMinSketchMonoid(EPS, DELTA, SEED)
  implicit val cmsGen =
    Arbitrary {
      for (v <- choose(0, 10000)) yield (cmsMonoid.create(v))
    }

  property("CountMinSketch is a Monoid") = monoidLaws[CMS]
}

class CountMinSketchTest extends Specification {


  val DELTA = 1E-10
  val EPS = 0.001
  val SEED = 1

  val CMS_MONOID = new CountMinSketchMonoid(EPS, DELTA, SEED)
  val RAND = new scala.util.Random

  /**
   * Returns the exact frequency of {x} in {data}.
   */
  def exactFrequency(data : Seq[Long], x : Long) : Long = {
    data.filter { _ == x }.size
  }

  /**
   * Returns the exact inner product between two data streams, when the streams
   * are viewed as count vectors.
   */
  def exactInnerProduct(data1 : Seq[Long], data2 : Seq[Long]) : Long = {
    val counts1 = data1.groupBy( x => x ).mapValues( _.size )
    val counts2 = data2.groupBy( x => x ).mapValues( _.size )

    (counts1.keys.toSet & counts2.keys.toSet).map { k => counts1(k) * counts2(k) }.sum
  }

  /**
   * Returns the elements in {data} that appear at least heavyHittersPct * data.size times.
   */
  def exactHeavyHitters(data : Seq[Long], heavyHittersPct : Double) : Set[Long] = {
    val counts = data.groupBy( x => x ).mapValues( _.size )
    val totalCount = counts.values.sum
    counts.filter { _._2 >= heavyHittersPct * totalCount }.keys.toSet
  }

  "CountMinSketch" should {

    "count total number of elements in a stream" in {
      val totalCount = 1243
      val range = 234
      val data = (0 to (totalCount - 1)).map { _ => RAND.nextInt(range).toLong }
      val cms = CMS_MONOID.create(data)

      cms.totalCount must be_==(totalCount)
    }

    "estimate frequencies" in {
      val totalCount = 5678
      val range = 897
      val data = (0 to (totalCount - 1)).map { _ => RAND.nextInt(range).toLong }
      val cms = CMS_MONOID.create(data)

      (0 to 100).foreach { _ =>
        val x = RAND.nextInt(range).toLong
        val exact = exactFrequency(data, x)
        val approx = cms.frequency(x).estimate
        val maxError = approx - cms.frequency(x).min

        approx must be_>=(exact)
        (approx - exact) must be_<=(maxError)
      }
    }

    "exactly compute frequencies in a small stream" in {
      val one = CMS_MONOID.create(1)
      val two = CMS_MONOID.create(2)
      val cms = CMS_MONOID.plus(CMS_MONOID.plus(one, two), two)

      cms.frequency(0).estimate must be_==(0)
      cms.frequency(1).estimate must be_==(1)
      cms.frequency(2).estimate must be_==(2)

      val three = CMS_MONOID.create(Seq(1L, 1L, 1L))
      three.frequency(1L).estimate must be_==(3)
      val four = CMS_MONOID.create(Seq(1L, 1L, 1L, 1L))
      four.frequency(1L).estimate must be_==(4)
      val cms2 = CMS_MONOID.plus(four, three)
      cms2.frequency(1L).estimate must be_==(7)
    }

    "estimate inner products" in {
      val totalCount = 5234
      val range = 1390
      val data1 = (0 to (totalCount - 1)).map { _ => RAND.nextInt(range).toLong }
      val data2 = (0 to (totalCount - 1)).map { _ => RAND.nextInt(range).toLong }
      val cms1 = CMS_MONOID.create(data1)
      val cms2 = CMS_MONOID.create(data1)

      val approxA = cms1.innerProduct(cms2)
      val approx = approxA.estimate
      val exact = exactInnerProduct(data1, data2)
      val maxError = approx - approxA.min

      approx must be_==(cms2.innerProduct(cms1).estimate)
      approx must be_>=(exact)
      (approx - exact) must be_<=(maxError)
    }

    "exactly compute inner product of small streams" in {
      // Nothing in common.
      val a1 = List(1L, 2L, 3L)
      val a2 = List(4L, 5L, 6L)
      CMS_MONOID.create(a1).innerProduct(CMS_MONOID.create(a2)).estimate must be_==(0)

      // One element in common.
      val b1 = List(1L, 2L, 3L)
      val b2 = List(3L, 5L, 6L)
      CMS_MONOID.create(b1).innerProduct(CMS_MONOID.create(b2)).estimate must be_==(1)

      // Multiple, non-repeating elements in common.
      val c1 = List(1L, 2L, 3L)
      val c2 = List(3L, 2L, 6L)
      CMS_MONOID.create(c1).innerProduct(CMS_MONOID.create(c2)).estimate must be_==(2)

      // Multiple, repeating elements in common.
      val d1 = List(1L, 2L, 2L, 3L, 3L)
      val d2 = List(2L, 3L, 3L, 6L)
      CMS_MONOID.create(d1).innerProduct(CMS_MONOID.create(d2)).estimate must be_==(6)
    }

    "estimate heavy hitters" in {
      // Simple way of making some elements appear much more often than others.
      val data1 = (1 to 3000).map { _ => RAND.nextInt(3).toLong }
      val data2 = (1 to 3000).map { _ => RAND.nextInt(10).toLong }
      val data3 = (1 to 1450).map { _ => -1L } // element close to being a 20% heavy hitter
      val data = data1 ++ data2 ++ data3

      // Find elements that appear at least 20% of the time.
      val cms = (new CountMinSketchMonoid(EPS, DELTA, SEED, 0.2)).create(data)

      val trueHhs = exactHeavyHitters(data, cms.heavyHittersPct)
      val estimatedHhs = cms.heavyHitters

      // All true heavy hitters must be claimed as heavy hitters.
      (trueHhs.intersect(estimatedHhs) == trueHhs) must be_==(true)

      // It should be very unlikely that any element with count less than
      // (heavyHittersPct - eps) * totalCount is claimed as a heavy hitter.
      val minHhCount = (cms.heavyHittersPct - cms.eps) * cms.totalCount
      val infrequent = data.groupBy{ x => x }.mapValues{ _.size }.filter{ _._2 < minHhCount }.keys.toSet
      infrequent.intersect(estimatedHhs).size must be_==(0)
    }

    "drop old heavy hitters when new heavy hitters replace them" in {
      val monoid = new CountMinSketchMonoid(EPS, DELTA, SEED, 0.3)
      val cms1 = monoid.create(Seq(1L, 2L, 2L))
      cms1.heavyHitters must be_==(Set(1L, 2L))

      val cms2 = cms1 ++ monoid.create(2L)
      cms2.heavyHitters must be_==(Set(2L))

      val cms3 = cms2 ++ monoid.create(1L)
      cms3.heavyHitters must be_==(Set(1L, 2L))

      val cms4 = cms3 ++ monoid.create(Seq(0L, 0L, 0L, 0L, 0L, 0L))
      cms4.heavyHitters must be_==(Set(0L))
    }

    "exactly compute heavy hitters in a small stream" in {
      val data1 = Seq(1L, 2L, 2L, 3L, 3L, 3L, 4L, 4L, 4L, 4L, 5L, 5L, 5L, 5L, 5L)
      val cms1 = (new CountMinSketchMonoid(EPS, DELTA, SEED, 0.01)).create(data1)
      val cms2 = (new CountMinSketchMonoid(EPS, DELTA, SEED, 0.1)).create(data1)
      val cms3 = (new CountMinSketchMonoid(EPS, DELTA, SEED, 0.3)).create(data1)
      val cms4 = (new CountMinSketchMonoid(EPS, DELTA, SEED, 0.9)).create(data1)

      cms1.heavyHitters must be_==(Set(1L, 2L, 3L, 4L, 5L))
      cms2.heavyHitters must be_==(Set(2L, 3L, 4L, 5L))
      cms3.heavyHitters must be_==(Set(5L))
      cms4.heavyHitters must be_==(Set[Long]())
    }

    "work as an Aggregator" in {
      val data1 = Seq(1L, 2L, 2L, 3L, 3L, 3L, 4L, 4L, 4L, 4L, 5L, 5L, 5L, 5L, 5L)
      val cms1 = CMS.aggregator(EPS, DELTA, SEED, 0.01).apply(data1)
      val cms2 = CMS.aggregator(EPS, DELTA, SEED, 0.1).apply(data1)
      val cms3 = CMS.aggregator(EPS, DELTA, SEED, 0.3).apply(data1)
      val cms4 = CMS.aggregator(EPS, DELTA, SEED, 0.9).apply(data1)

      cms1.heavyHitters must be_==(Set(1L, 2L, 3L, 4L, 5L))
      cms2.heavyHitters must be_==(Set(2L, 3L, 4L, 5L))
      cms3.heavyHitters must be_==(Set(5L))
      cms4.heavyHitters must be_==(Set[Long]())
    }
  }
}
