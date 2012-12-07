package com.twitter.algebird

import org.specs._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose

object CountMinSketchLaws extends Properties("CountMinSketch") with BaseProperties {
  val DELTA = 1E-10
  val EPS = 0.001
  val SEED = 1

  implicit val cmsMonoid = new CountMinSketchMonoid(EPS, DELTA, SEED)
  implicit val cmsGen =
    Arbitrary {
      for (v <- choose(0, 10000)) yield (cmsMonoid.create(v))
    }

  property("CountMinSketch is a Monoid") = monoidLaws[CMS]
}

class CountMinSketchTest extends Specification {
  noDetailedDiffs()

  // The randomized tests should fail with probability less than
  // delta.
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
   * Returns the estimated frequency of {x} in the given Count-Min sketch
   * structure.
   */
  def approximateFrequency(cms : CMS, x : Long) : Long = {
    cms.estimateFrequency(x)
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
   * Returns the estimated inner product between two Count-Min sketch structures.
   */
  def approximateInnerProduct(cms1 : CMS, cms2 : CMS) : Long = {
    cms1.estimateInnerProduct(cms2)
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
        val approx = approximateFrequency(cms, x)
        val maxError = cms.maxErrorOfFrequencyEstimate

        approx must be_>=(exact)
        (approx - exact).toDouble must be_<=(maxError)
      }
    }

    "exactly estimate frequencies when given a small stream" in {
      val one = CMS_MONOID.create(1)
      val two = CMS_MONOID.create(2)
      val cms = CMS_MONOID.plus(CMS_MONOID.plus(one, two), two)

      cms.estimateFrequency(0) must be_==(0)
      cms.estimateFrequency(1) must be_==(1)
      cms.estimateFrequency(2) must be_==(2)
    }

    "estimate inner products" in {
      val totalCount = 5234
      val range = 1390
      val data1 = (0 to (totalCount - 1)).map { _ => RAND.nextInt(range).toLong }
      val data2 = (0 to (totalCount - 1)).map { _ => RAND.nextInt(range).toLong }
      val cms1 = CMS_MONOID.create(data1)
      val cms2 = CMS_MONOID.create(data1)

      val approx = cms1.estimateInnerProduct(cms2)
      val exact = exactInnerProduct(data1, data2)
      val maxError = cms1.maxErrorOfInnerProductEstimate(cms2)

      approx must be_==(cms2.estimateInnerProduct(cms1))
      approx must be_>=(exact)
      (approx - exact).toDouble must be_<=(maxError)
    }

    "exactly estimate inner products when given a small stream" in {
      // Nothing in common.
      val a1 = List(1L, 2L, 3L)
      val a2 = List(4L, 5L, 6L)
      CMS_MONOID.create(a1).estimateInnerProduct(CMS_MONOID.create(a2)) must be_==(0)

      // One element in common.
      val b1 = List(1L, 2L, 3L)
      val b2 = List(3L, 5L, 6L)
      CMS_MONOID.create(b1).estimateInnerProduct(CMS_MONOID.create(b2)) must be_==(1)

      // Multiple, non-repeating elements in common.
      val c1 = List(1L, 2L, 3L)
      val c2 = List(3L, 2L, 6L)
      CMS_MONOID.create(c1).estimateInnerProduct(CMS_MONOID.create(c2)) must be_==(2)

      // Multiple, repeating elements in common.
      val d1 = List(1L, 2L, 2L, 3L, 3L)
      val d2 = List(2L, 3L, 3L, 6L)
      CMS_MONOID.create(d1).estimateInnerProduct(CMS_MONOID.create(d2)) must be_==(6)
    }
  }
}