package com.twitter.algebird

import org.scalatest.{ PropSpec, Matchers, WordSpec }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary }

import CMSHasherImplicits._

class CmsLaws extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._

  val DELTA = 1E-8
  val EPS = 0.005
  val SEED = 1

  private def createArbitrary[K: Numeric](cmsMonoid: CMSMonoid[K]): Arbitrary[CMS[K]] = {
    val k = implicitly[Numeric[K]]
    Arbitrary {
      for (v <- Gen.choose(0, 10000)) yield cmsMonoid.create(k.fromInt(v))
    }
  }

  property("CountMinSketch[Short] is a Monoid") {
    implicit val cmsMonoid = CMS.monoid[Short](EPS, DELTA, SEED)
    implicit val cmsGen = createArbitrary[Short](cmsMonoid)
    monoidLaws[CMS[Short]]
  }

  property("CountMinSketch[Int] is a Monoid") {
    implicit val cmsMonoid = CMS.monoid[Int](EPS, DELTA, SEED)
    implicit val cmsGen = createArbitrary[Int](cmsMonoid)
    monoidLaws[CMS[Int]]
  }

  property("CountMinSketch[Long] is a Monoid") {
    implicit val cmsMonoid = CMS.monoid[Long](EPS, DELTA, SEED)
    implicit val cmsGen = createArbitrary[Long](cmsMonoid)
    monoidLaws[CMS[Long]]
  }

  property("CountMinSketch[BigInt] is a Monoid") {
    implicit val cmsMonoid = CMS.monoid[BigInt](EPS, DELTA, SEED)
    implicit val cmsGen = createArbitrary[BigInt](cmsMonoid)
    monoidLaws[CMS[BigInt]]
  }

}

class TopPctCmsLaws extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._

  val DELTA = 1E-8
  val EPS = 0.005
  val SEED = 1
  val HEAVY_HITTERS_PCT = 0.1

  private def createArbitrary[K: Numeric](cmsMonoid: TopPctCMSMonoid[K]): Arbitrary[TopCMS[K]] = {
    val k = implicitly[Numeric[K]]
    Arbitrary {
      for (v <- Gen.choose(0, 10000)) yield cmsMonoid.create(k.fromInt(v))
    }
  }

  property("TopPctCms[Short] is a Monoid") {
    implicit val cmsMonoid = TopPctCMS.monoid[Short](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)
    implicit val cmsGen = createArbitrary[Short](cmsMonoid)
    monoidLaws[TopCMS[Short]]
  }

  property("TopPctCms[Int] is a Monoid") {
    implicit val cmsMonoid = TopPctCMS.monoid[Int](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)
    implicit val cmsGen = createArbitrary[Int](cmsMonoid)
    monoidLaws[TopCMS[Int]]
  }

  property("TopPctCms[Long] is a Monoid") {
    implicit val cmsMonoid = TopPctCMS.monoid[Long](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)
    implicit val cmsGen = createArbitrary[Long](cmsMonoid)
    monoidLaws[TopCMS[Long]]
  }

  property("TopPctCms[BigInt] is a Monoid") {
    implicit val cmsMonoid = TopPctCMS.monoid[BigInt](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)
    implicit val cmsGen = createArbitrary[BigInt](cmsMonoid)
    monoidLaws[TopCMS[BigInt]]
  }

}

class CMSShortTest extends CMSTest[Short]
class CMSIntTest extends CMSTest[Int]
class CMSLongTest extends CMSTest[Long]
class CMSBigIntTest extends CMSTest[BigInt]

abstract class CMSTest[K: Ordering: CMSHasher: Numeric] extends WordSpec with Matchers {

  val DELTA = 1E-10
  val EPS = 0.001
  val SEED = 1

  // We use TopPctCMS for testing CMSCounting functionality.  We argue that because TopPctCMS[K] encapsulates CMS[K]
  // and uses it for all its counting/querying functionality (like an adapter) we can test CMS[K] indirectly through
  // testing TopPctCMS[K].
  val COUNTING_CMS_MONOID = {
    val ANY_HEAVY_HITTERS_PCT = 0.1 // heavy hitters functionality is not relevant for the tests using this monoid
    TopPctCMS.monoid[K](EPS, DELTA, SEED, ANY_HEAVY_HITTERS_PCT)
  }

  val RAND = new scala.util.Random

  // Convenience methods to convert from `Int` to the actual `K` type, and we prefer these conversions to be explicit
  // (cf. JavaConverters vs. JavaConversions). We use the name `toK` to clarify the intent and to prevent name conflicts
  // with the existing `to[Col]` method in Scala.
  implicit class IntCast(x: Int) {
    def toK[A: Numeric]: A = implicitly[Numeric[A]].fromInt(x)
  }

  implicit class SeqCast(xs: Seq[Int]) {
    def toK[A: Numeric]: Seq[A] = xs map { _.toK[A] }
  }

  implicit class SetCast(xs: Set[Int]) {
    def toK[A: Numeric]: Set[A] = xs map { _.toK[A] }
  }

  /**
   * Returns the exact frequency of {x} in {data}.
   */
  def exactFrequency(data: Seq[K], x: K): Long = data.count(_ == x)

  /**
   * Returns the exact inner product between two data streams, when the streams
   * are viewed as count vectors.
   */
  def exactInnerProduct(data1: Seq[K], data2: Seq[K]): Long = {
    val counts1 = data1.groupBy(x => x).mapValues(_.size)
    val counts2 = data2.groupBy(x => x).mapValues(_.size)

    (counts1.keys.toSet & counts2.keys.toSet).map { k => counts1(k) * counts2(k) }.sum
  }

  /**
   * Returns the elements in {data} that appear at least heavyHittersPct * data.size times.
   */
  def exactHeavyHitters(data: Seq[K], heavyHittersPct: Double): Set[K] = {
    val counts = data.groupBy(x => x).mapValues(_.size)
    val totalCount = counts.values.sum
    counts.filter { _._2 >= heavyHittersPct * totalCount }.keys.toSet
  }

  "A Count-Min sketch implementing CMSCounting" should {

    "count total number of elements in a stream" in {
      val totalCount = 1243
      val range = 234
      val data = (0 to (totalCount - 1)).map { _ => RAND.nextInt(range) }.toK[K]
      val cms = COUNTING_CMS_MONOID.create(data)

      cms.totalCount should be(totalCount)
    }

    "estimate frequencies" in {
      val totalCount = 5678
      val range = 897
      val data = (0 to (totalCount - 1)).map { _ => RAND.nextInt(range) }.toK[K]
      val cms = COUNTING_CMS_MONOID.create(data)

      (0 to 100).foreach { _ =>
        val x = RAND.nextInt(range).toK[K]
        val exact = exactFrequency(data, x)
        val approx = cms.frequency(x).estimate
        val estimationError = approx - exact
        val maxError = approx - cms.frequency(x).min
        val beWithinTolerance = be >= 0L and be <= maxError

        approx should be >= exact
        estimationError should beWithinTolerance
      }
    }

    "exactly compute frequencies in a small stream" in {
      val one = COUNTING_CMS_MONOID.create(1.toK[K])
      val two = COUNTING_CMS_MONOID.create(2.toK[K])
      val cms = COUNTING_CMS_MONOID.plus(COUNTING_CMS_MONOID.plus(one, two), two)

      cms.frequency(0.toK[K]).estimate should be(0)
      cms.frequency(1.toK[K]).estimate should be(1)
      cms.frequency(2.toK[K]).estimate should be(2)

      val three = COUNTING_CMS_MONOID.create(Seq(1, 1, 1).toK[K])
      three.frequency(1.toK[K]).estimate should be(3)
      val four = COUNTING_CMS_MONOID.create(Seq(1, 1, 1, 1).toK[K])
      four.frequency(1.toK[K]).estimate should be(4)
      val cms2 = COUNTING_CMS_MONOID.plus(four, three)
      cms2.frequency(1.toK[K]).estimate should be(7)
    }

    "estimate inner products" in {
      val totalCount = 5234
      val range = 1390
      val data1 = (0 to (totalCount - 1)).map { _ => RAND.nextInt(range) }.toK[K]
      val data2 = (0 to (totalCount - 1)).map { _ => RAND.nextInt(range) }.toK[K]
      val cms1 = COUNTING_CMS_MONOID.create(data1)
      val cms2 = COUNTING_CMS_MONOID.create(data2)

      val approxA = cms1.innerProduct(cms2)
      val approx = approxA.estimate
      val exact = exactInnerProduct(data1, data2)
      val estimationError = approx - exact
      val maxError = approx - approxA.min
      val beWithinTolerance = be >= 0L and be <= maxError

      approx should be(cms2.innerProduct(cms1).estimate)
      approx should be >= exact
      estimationError should beWithinTolerance
    }

    "exactly compute inner product of small streams" in {
      // Nothing in common.
      val a1 = List(1, 2, 3).toK[K]
      val a2 = List(4, 5, 6).toK[K]
      COUNTING_CMS_MONOID.create(a1).innerProduct(COUNTING_CMS_MONOID.create(a2)).estimate should be(0)

      // One element in common.
      val b1 = List(1, 2, 3).toK[K]
      val b2 = List(3, 5, 6).toK[K]
      COUNTING_CMS_MONOID.create(b1).innerProduct(COUNTING_CMS_MONOID.create(b2)).estimate should be(1)

      // Multiple, non-repeating elements in common.
      val c1 = List(1, 2, 3).toK[K]
      val c2 = List(3, 2, 6).toK[K]
      COUNTING_CMS_MONOID.create(c1).innerProduct(COUNTING_CMS_MONOID.create(c2)).estimate should be(2)

      // Multiple, repeating elements in common.
      val d1 = List(1, 2, 2, 3, 3).toK[K]
      val d2 = List(2, 3, 3, 6).toK[K]
      COUNTING_CMS_MONOID.create(d1).innerProduct(COUNTING_CMS_MONOID.create(d2)).estimate should be(6)
    }

    "work as an Aggregator when created from a single, small stream" in {
      val data1 = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5).toK[K]

      val cms = CMS.aggregator[K](EPS, DELTA, SEED).apply(data1)
      cms.frequency(1.toK[K]).estimate should be(1L)
      cms.frequency(2.toK[K]).estimate should be(2L)
      cms.frequency(3.toK[K]).estimate should be(3L)
      cms.frequency(4.toK[K]).estimate should be(4L)
      cms.frequency(5.toK[K]).estimate should be(5L)

      val topPctCMS = {
        val anyHeavyHittersPct = 0.1 // exact setting not relevant for this test
        TopPctCMS.aggregator[K](EPS, DELTA, SEED, anyHeavyHittersPct).apply(data1)
      }
      topPctCMS.frequency(1.toK[K]).estimate should be(1L)
      topPctCMS.frequency(2.toK[K]).estimate should be(2L)
      topPctCMS.frequency(3.toK[K]).estimate should be(3L)
      topPctCMS.frequency(4.toK[K]).estimate should be(4L)
      topPctCMS.frequency(5.toK[K]).estimate should be(5L)

      val topNCMS = {
        val anyHeavyHittersN = 1 // exact setting not relevant for this test
        TopNCMS.aggregator[K](EPS, DELTA, SEED, anyHeavyHittersN).apply(data1)
      }
      topNCMS.frequency(1.toK[K]).estimate should be(1L)
      topNCMS.frequency(2.toK[K]).estimate should be(2L)
      topNCMS.frequency(3.toK[K]).estimate should be(3L)
      topNCMS.frequency(4.toK[K]).estimate should be(4L)
      topNCMS.frequency(5.toK[K]).estimate should be(5L)
    }

  }

  "A Top-% Count-Min sketch implementing CMSHeavyHitters" should {

    "estimate heavy hitters" in {
      // Simple way of making some elements appear much more often than others.
      val data1 = (1 to 3000).map { _ => RAND.nextInt(3) }.toK[K]
      val data2 = (1 to 3000).map { _ => RAND.nextInt(10) }.toK[K]
      val data3 = (1 to 1450).map { _ => -1 }.toK[K] // element close to being a 20% heavy hitter
      val data = data1 ++ data2 ++ data3

      // Find elements that appear at least 20% of the time.
      val heavyHittersPct = 0.2
      val cms = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.2).create(data)

      val trueHhs = exactHeavyHitters(data, heavyHittersPct)
      val estimatedHhs = cms.heavyHitters

      // All true heavy hitters must be claimed as heavy hitters.
      trueHhs.intersect(estimatedHhs) should be(trueHhs)

      // It should be very unlikely that any element with count less than
      // (heavyHittersPct - eps) * totalCount is claimed as a heavy hitter.
      val minHhCount = (heavyHittersPct - cms.eps) * cms.totalCount
      val infrequent = data.groupBy { x => x }.mapValues { _.size }.filter { _._2 < minHhCount }.keys.toSet
      infrequent.intersect(estimatedHhs) should be('empty)
    }

    "(when adding CMS instances) drop old heavy hitters when new heavy hitters replace them" in {
      val monoid = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.3)
      val cms1 = monoid.create(Seq(1, 2, 2).toK[K])
      cms1.heavyHitters should be(Set(1, 2))

      val cms2 = cms1 ++ monoid.create(2.toK[K])
      cms2.heavyHitters should be(Set(2))

      val cms3 = cms2 ++ monoid.create(1.toK[K])
      cms3.heavyHitters should be(Set(1, 2))

      val cms4 = cms3 ++ monoid.create(Seq(0, 0, 0, 0, 0, 0).toK[K])
      cms4.heavyHitters should be(Set(0))
    }

    "(when adding individual items) drop old heavy hitters when new heavy hitters replace them" in {
      val monoid = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.3)
      val cms1 = monoid.create(Seq(1, 2, 2).toK[K])
      cms1.heavyHitters should be(Set(1, 2))

      val cms2 = cms1 + 2.toK[K]
      cms2.heavyHitters should be(Set(2))

      val cms3 = cms2 + 1.toK[K]
      cms3.heavyHitters should be(Set(1, 2))

      val heaviest = 0.toK[K]
      val cms4 = cms3 + heaviest + heaviest + heaviest + heaviest + heaviest + heaviest
      cms4.heavyHitters should be(Set(heaviest))
    }

    "(when adding CMS instances) merge heavy hitters correctly [GH-353 regression test]" in {
      // See https://github.com/twitter/algebird/issues/353
      val monoid = TopPctCMS.monoid(EPS, DELTA, SEED, 0.1)

      val data1 = Seq(1, 1, 1, 2, 2, 3).toK[K]
      val data2 = Seq(3, 4, 4, 4, 5, 5).toK[K]
      val data3 = Seq(3, 6, 6, 6, 7, 7).toK[K]
      val data4 = Seq(3, 8, 8, 8, 9, 9).toK[K]
      val singleData = data1 ++ data2 ++ data3 ++ data4

      /*
        Data sets from above shown in tabular view

        Item    1   2   3   4   total (= singleData)
        ----------------------------------------
        A (1)   3   -   -   -   3
        B (2)   2   -   -   -   2
        C (3)   1   1   1   1   4  <<< C is global top 1 heavy hitter
        D (4)   -   3   -   -   3
        E (5)   -   2   -   -   2
        F (6)   -   -   3   -   3
        G (7)   -   -   2   -   2
        H (8)   -   -   -   3   3
        I (9)   -   -   -   2   2

       */

      val cms1 = monoid.create(data1)
      val cms2 = monoid.create(data2)
      val cms3 = monoid.create(data3)
      val cms4 = monoid.create(data4)
      val aggregated = cms1 ++ cms2 ++ cms3 ++ cms4

      val single = monoid.create(singleData)
      aggregated.heavyHitters should be(single.heavyHitters)
      aggregated.heavyHitters contains (3.toK[K]) // C=3 is global top 1 heavy hitter
    }

    "exactly compute heavy hitters when created from a single, small stream" in {
      val data1 = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5).toK[K]

      val cms1 = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.01).create(data1)
      cms1.heavyHitters should be(Set(1, 2, 3, 4, 5))

      val cms2 = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.1).create(data1)
      cms2.heavyHitters should be(Set(2, 3, 4, 5))

      val cms3 = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.3).create(data1)
      cms3.heavyHitters should be(Set(5))

      val cms4 = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.9).create(data1)
      cms4.heavyHitters should be(Set[K]())
    }

    "work as an Aggregator when created from a single, small stream" in {
      val data1 = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5).toK[K]

      val cms1 = TopPctCMS.aggregator[K](EPS, DELTA, SEED, 0.01).apply(data1)
      cms1.heavyHitters should be(Set(1, 2, 3, 4, 5))

      val cms2 = TopPctCMS.aggregator[K](EPS, DELTA, SEED, 0.1).apply(data1)
      cms2.heavyHitters should be(Set(2, 3, 4, 5))

      val cms3 = TopPctCMS.aggregator[K](EPS, DELTA, SEED, 0.3).apply(data1)
      cms3.heavyHitters should be(Set(5))

      val cms4 = TopPctCMS.aggregator[K](EPS, DELTA, SEED, 0.9).apply(data1)
      cms4.heavyHitters should be(Set[K]())
    }

  }

  "A Top-N Count-Min sketch implementing CMSHeavyHitters" should {

    // Note: As described in https://github.com/twitter/algebird/issues/353, a top-N CMS is, in general, not able to
    // merge heavy hitters correctly.  This is because merging top-N based heavy hitters is not an associative
    // operation.

    // This test involves merging of top-N CMS instances, which is not an associative operation.  This means that the
    // success or failure of this test depends on the merging order and/or the test data characteristics.
    "(when adding CMS instances) drop old heavy hitters when new heavy hitters replace them, if merge order matches data" in {
      val heavyHittersN = 2
      val monoid = TopNCMS.monoid[K](EPS, DELTA, SEED, heavyHittersN)
      val cms1 = monoid.create(Seq(1, 2, 2).toK[K])
      cms1.heavyHitters should be(Set(1, 2))
      val cms2 = cms1 ++ monoid.create(Seq(3, 3, 3).toK[K])
      cms2.heavyHitters should be(Set(2, 3))
      val cms3 = cms2 ++ monoid.create(Seq(1, 1, 1).toK[K])
      cms3.heavyHitters should be(Set(3, 1))
      val cms4 = cms3 ++ monoid.create(Seq(6, 6, 6, 6, 6, 6).toK[K])
      cms4.heavyHitters should be(Set(1, 6))
    }

    "(when adding individual items) drop old heavy hitters when new heavy hitters replace them" in {
      val monoid = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.3)
      val cms1 = monoid.create(Seq(1, 2, 2).toK[K])
      cms1.heavyHitters should be(Set(1, 2))

      val cms2 = cms1 + 2.toK[K]
      cms2.heavyHitters should be(Set(2))

      val cms3 = cms2 + 1.toK[K]
      cms3.heavyHitters should be(Set(1, 2))

      val heaviest = 0.toK[K]
      val cms4 = cms3 + heaviest + heaviest + heaviest + heaviest + heaviest + heaviest
      cms4.heavyHitters should be(Set(heaviest))
    }

    // This test documents the order bias of top-N CMS, i.e. it's a negative test case.
    // See https://github.com/twitter/algebird/issues/353
    "(when adding CMS instances) generally FAIL to merge heavy hitters correctly due to order bias" in {
      val topN = 2
      val monoid = TopNCMS.monoid(EPS, DELTA, SEED, topN)

      val data1 = Seq(1, 1, 1, 2, 2, 3).toK[K]
      val data2 = Seq(3, 4, 4, 4, 5, 5).toK[K]
      val data3 = Seq(3, 6, 6, 6, 7, 7).toK[K]
      val data4 = Seq(3, 8, 8, 8, 9, 9).toK[K]
      val singleData = data1 ++ data2 ++ data3 ++ data4

      /*
        Data sets from above shown in tabular view

        Item    1   2   3   4   total (= singleData)
        ----------------------------------------
        A (1)   3   -   -   -   3
        B (2)   2   -   -   -   2
        C (3)   1   1   1   1   4  <<< C is global top 1 heavy hitter
        D (4)   -   3   -   -   3
        E (5)   -   2   -   -   2
        F (6)   -   -   3   -   3
        G (7)   -   -   2   -   2
        H (8)   -   -   -   3   3
        I (9)   -   -   -   2   2

       */

      val cms1 = monoid.create(data1)
      val cms2 = monoid.create(data2)
      val cms3 = monoid.create(data3)
      val cms4 = monoid.create(data4)
      val aggregated = cms1 ++ cms2 ++ cms3 ++ cms4

      val single = monoid.create(singleData)
      aggregated.heavyHitters shouldNot be(single.heavyHitters)
      aggregated.heavyHitters shouldNot contain(3.toK[K]) // C=3 is global top 1 heavy hitter
    }

    // Compared to adding top-N CMS instances, which is generally unsafe because of order bias (see test cases above),
    // adding individual items to a top-N CMS is a safe operation.
    // See https://github.com/twitter/algebird/issues/353
    "(when adding individual items) merge heavy hitters correctly [GH-353 regression test]" in {
      val topN = 2
      val monoid = TopNCMS.monoid(EPS, DELTA, SEED, topN)

      val data1 = Seq(1, 1, 1, 2, 2, 3).toK[K]
      val data2 = Seq(3, 4, 4, 4, 5, 5).toK[K]
      val data3 = Seq(3, 6, 6, 6, 7, 7).toK[K]
      val data4 = Seq(3, 8, 8, 8, 9, 9).toK[K]
      val singleData = data1 ++ data2 ++ data3 ++ data4

      /*
        Data sets from above shown in tabular view

        Item    1   2   3   4   total (= singleData)
        ----------------------------------------
        A (1)   3   -   -   -   3
        B (2)   2   -   -   -   2
        C (3)   1   1   1   1   4  <<< C is global top 1 heavy hitter
        D (4)   -   3   -   -   3
        E (5)   -   2   -   -   2
        F (6)   -   -   3   -   3
        G (7)   -   -   2   -   2
        H (8)   -   -   -   3   3
        I (9)   -   -   -   2   2

       */

      val cms1 = monoid.create(data1)
      val cms2 = cms1 + 3.toK[K] + 4.toK[K] + 4.toK[K] + 4.toK[K] + 5.toK[K] + 5.toK[K] // effectively "++ data2"
      val cms3 = cms2 + 3.toK[K] + 6.toK[K] + 6.toK[K] + 6.toK[K] + 7.toK[K] + 7.toK[K] // "++ data3"
      val aggregated = cms3 + 3.toK[K] + 8.toK[K] + 8.toK[K] + 8.toK[K] + 9.toK[K] + 9.toK[K] // "++ data4"

      val single = monoid.create(singleData)
      aggregated.heavyHitters should be(single.heavyHitters)
      aggregated.heavyHitters should contain(3.toK[K]) // C=3 is global top 1 heavy hitter
    }

    "exactly compute heavy hitters when created a from single, small stream" in {
      val data1 = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5).toK[K]

      val cms1 = TopNCMS.monoid[K](EPS, DELTA, SEED, 5).create(data1)
      cms1.heavyHitters should be(Set(1, 2, 3, 4, 5))

      val cms2 = TopNCMS.monoid[K](EPS, DELTA, SEED, 4).create(data1)
      cms2.heavyHitters should be(Set(2, 3, 4, 5))

      val cms3 = TopNCMS.monoid[K](EPS, DELTA, SEED, 3).create(data1)
      cms3.heavyHitters should be(Set(3, 4, 5))

      val cms4 = TopNCMS.monoid[K](EPS, DELTA, SEED, 2).create(data1)
      cms4.heavyHitters should be(Set(4, 5))

      val cms5 = TopNCMS.monoid[K](EPS, DELTA, SEED, 1).create(data1)
      cms5.heavyHitters should be(Set(5))
    }

    "work as an Aggregator when created from a single, small stream" in {
      val data1 = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5).toK[K]

      val cms1 = TopNCMS.aggregator[K](EPS, DELTA, SEED, 5).apply(data1)
      cms1.heavyHitters should be(Set(1, 2, 3, 4, 5))

      val cms2 = TopNCMS.aggregator[K](EPS, DELTA, SEED, 4).apply(data1)
      cms2.heavyHitters should be(Set(2, 3, 4, 5))

      val cms3 = TopNCMS.aggregator[K](EPS, DELTA, SEED, 3).apply(data1)
      cms3.heavyHitters should be(Set(3, 4, 5))

      val cms4 = TopNCMS.aggregator[K](EPS, DELTA, SEED, 2).apply(data1)
      cms4.heavyHitters should be(Set(4, 5))

      val cms5 = TopNCMS.aggregator[K](EPS, DELTA, SEED, 1).apply(data1)
      cms5.heavyHitters should be(Set(5))
    }

  }

}

class CMSFunctionsSpec extends PropSpec with PropertyChecks with Matchers {

  property("roundtrips width->eps->width") {
    forAll { (i: Int) =>
      whenever(i > 0) {
        CMSFunctions.width(CMSFunctions.eps(i)) should be(i)
      }
    }
  }

  property("roundtrips depth->delta->depth for common depth values") {
    // For all i > 709 this test break because of precision limits:  For all i > 709 will return 0.0, which is not the
    // mathematically correct value but rather the asymptote of delta.
    val maxI = 709
    forAll(Gen.choose(0, maxI)) { (i: Int) =>
      CMSFunctions.depth(CMSFunctions.delta(i)) should be(i)
    }
  }

  // Documents a precision error that is exposed by all depths > 709.
  // For all i > 709, CMSFunctions.delta(i) will return 0.0, which is not the mathematically correct value but rather
  // the asymptote of the delta function.
  property("throw IAE when deriving delta from invalid depth values") {
    val maxValidDelta = 709
    forAll(Gen.choose(maxValidDelta + 1, 10000)) { (invalidDepth: Int) =>
      val exception = intercept[IllegalArgumentException] {
        CMSFunctions.delta(invalidDepth)
      }
      exception.getMessage should fullyMatch regex
        """requirement failed: depth must be smaller as it causes precision errors when computing delta \(\d+ led to an invalid delta of 0.0\)"""
    }
  }

  property("throw IAE when deriving depth from invalid delta values") {
    val invalidDeltas = Table("invalidDelta", 0.0, 1E-330, 1E-400)
    forAll(invalidDeltas) { (invalidDelta: Double) =>
      val exception = intercept[IllegalArgumentException] {
        CMSFunctions.depth(invalidDelta)
      }
      exception.getMessage should be("requirement failed: delta must lie in (0, 1)")
    }
  }

}

class CMSParamsSpec extends PropSpec with PropertyChecks with Matchers {

  val AnyEps = 0.001
  val AnyDelta = 1E-5
  val AnyHashes = {
    val AnySeed = 1
    CMSFunctions.generateHashes[Long](AnyEps, AnyDelta, AnySeed)
  }

  property("throw IAE for invalid eps values") {
    val invalidEpsilons = Table("invalidEps", 0.0, 1.0, 2.0, 100.0)
    forAll(invalidEpsilons) { (invalidEps: Double) =>
      val exception = intercept[IllegalArgumentException] {
        CMSParams(AnyHashes, invalidEps, AnyDelta)
      }
      exception.getMessage should be("requirement failed: eps must lie in (0, 1)")
    }
  }

  property("throw IAE for invalid delta values") {
    val invalidDeltas = Table("invalidDelta", 0.0, 1.0, 2.0, 100.0, 1E-330, 1E-400)
    forAll(invalidDeltas) { (invalidDelta: Double) =>
      val exception = intercept[IllegalArgumentException] {
        CMSParams(AnyHashes, AnyEps, invalidDelta)
      }
      exception.getMessage should be("requirement failed: delta must lie in (0, 1)")
    }
  }

  property("throw IAE when we do not have enough hashes") {
    val tooFewHashes = Seq.empty[CMSHash[Long]]
    val exception = intercept[IllegalArgumentException] {
      CMSParams(tooFewHashes, AnyEps, AnyDelta)
    }
    exception.getMessage should fullyMatch regex """requirement failed: we require at least (\d+) hash functions"""
  }

}

/**
 * This spec verifies that we provide legacy types for the CMS and CountMinSketchMonoid classes we had in Algebird
 * versions < 0.8.1.  Note that this spec is not meant to verify their actual functionality.
 */
class LegacyCMSSpec extends WordSpec with Matchers {

  import legacy.CountMinSketchMonoid

  val DELTA = 1E-10
  val EPS = 0.001
  val SEED = 1

  val CMS_MONOID: CountMinSketchMonoid = CountMinSketchMonoid(EPS, DELTA, SEED)

  "The legacy package" should {

    "provide a legacy type for the CMS implementation in Algebird versions < 0.8.1" in {
      val cms: legacy.CMS = CMS_MONOID.create(Seq(0L, 0L))
      cms.frequency(0L).estimate should be (2)
      cms.heavyHitters should be(Set(0L))
    }

    "provide a legacy type for the CMS monoid implementation in Algebird versions < 0.8.1" in {
      val cmsMonoid: CountMinSketchMonoid = {
        val eps = 0.001
        val delta = 1E-5
        val seed = 1
        val heavyHittersPct = 0.1
        CountMinSketchMonoid(eps, delta, seed, heavyHittersPct)
      }
      val cms = cmsMonoid.create(Seq(0L, 0L))
      cms.frequency(0L).estimate should be (2)
      cms.heavyHitters should be(Set(0L))
    }

  }

}