package com.twitter.algebird

import org.scalatest.{ PropSpec, Matchers, WordSpec }
import org.scalatest.prop.{ GeneratorDrivenPropertyChecks, PropertyChecks }
import org.scalatest.prop.Checkers.check
import org.scalacheck.{ Gen, Arbitrary, Properties }

import CmsTestImplicits._

import scala.util.Random
import CMSHasherImplicits.CMSHasherBigInt

class CmsLaws extends PropSpec with PropertyChecks with Matchers {

  import BaseProperties._

  val DELTA = 1E-8
  val EPS = 0.005
  val SEED = 1

  private def createArbitrary[K: FromIntLike](cmsMonoid: CMSMonoid[K]): Arbitrary[CMS[K]] = {
    val k = implicitly[FromIntLike[K]]
    Arbitrary {
      for (v <- Gen.choose(0, 10000)) yield cmsMonoid.create(k.fromInt(v))
    }
  }

  implicit def cmsEquiv[K]: Equiv[CMS[K]] =
    new Equiv[CMS[K]] {
      def equiv(x: CMS[K], y: CMS[K]): Boolean = {
        val d = CMSInstance(x.params)
        (d ++ x) == (d ++ y)
      }
    }

  property("CountMinSketch[Short] is a Monoid") {
    implicit val cmsMonoid = CMS.monoid[Short](EPS, DELTA, SEED)
    implicit val cmsGen = createArbitrary[Short](cmsMonoid)
    check(monoidLawsEquiv[CMS[Short]])
  }

  property("CountMinSketch[Int] is a Monoid") {
    implicit val cmsMonoid = CMS.monoid[Int](EPS, DELTA, SEED)
    implicit val cmsGen = createArbitrary[Int](cmsMonoid)
    check(monoidLawsEquiv[CMS[Int]])
  }

  property("CountMinSketch[Long] is a Monoid") {
    implicit val cmsMonoid = CMS.monoid[Long](EPS, DELTA, SEED)
    implicit val cmsGen = createArbitrary[Long](cmsMonoid)
    check(monoidLawsEquiv[CMS[Long]])
  }

  property("CountMinSketch[BigInt] is a Monoid") {
    implicit val cmsMonoid = CMS.monoid[BigInt](EPS, DELTA, SEED)
    implicit val cmsGen = createArbitrary[BigInt](cmsMonoid)
    check(monoidLawsEquiv[CMS[BigInt]])
  }

  property("CountMinSketch[BigDecimal] is a Monoid") {
    implicit val cmsMonoid = CMS.monoid[BigDecimal](EPS, DELTA, SEED)
    implicit val cmsGen = createArbitrary[BigDecimal](cmsMonoid)
    check(monoidLawsEquiv[CMS[BigDecimal]])
  }

  property("CountMinSketch[String] is a Monoid") {
    implicit val cmsMonoid = CMS.monoid[String](EPS, DELTA, SEED)
    implicit val cmsGen = createArbitrary[String](cmsMonoid)
    check(monoidLawsEquiv[CMS[String]])
  }

  property("CountMinSketch[Bytes] is a Monoid") {
    implicit val cmsMonoid = CMS.monoid[Bytes](EPS, DELTA, SEED)
    implicit val cmsGen = createArbitrary[Bytes](cmsMonoid)
    check(monoidLawsEquiv[CMS[Bytes]])
  }

}

class TopPctCmsLaws extends PropSpec with PropertyChecks with Matchers {

  import BaseProperties._

  val DELTA = 1E-8
  val EPS = 0.005
  val SEED = 1
  val HEAVY_HITTERS_PCT = 0.1

  private def createArbitrary[K: FromIntLike](cmsMonoid: TopPctCMSMonoid[K]): Arbitrary[TopCMS[K]] = {
    val k = implicitly[FromIntLike[K]]
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

  property("TopPctCms[BigDecimal] is a Monoid") {
    implicit val cmsMonoid = TopPctCMS.monoid[BigDecimal](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)
    implicit val cmsGen = createArbitrary[BigDecimal](cmsMonoid)
    monoidLaws[TopCMS[BigDecimal]]
  }

  property("TopPctCms[String] is a Monoid") {
    implicit val cmsMonoid = TopPctCMS.monoid[String](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)
    implicit val cmsGen = createArbitrary[String](cmsMonoid)
    monoidLaws[TopCMS[String]]
  }

  property("TopPctCms[Bytes] is a Monoid") {
    implicit val cmsMonoid = TopPctCMS.monoid[Bytes](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)
    implicit val cmsGen = createArbitrary[Bytes](cmsMonoid)
    monoidLaws[TopCMS[Bytes]]
  }

}

class SparseCMSTest extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {

  import BaseProperties._

  val DELTA = 1E-8
  val EPS = 0.005
  val SEED = 1

  "correctly count SparseCMS numbers" in {
    val cmsMonoid = CMS.monoid[Int](EPS, DELTA, SEED)
    val cmsZero = cmsMonoid.zero
    val cms1 = cmsZero + (1, 100) + (1, 200)
    cms1.frequency(1).estimate should be(300)
    val cms2 = cmsZero + (1, 100) + (2, 100)
    val cms3 = cms1 ++ cms2
    cms3.frequency(1).estimate should be(400)
  }
}

class CMSInstanceTest extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {

  import BaseProperties._

  val DELTA = 1E-8
  val EPS = 0.005
  val SEED = 1

  "correctly count CMSItem numbers" in {
    val cmsMonoid = CMS.monoid[Int](EPS, DELTA, SEED)
    val cmsZero = cmsMonoid.zero
    val cms = cmsZero + (1, 100) + (2, 200)
    cms.frequency(1).estimate should be(100)
    cms.frequency(2).estimate should be(200)
  }
}

/**
 * Verifies contramap functionality, which allows us to translate `CMSHasher[K]` into `CMSHasher[L]`, given `f: L => K`.
 */
class CMSContraMapSpec extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {

  "translates CMSHasher[K] into CMSHasher[L], given a function f: L => K" in {
    // Given a "source" CMSHasher[K]
    val sourceHasher: CMSHasher[String] = CMSHasher.CMSHasherString
    // and a translation function from an unsupported type L (here: Seq[Byte]) to K
    def f(bytes: Seq[Byte]): String = new String(bytes.toArray[Byte], "UTF-8")

    // When we run contramap on a CMSHasher[K] supplying f,
    // then the result should be a CMSHasher[L]...
    val targetHasher: CMSHasher[Seq[Byte]] = sourceHasher.contramap((d: Seq[Byte]) => f(d))
    targetHasher shouldBe an[CMSHasher[_]] // Can't test CMSHasher[Seq[Byte]] specifically because of type erasure.

    // ...and hashing should work correctly (this is only a smoke test).
    val a = 4
    val b = 0
    val width = 1234
    val x = Array(113.toByte).toSeq // same as Seq(133.toByte)
    val result = targetHasher.hash(a, b, width)(x)
    val expected = sourceHasher.hash(a, b, width)("q")
    result should be(expected)
    result should be(434)
  }

  "supports, via contramap, creating CMS monoids for such types K that are not supported out of the box" in {
    // Given a "source" CMSHasher[K] which is supported out of the box
    val sourceHasher: CMSHasher[String] = CMSHasher.CMSHasherString
    // and a translation function from an unsupported type L (here: Seq[Byte]) to K
    def f(bytes: Seq[Byte]): String = new String(bytes.toArray[Byte], "UTF-8")

    // When we create a monoid for L
    implicit val hasherBytes = sourceHasher.contramap((d: Seq[Byte]) => f(d))
    val bytesMonoid: CMSMonoid[Seq[Byte]] = {
      val anyDelta = 1E-10
      val anyEps = 0.001
      val anySeed = 1
      CMS.monoid[Seq[Byte]](anyEps, anyDelta, anySeed)
    }

    // Then we should be able to create CMS instances for L.
    // To verify this we perform a basic smoke test of the resulting monoids.  This test mimics the "exactly compute
    // frequencies in a small stream" scenario in the full-fledged CMS spec.
    val oneKey = Seq(1).toK[Byte]
    val one = bytesMonoid.create(oneKey)
    one.frequency(oneKey).estimate should be(1)
    one.frequency(Seq(2).toK[Byte]).estimate should be(0)
    val two = bytesMonoid.create(Seq(2).toK[Byte])
    two.frequency(oneKey).estimate should be(0)
    two.frequency(Seq(2).toK[Byte]).estimate should be(1)
    val cms = bytesMonoid.plus(bytesMonoid.plus(one, two), two)

    cms.frequency(Seq(0).toK[Byte]).estimate should be(0)
    cms.frequency(oneKey).estimate should be(1)
    cms.frequency(Seq(2).toK[Byte]).estimate should be(2)

    val three = bytesMonoid.create(Seq(oneKey, oneKey, oneKey))
    three.frequency(oneKey).estimate should be(3)
    val four = bytesMonoid.create(Seq(oneKey, oneKey, oneKey, oneKey))
    four.frequency(oneKey).estimate should be(4)
    val cms2 = bytesMonoid.plus(four, three)
    cms2.frequency(oneKey).estimate should be(7)
  }

  "supports, via contramap, creating TopPctCMS monoids for such types K that are not supported out of the box" in {
    // Given a "source" CMSHasher[K] which is supported out of the box
    val sourceHasher: CMSHasher[String] = CMSHasher.CMSHasherString
    // and a translation function from an unsupported type L (here: Seq[Byte]) to K
    def f(bytes: Seq[Byte]): String = new String(bytes.toArray[Byte], "UTF-8")

    // When we create a monoid for L (here: Seq[Byte])
    implicit val hasherBytes = sourceHasher.contramap((d: Seq[Byte]) => f(d))

    // Then we should be able to create TopPctCMS instances for L.
    // To verify this we perform a basic smoke test of the resulting monoids.  This test mimics the "exactly compute
    // heavy hitters when created from a single, small stream" scenario in the full-fledged TopPctCMS spec.
    val oneKey = Seq(1).toK[Byte]
    val twoKey = Seq(2).toK[Byte]
    val threeKey = Seq(3).toK[Byte]
    val fourKey = Seq(4).toK[Byte]
    val fiveKey = Seq(5).toK[Byte]
    val data1: Seq[Seq[Byte]] = Seq(oneKey, twoKey, twoKey, threeKey, threeKey, threeKey,
      fourKey, fourKey, fourKey, fourKey,
      fiveKey, fiveKey, fiveKey, fiveKey, fiveKey)
    val minWidth = data1.distinct.size

    forAll(
      (Gen.choose(1, 70), "depth"),
      (Gen.choose(minWidth, 1000), "width"),
      (Gen.choose(Int.MinValue, Int.MaxValue), "seed")) { (depth: Int, width: Int, seed: Int) =>
        val cms1: TopCMS[Seq[Byte]] = TopPctCMS.monoid[Seq[Byte]](depth, width, seed, 0.01).create(data1)
        cms1.heavyHitters should be(Set(oneKey, twoKey, threeKey, fourKey, fiveKey))

        val cms2: TopCMS[Seq[Byte]] = TopPctCMS.monoid[Seq[Byte]](depth, width, seed, 0.1).create(data1)
        cms2.heavyHitters should be(Set(twoKey, threeKey, fourKey, fiveKey))

        val cms3: TopCMS[Seq[Byte]] = TopPctCMS.monoid[Seq[Byte]](depth, width, seed, 0.3).create(data1)
        cms3.heavyHitters should be(Set(fiveKey))

        val cms4: TopCMS[Seq[Byte]] = TopPctCMS.monoid[Seq[Byte]](depth, width, seed, 0.9).create(data1)
        cms4.heavyHitters should be(Set.empty[Seq[Byte]])
      }
  }

}

class CMSShortTest extends CMSTest[Short]
class CMSIntTest extends CMSTest[Int]
class CMSLongTest extends CMSTest[Long]
class CMSBigIntTest extends CMSTest[BigInt]
class CMSBigDecimalTest extends CMSTest[BigDecimal]
class CMSStringTest extends CMSTest[String]
class CMSBytesTest extends CMSTest[Bytes]

abstract class CmsProperty[K] extends ApproximateProperty

object CmsProperty {
  val delta = 1E-10
  val eps = 0.001
  val seed = 1

  def makeApproximate[K: CMSHasher](exact: Vector[K]) = {
    val cmsMonoid: CMSMonoid[K] = CMS.monoid(eps, delta, seed)
    cmsMonoid.sum(exact.map(cmsMonoid.create(_)))
  }
}

abstract class CmsFrequencyProperty[K: CMSHasher: Gen] extends CmsProperty {
  type Exact = Vector[K]
  type Approx = CMS[K]

  type Input = K
  type Result = Long

  def makeApproximate(e: Exact) = CmsProperty.makeApproximate(e)
  def inputGenerator(e: Vector[K]): Gen[K] = Gen.oneOf(e)

  def exactResult(vec: Vector[K], key: K) = vec.count(_ == key)
  def approximateResult(cms: CMS[K], key: K) = cms.frequency(key)
}

class CmsSmallFrequencyProperty[K: CMSHasher: Gen] extends CmsFrequencyProperty[K] {
  def exactGenerator: Gen[Vector[K]] = Gen.containerOf[Vector, K](implicitly[Gen[K]])
}

class CmsLargeFrequencyProperty[K: CMSHasher: Gen] extends CmsFrequencyProperty[K] {
  def exactGenerator: Gen[Vector[K]] = Gen.containerOfN[Vector, K](100000, implicitly[Gen[K]])
}

class CmsInnerProductProperty[K: CMSHasher: Gen] extends CmsProperty[K] {
  type Exact = (Vector[K], Vector[K])
  type Approx = (CMS[K], CMS[K])

  type Input = Unit
  type Result = Long

  def makeApproximate(exacts: (Vector[K], Vector[K])) =
    (CmsProperty.makeApproximate(exacts._1), CmsProperty.makeApproximate(exacts._2))

  def exactGenerator: Gen[(Vector[K], Vector[K])] =
    for {
      list1 <- Gen.containerOfN[Vector, K](10000, implicitly[Gen[K]])
      list2 <- Gen.containerOfN[Vector, K](10000, implicitly[Gen[K]])
    } yield (list1, list2)

  def inputGenerator(e: (Vector[K], Vector[K])): Gen[Unit] = Gen.const(())

  def exactResult(lists: (Vector[K], Vector[K]), input: Unit) = {
    val counts1 = lists._1.groupBy(identity).mapValues(_.size)
    val counts2 = lists._2.groupBy(identity).mapValues(_.size)
    (counts1.keys.toSet & counts2.keys.toSet).toSeq.map { k => counts1(k) * counts2(k) }.sum
  }

  def approximateResult(cmses: (CMS[K], CMS[K]), input: Unit) = cmses._1.innerProduct(cmses._2)
}

class CmsTotalCountProperty[K: CMSHasher: Gen] extends CmsProperty[K] {
  type Exact = Vector[K]
  type Approx = CMS[K]

  type Input = Unit
  type Result = Long

  def makeApproximate(exact: Vector[K]) = CmsProperty.makeApproximate(exact)

  def exactGenerator: Gen[Vector[K]] = Gen.containerOfN[Vector, K](10000, implicitly[Gen[K]])

  def inputGenerator(e: Vector[K]): Gen[Unit] = Gen.const(())

  def exactResult(list: Vector[K], input: Unit) = list.length

  def approximateResult(cms: CMS[K], input: Unit) = Approximate.exact(cms.totalCount)
}

class CmsProperties extends ApproximateProperties("CountMinSketch") {
  import ApproximateProperty.toProp

  implicit val intGen = Gen.choose(1, 100)

  property("CMS works for small lists") = toProp(new CmsSmallFrequencyProperty[Int](), 10, 10, 0.01)
  property("CMS works for large lists") = toProp(new CmsLargeFrequencyProperty[Int](), 10, 10, 0.01)
  property("CMS inner product works") = toProp(new CmsInnerProductProperty[Int](), 10, 10, 0.01)
  property("CMS counts total count") = toProp(new CmsTotalCountProperty[Int](), 10, 10, 0.01)
}

abstract class CMSTest[K: CMSHasher: FromIntLike] extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {

  val DELTA = 1E-10
  val EPS = 0.001
  val SEED = 1

  private[this] val maxDepth = 70
  private[this] val maxWidth = 1000

  // We use TopPctCMS for testing CMSCounting functionality.  We argue that because TopPctCMS[K] encapsulates CMS[K]
  // and uses it for all its counting/querying functionality (like an adapter) we can test CMS[K] indirectly through
  // testing TopPctCMS[K].
  val COUNTING_CMS_MONOID = {
    val ANY_HEAVY_HITTERS_PCT = 0.1 // heavy hitters functionality is not relevant for the tests using this monoid
    TopPctCMS.monoid[K](EPS, DELTA, SEED, ANY_HEAVY_HITTERS_PCT)
  }

  val RAND = new scala.util.Random

  /**
   * Returns the elements in {data} that appear at least heavyHittersPct * data.size times.
   */
  def exactHeavyHitters(data: Seq[K], heavyHittersPct: Double): Set[K] = {
    val counts = data.groupBy(x => x).mapValues(_.size)
    val totalCount = counts.values.sum
    counts.filter { _._2 >= heavyHittersPct * totalCount }.keys.toSet
  }

  /**
   * Creates a random data stream.
   *
   * @param size Number of stream elements.
   * @param range Elements are randomly drawn from [0, range).
   * @return
   */
  def createRandomStream(size: Int, range: Int, rnd: Random = RAND): Seq[K] = {
    require(size > 0)
    (1 to size).map { _ => rnd.nextInt(range).toK[K] }
  }

  "A Count-Min sketch implementing CMSCounting" should {

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

    "create correct sketches out of a single item" in {
      forAll{ (x: Int) =>
        val data = x.toK[K]
        val cmsMonoid = {
          val anyHeavyHittersPct = 0.1 // exact setting not relevant for this test
          TopPctCMS.monoid[K](EPS, DELTA, SEED, anyHeavyHittersPct)
        }
        val topCms = cmsMonoid.create(data)
        topCms.totalCount should be(1)
        topCms.cms.totalCount should be(1)
        topCms.frequency(x.toK[K]).estimate should be(1)
        // Poor man's way to come up with an item that is not x and that is very unlikely to hash to the same slot.
        val otherItem = x + 1
        topCms.frequency(otherItem.toK[K]).estimate should be(0)
        // The following assert indirectly verifies whether the counting table is not all-zero (cf. GH-393).
        topCms.innerProduct(topCms).estimate should be(1)
      }
    }

    "create correct sketches out of a single-item stream" in {
      forAll{ (x: Int) =>
        val data = Seq(x).toK[K]
        val cmsMonoid = {
          val anyHeavyHittersPct = 0.1 // exact setting not relevant for this test
          TopPctCMS.monoid[K](EPS, DELTA, SEED, anyHeavyHittersPct)
        }
        val topCms = cmsMonoid.create(data)
        topCms.totalCount should be(1)
        topCms.cms.totalCount should be(1)
        topCms.frequency(x.toK[K]).estimate should be(1)
        // Poor man's way to come up with an item that is not x and that is very unlikely to hash to the same slot.
        val otherItem = x + 1
        topCms.frequency(otherItem.toK[K]).estimate should be(0)
        // The following assert indirectly verifies whether the counting table is not all-zero (cf. GH-393).
        topCms.innerProduct(topCms).estimate should be(1)
      }
    }

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
      cms1.heavyHitters should be(Set(1, 2).toK[K])

      val cms2 = cms1 ++ monoid.create(2.toK[K])
      cms2.heavyHitters should be(Set(2).toK[K])

      val cms3 = cms2 ++ monoid.create(1.toK[K])
      cms3.heavyHitters should be(Set(1, 2).toK[K])

      val cms4 = cms3 ++ monoid.create(Seq(0, 0, 0, 0, 0, 0).toK[K])
      cms4.heavyHitters should be(Set(0).toK[K])
    }

    "(when adding individual items) drop old heavy hitters when new heavy hitters replace them" in {
      val monoid = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.3)
      val cms1 = monoid.create(Seq(1, 2, 2).toK[K])
      cms1.heavyHitters should be(Set(1, 2).toK[K])

      val cms2 = cms1 + 2.toK[K]
      cms2.heavyHitters should be(Set(2).toK[K])

      val cms3 = cms2 + 1.toK[K]
      cms3.heavyHitters should be(Set(1, 2).toK[K])

      val heaviest = 0.toK[K]
      val cms4 = cms3 + heaviest + heaviest + heaviest + heaviest + heaviest + heaviest
      cms4.heavyHitters should be(Set(heaviest))
    }

    "(when adding CMS instances) merge heavy hitters correctly [GH-353 regression test]" in {
      // See https://github.com/twitter/algebird/issues/353
      val monoid = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.1)

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

      val minDepth = 2 // Use 2 to be on the safe side in case we happen to run into hash collisions
      val minWidth = data1.distinct.size

      forAll(
        (Gen.choose(minDepth, maxDepth), "depth"),
        (Gen.choose(minWidth, maxWidth), "width"),
        (Gen.choose(Int.MinValue, Int.MaxValue), "seed")) { (depth: Int, width: Int, seed: Int) =>
          val cms1 = TopPctCMS.monoid[K](depth, width, seed, 0.01).create(data1)
          cms1.heavyHitters should be(Set(1, 2, 3, 4, 5).toK[K])

          val cms2 = TopPctCMS.monoid[K](depth, width, seed, 0.1).create(data1)
          cms2.heavyHitters should be(Set(2, 3, 4, 5).toK[K])

          val cms3 = TopPctCMS.monoid[K](depth, width, seed, 0.3).create(data1)
          cms3.heavyHitters should be(Set(5).toK[K])

          val cms4 = TopPctCMS.monoid[K](depth, width, seed, 0.9).create(data1)
          cms4.heavyHitters should be(Set[K]())
        }
    }

    "work as an Aggregator when created from a single, small stream" in {
      val data1 = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5).toK[K]

      val minDepth = 2 // Use 2 to be on the safe side in case we happen to run into hash collisions
      val minWidth = data1.distinct.size * 4

      forAll(
        (Gen.choose(minDepth, maxDepth), "depth"),
        (Gen.choose(minWidth, maxWidth), "width"),
        (Gen.choose(Int.MinValue, Int.MaxValue), "seed")) { (depth: Int, width: Int, seed: Int) =>
          val cms1 = TopPctCMS.aggregator[K](depth, width, seed, 0.01).apply(data1)
          cms1.heavyHitters should be(Set(1, 2, 3, 4, 5).toK[K])

          val cms2 = TopPctCMS.aggregator[K](depth, width, seed, 0.1).apply(data1)
          cms2.heavyHitters should be(Set(2, 3, 4, 5).toK[K])

          val cms3 = TopPctCMS.aggregator[K](depth, width, seed, 0.3).apply(data1)
          cms3.heavyHitters should be(Set(5).toK[K])

          val cms4 = TopPctCMS.aggregator[K](depth, width, seed, 0.9).apply(data1)
          cms4.heavyHitters should be(Set[K]())
        }
    }

  }

  "A Top-N Count-Min sketch implementing CMSHeavyHitters" should {

    // Note: As described in https://github.com/twitter/algebird/issues/353, a top-N CMS is, in general, not able to
    // merge heavy hitters correctly.  This is because merging top-N based heavy hitters is not an associative
    // operation.

    "create correct sketches out of a single item" in {
      forAll{ (x: Int) =>
        val data = x.toK[K]
        val cmsMonoid = {
          val anyHeavyHittersN = 2 // exact setting not relevant for this test
          TopNCMS.monoid[K](EPS, DELTA, SEED, anyHeavyHittersN)
        }
        val topCms = cmsMonoid.create(data)
        topCms.totalCount should be(1)
        topCms.cms.totalCount should be(1)
        topCms.frequency(x.toK[K]).estimate should be(1)
        // Poor man's way to come up with an item that is not x and that is very unlikely to hash to the same slot.
        val otherItem = x + 1
        topCms.frequency(otherItem.toK[K]).estimate should be(0)
        // The following assert indirectly verifies whether the counting table is not all-zero (cf. GH-393).
        topCms.innerProduct(topCms).estimate should be(1)
      }
    }

    "create correct sketches out of a single-item stream" in {
      forAll{ (x: Int) =>
        val data = Seq(x).toK[K]
        val cmsMonoid = {
          val anyHeavyHittersN = 2 // exact setting not relevant for this test
          TopNCMS.monoid[K](EPS, DELTA, SEED, anyHeavyHittersN)
        }
        val topCms = cmsMonoid.create(data)
        topCms.totalCount should be(1)
        topCms.cms.totalCount should be(1)
        topCms.frequency(x.toK[K]).estimate should be(1)
        // Poor man's way to come up with an item that is not x and that is very unlikely to hash to the same slot.
        val otherItem = x + 1
        topCms.frequency(otherItem.toK[K]).estimate should be(0)
        // The following assert indirectly verifies whether the counting table is not all-zero (cf. GH-393).
        topCms.innerProduct(topCms).estimate should be(1)
      }
    }

    // This test involves merging of top-N CMS instances, which is not an associative operation.  This means that the
    // success or failure of this test depends on the merging order and/or the test data characteristics.
    "(when adding CMS instances) drop old heavy hitters when new heavy hitters replace them, if merge order matches data" in {
      val heavyHittersN = 2
      val monoid = TopNCMS.monoid[K](EPS, DELTA, SEED, heavyHittersN)
      val cms1 = monoid.create(Seq(1, 2, 2).toK[K])
      cms1.heavyHitters should be(Set(1, 2).toK[K])
      val cms2 = cms1 ++ monoid.create(Seq(3, 3, 3).toK[K])
      cms2.heavyHitters should be(Set(2, 3).toK[K])
      val cms3 = cms2 ++ monoid.create(Seq(1, 1, 1).toK[K])
      cms3.heavyHitters should be(Set(3, 1).toK[K])
      val cms4 = cms3 ++ monoid.create(Seq(6, 6, 6, 6, 6, 6).toK[K])
      cms4.heavyHitters should be(Set(1, 6).toK[K])
    }

    "(when adding individual items) drop old heavy hitters when new heavy hitters replace them" in {
      val monoid = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.3)
      val cms1 = monoid.create(Seq(1, 2, 2).toK[K])
      cms1.heavyHitters should be(Set(1, 2).toK[K])

      val cms2 = cms1 + 2.toK[K]
      cms2.heavyHitters should be(Set(2).toK[K])

      val cms3 = cms2 + 1.toK[K]
      cms3.heavyHitters should be(Set(1, 2).toK[K])

      val heaviest = 0.toK[K]
      val cms4 = cms3 + heaviest + heaviest + heaviest + heaviest + heaviest + heaviest
      cms4.heavyHitters should be(Set(heaviest))
    }

    // This test documents the order bias of top-N CMS, i.e. it's a negative test case.
    // See https://github.com/twitter/algebird/issues/353
    "(when adding CMS instances) generally FAIL to merge heavy hitters correctly due to order bias" in {
      val topN = 2
      val monoid = TopNCMS.monoid[K](EPS, DELTA, SEED, topN)

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
      aggregated.heavyHitters shouldNot equal(single.heavyHitters)
      aggregated.heavyHitters shouldNot contain(3.toK[K]) // C=3 is global top 1 heavy hitter
    }

    // Compared to adding top-N CMS instances, which is generally unsafe because of order bias (see test cases above),
    // adding individual items to a top-N CMS is a safe operation.
    // See https://github.com/twitter/algebird/issues/353
    "(when adding individual items) merge heavy hitters correctly [GH-353 regression test]" in {
      val topN = 2
      val monoid = TopNCMS.monoid[K](EPS, DELTA, SEED, topN)

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

      val minDepth = 2 // Use 2 to be on the safe side in case we happen to run into hash collisions
      val minWidth = data1.distinct.size

      forAll(
        (Gen.choose(minDepth, maxDepth), "depth"),
        (Gen.choose(minWidth, maxWidth), "width"),
        (Gen.choose(Int.MinValue, Int.MaxValue), "seed")) { (depth: Int, width: Int, seed: Int) =>
          val size = math.abs(seed % 5) + 1 // a number 1 to 5
          val hh = ((6 - size) to 5).toSet
          val cms = TopNCMS.monoid[K](depth, width, seed, size).create(data1)
          cms.heavyHitters should be(hh.toK[K])
        }
    }

    "work as an Aggregator when created from a single, small stream" in {
      val data1 = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5).toK[K]

      val minDepth = 2 // Use 2 to be on the safe side in case we happen to run into hash collisions
      val minWidth = data1.distinct.size * 2

      forAll(
        (Gen.choose(minDepth, maxDepth), "depth"),
        (Gen.choose(minWidth, maxWidth), "width"),
        (Gen.choose(Int.MinValue, Int.MaxValue), "seed")) { (depth: Int, width: Int, seed: Int) =>
          val size = math.abs(seed % 5) + 1 // a number 1 to 5
          val hh = ((6 - size) to 5).toSet
          val cms = TopNCMS.aggregator[K](depth, width, seed, size).apply(data1)
          cms.heavyHitters should be(hh.toK[K])
        }
    }

  }

  "A Scoped Top-N Count-Min sketch implementing CMSHeavyHitters" should {

    "create correct sketches out of a single item" in {
      forAll{ (x: Int, y: Int) =>
        val data = (x, y).toK[K]
        val cmsMonoid = {
          val heavyHittersN = 2
          ScopedTopNCMS.monoid[K, K](EPS, DELTA, SEED, heavyHittersN)
        }
        val topCms = cmsMonoid.create(data)
        topCms.totalCount should be(1)
        topCms.cms.totalCount should be(1)
        topCms.frequency((x, y).toK[K]).estimate should be(1)
        // Poor man's way to come up with an item that is not x and that is very unlikely to hash to the same slot.
        val otherItem = (x + 1, y)
        topCms.frequency(otherItem.toK[K]).estimate should be(0)
        // The following assert indirectly verifies whether the counting table is not all-zero (cf. GH-393).
        topCms.innerProduct(topCms).estimate should be(1)
      }
    }

    "(when adding CMS instances) keep all heavy hitters keys" in {
      val heavyHittersN = 1
      val monoid = ScopedTopNCMS.monoid[K, K](EPS, DELTA, SEED, heavyHittersN)
      val cms1 = monoid.create(Seq((1, 1), (2, 3), (2, 3)).toK[K])
      cms1.heavyHitters should be(Set((1, 1), (2, 3)).toK[K])
      val cms2 = cms1 ++ monoid.create(Seq((3, 8), (3, 8), (3, 8)).toK[K])
      cms2.heavyHitters should be(Set((1, 1), (2, 3), (3, 8)).toK[K])
      val cms3 = cms2 ++ monoid.create(Seq((1, 1), (1, 1), (1, 1)).toK[K])
      cms3.heavyHitters should be(Set((1, 1), (2, 3), (3, 8)).toK[K])
      val cms4 = cms3 ++ monoid.create(Seq((6, 2), (6, 2), (6, 2), (6, 2), (6, 2), (6, 2)).toK[K])
      cms4.heavyHitters should be(Set((1, 1), (2, 3), (3, 8), (6, 2)).toK[K])
    }

    "(when adding CMS instances) drop old heavy hitters for the same key when new heavy hitters replace them" in {
      val heavyHittersN = 2
      val monoid = ScopedTopNCMS.monoid[K, K](EPS, DELTA, SEED, heavyHittersN)
      val cms1 = monoid.create(Seq((4, 1), (4, 2), (4, 2)).toK[K])
      cms1.heavyHitters should be(Set((4, 1), (4, 2)).toK[K])
      val cms2 = cms1 ++ monoid.create(Seq((4, 3), (4, 3), (4, 3)).toK[K])
      cms2.heavyHitters should be(Set((4, 2), (4, 3)).toK[K])
      val cms3 = cms2 ++ monoid.create(Seq((4, 1), (4, 1), (4, 1)).toK[K])
      cms3.heavyHitters should be(Set((4, 3), (4, 1)).toK[K])
      val cms4 = cms3 ++ monoid.create(Seq((4, 6), (4, 6), (4, 6), (4, 6), (4, 6), (4, 6)).toK[K])
      cms4.heavyHitters should be(Set((4, 1), (4, 6)).toK[K])
    }

    "trim multiple keys at once" in {
      val heavyHittersN = 2
      val data =
        Seq(1, 2, 2, 3, 3, 3, 6, 6, 6, 6, 6, 6).flatMap { i => Seq((4, i), (7, i + 2)) }.toK[K]
      val monoid = ScopedTopNCMS.monoid[K, K](EPS, DELTA, SEED, heavyHittersN)
      val cms = monoid.create(data)
      cms.heavyHitters should be(Set((4, 3), (4, 6), (7, 5), (7, 8)).toK[K])
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
    forAll((Gen.choose(1, maxI), "depth")) { (i: Int) =>
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

class CMSHasherShortSpec extends CMSHasherSpec[Short]
class CMSHasherIntSpec extends CMSHasherSpec[Int]
class CMSHasherLongSpec extends CMSHasherSpec[Long]
class CMSHasherBigIntSpec extends CMSHasherSpec[BigInt]
class CMSHasherBigDecimalSpec extends CMSHasherSpec[BigDecimal]
class CMSHasherStringSpec extends CMSHasherSpec[String]
class CMSHasherBytesSpec extends CMSHasherSpec[Bytes]

abstract class CMSHasherSpec[K: CMSHasher: FromIntLike] extends PropSpec with PropertyChecks with Matchers {

  property("returns hashes (i.e. slots) in the range [0, width)") {
    forAll { (a: Int, b: Int, width: Int, x: Int) =>
      whenever (width > 0) {
        val hash = CMSHash[K](a, b, width)
        val hashValue = hash(x.toK[K])

        hashValue should be >= 0
        hashValue should be < width
      }
    }
  }

  property("returns scoped hashes in the range [0, width)") {
    forAll { (a: Int, b: Int, width: Int, x: Int, y: Int) =>
      whenever (width > 0) {
        val hasher = ScopedTopNCMS.scopedHasher[K, K]
        val hashValue = hasher.hash(a, b, width)((x, y).toK[K])

        hashValue should be >= 0
        hashValue should be < width
      }
    }
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

object CmsTestImplicits {

  // Convenience methods to convert from `Int` to the actual `K` type, and we prefer these conversions to be explicit
  // (cf. JavaConverters vs. JavaConversions). We use the name `toK` to clarify the intent and to prevent name conflicts
  // with the existing `to[Col]` method in Scala.

  implicit class IntCast(x: Int) {
    def toK[T: FromIntLike]: T = implicitly[FromIntLike[T]].fromInt(x)
  }

  implicit class PairCast(x: (Int, Int)) {
    def toK[T: FromIntLike]: (T, T) = (x._1.toK[T], x._2.toK[T])
  }

  implicit class SeqCast(xs: Seq[Int]) {
    def toK[T: FromIntLike]: Seq[T] = xs map { _.toK[T] }
  }

  implicit class PairSeqCast(xs: Seq[(Int, Int)]) {
    def toK[T: FromIntLike]: Seq[(T, T)] = xs map { _.toK[T] }
  }

  implicit class SetCast(xs: Set[Int]) {
    def toK[T: FromIntLike]: Set[T] = xs map { _.toK[T] }
  }

  implicit class PairSetCast(xs: Set[(Int, Int)]) {
    def toK[T: FromIntLike]: Set[(T, T)] = xs map { _.toK[T] }
  }

}
