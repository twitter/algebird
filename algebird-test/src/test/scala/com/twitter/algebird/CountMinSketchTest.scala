package com.twitter.algebird

import org.scalatest.{Matchers, PropSpec, WordSpec}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalacheck.{Arbitrary, Gen, Properties}

import scala.util.Random
import CMSHasherImplicits.CMSHasherBigInt

object CmsLaws {
  def int2Bytes(i: Int): Bytes = Bytes(BigInt(i).toByteArray)
}

class CmsLaws extends CheckProperties {
  import BaseProperties._

  val DELTA = 1e-8
  val EPS = 0.005
  val SEED = 1

  def monoid[K: CMSHasher]: CMSMonoid[K] =
    CMS.monoid[K](EPS, DELTA, SEED)

  private def cmsArb[K](cmsMonoid: CMSMonoid[K])(toK: Int => K): Arbitrary[CMS[K]] =
    Arbitrary(Gen.choose(0, 10000).map(v => cmsMonoid.create(toK(v))))

  private def createArbitrary[K](monoid: CMSMonoid[K])(implicit r: Ring[K]): Arbitrary[CMS[K]] =
    cmsArb(monoid)(r.fromInt(_))

  implicit def cmsEquiv[K]: Equiv[CMS[K]] =
    new Equiv[CMS[K]] {
      def equiv(x: CMS[K], y: CMS[K]): Boolean = {
        val d = CMSInstance(x.params)
        (d ++ x) == (d ++ y)
      }
    }

  property("CountMinSketch[Short] is a Monoid") {
    implicit val cmsMonoid = monoid[Short]
    implicit val cmsGen = createArbitrary[Short](cmsMonoid)
    commutativeMonoidLaws[CMS[Short]]
  }

  property("CountMinSketch[Int] is a Monoid") {
    implicit val cmsMonoid = monoid[Int]
    implicit val cmsGen = createArbitrary[Int](cmsMonoid)
    commutativeMonoidLaws[CMS[Int]]
  }

  property("CountMinSketch[Long] is a Monoid") {
    implicit val cmsMonoid = monoid[Long]
    implicit val cmsGen = createArbitrary[Long](cmsMonoid)
    commutativeMonoidLaws[CMS[Long]]
  }

  property("CountMinSketch[BigInt] is a Monoid") {
    implicit val cmsMonoid = monoid[BigInt]
    implicit val cmsGen = createArbitrary[BigInt](cmsMonoid)
    commutativeMonoidLaws[CMS[BigInt]]
  }

  property("CountMinSketch[BigDecimal] is a Monoid") {
    implicit val cmsMonoid = monoid[BigDecimal]
    implicit val cmsGen = createArbitrary[BigDecimal](cmsMonoid)
    commutativeMonoidLaws[CMS[BigDecimal]]
  }

  property("CountMinSketch[String] is a Monoid") {
    implicit val cmsMonoid = monoid[String]
    implicit val cmsGen = cmsArb(cmsMonoid)(_.toString)
    commutativeMonoidLaws[CMS[String]]
  }

  property("CountMinSketch[Bytes] is a commutative monoid") {
    implicit val cmsMonoid = monoid[Bytes]
    implicit val cmsGen = cmsArb(cmsMonoid)(CmsLaws.int2Bytes(_))
    commutativeMonoidLaws[CMS[Bytes]]
  }
}

class TopPctCmsLaws extends CheckProperties {
  import BaseProperties._

  val DELTA = 1e-8
  val EPS = 0.005
  val SEED = 1
  val HEAVY_HITTERS_PCT = 0.1

  def monoid[K: CMSHasher]: TopCMSMonoid[K] =
    TopPctCMS.monoid[K](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)

  private def topCmsArb[K](cmsMonoid: TopCMSMonoid[K])(toK: Int => K): Arbitrary[TopCMS[K]] =
    Arbitrary(Gen.choose(0, 10000).map(v => cmsMonoid.create(toK(v))))

  private def createArbitrary[K](monoid: TopCMSMonoid[K])(implicit r: Ring[K]): Arbitrary[TopCMS[K]] =
    topCmsArb(monoid)(r.fromInt(_))

  implicit def topCmsEquiv[K]: Equiv[TopCMS[K]] =
    new Equiv[TopCMS[K]] {
      def equiv(x: TopCMS[K], y: TopCMS[K]): Boolean =
        (x ++ x) == (x ++ y)
    }

  property("TopPctCms[Short] is a Monoid") {
    implicit val cmsMonoid = monoid[Short]
    implicit val cmsGen = createArbitrary[Short](cmsMonoid)
    commutativeMonoidLaws[TopCMS[Short]]
  }

  property("TopPctCms[Int] is a Monoid") {
    implicit val cmsMonoid = monoid[Int]
    implicit val cmsGen = createArbitrary[Int](cmsMonoid)
    commutativeMonoidLaws[TopCMS[Int]]
  }

  property("TopPctCms[Long] is a Monoid") {
    implicit val cmsMonoid = monoid[Long]
    implicit val cmsGen = createArbitrary[Long](cmsMonoid)
    commutativeMonoidLaws[TopCMS[Long]]
  }

  property("TopPctCms[BigInt] is a Monoid") {
    implicit val cmsMonoid = monoid[BigInt]
    implicit val cmsGen = createArbitrary[BigInt](cmsMonoid)
    commutativeMonoidLaws[TopCMS[BigInt]]
  }

  property("TopPctCms[BigDecimal] is a Monoid") {
    implicit val cmsMonoid = monoid[BigDecimal]
    implicit val cmsGen = createArbitrary[BigDecimal](cmsMonoid)
    commutativeMonoidLaws[TopCMS[BigDecimal]]
  }

  property("TopPctCms[String] is a Monoid") {
    implicit val cmsMonoid = monoid[String]
    implicit val cmsGen = topCmsArb(cmsMonoid)(_.toString)
    commutativeMonoidLaws[TopCMS[String]]
  }

  property("TopPctCms[Bytes] is a Monoid") {
    implicit val cmsMonoid = monoid[Bytes]
    implicit val cmsGen = topCmsArb(cmsMonoid)(CmsLaws.int2Bytes(_))
    commutativeMonoidLaws[TopCMS[Bytes]]
  }
}

class SparseCMSTest extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {

  import BaseProperties._

  val DELTA = 1e-8
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

  val DELTA = 1e-8
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
    val targetHasher: CMSHasher[Seq[Byte]] =
      sourceHasher.contramap((d: Seq[Byte]) => f(d))
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
      val anyDelta = 1e-10
      val anyEps = 0.001
      val anySeed = 1
      CMS.monoid[Seq[Byte]](anyEps, anyDelta, anySeed)
    }

    // Then we should be able to create CMS instances for L.
    // To verify this we perform a basic smoke test of the resulting monoids.  This test mimics the "exactly compute
    // frequencies in a small stream" scenario in the full-fledged CMS spec.
    val oneKey = Seq[Byte](1)
    val one = bytesMonoid.create(oneKey)
    one.frequency(oneKey).estimate should be(1)
    one.frequency(Seq[Byte](2)).estimate should be(0)
    val two = bytesMonoid.create(Seq[Byte](2))
    two.frequency(oneKey).estimate should be(0)
    two.frequency(Seq[Byte](2)).estimate should be(1)
    val cms = bytesMonoid.plus(bytesMonoid.plus(one, two), two)

    cms.frequency(Seq[Byte](0)).estimate should be(0)
    cms.frequency(oneKey).estimate should be(1)
    cms.frequency(Seq[Byte](2)).estimate should be(2)

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
    val oneKey = Seq[Byte](1)
    val twoKey = Seq[Byte](2)
    val threeKey = Seq[Byte](3)
    val fourKey = Seq[Byte](4)
    val fiveKey = Seq[Byte](5)
    val data1: Seq[Seq[Byte]] = Seq(
      oneKey,
      twoKey,
      twoKey,
      threeKey,
      threeKey,
      threeKey,
      fourKey,
      fourKey,
      fourKey,
      fourKey,
      fiveKey,
      fiveKey,
      fiveKey,
      fiveKey,
      fiveKey
    )
    val minWidth = data1.distinct.size

    forAll(
      (Gen.choose(1, 70), "depth"),
      (Gen.choose(minWidth, 1000), "width"),
      (Gen.choose(Int.MinValue, Int.MaxValue), "seed")
    ) { (depth: Int, width: Int, seed: Int) =>
      val cms1: TopCMS[Seq[Byte]] =
        TopPctCMS.monoid[Seq[Byte]](depth, width, seed, 0.01).create(data1)
      cms1.heavyHitters should be(Set(oneKey, twoKey, threeKey, fourKey, fiveKey))

      val cms2: TopCMS[Seq[Byte]] =
        TopPctCMS.monoid[Seq[Byte]](depth, width, seed, 0.1).create(data1)
      cms2.heavyHitters should be(Set(twoKey, threeKey, fourKey, fiveKey))

      val cms3: TopCMS[Seq[Byte]] =
        TopPctCMS.monoid[Seq[Byte]](depth, width, seed, 0.3).create(data1)
      cms3.heavyHitters should be(Set(fiveKey))

      val cms4: TopCMS[Seq[Byte]] =
        TopPctCMS.monoid[Seq[Byte]](depth, width, seed, 0.9).create(data1)
      cms4.heavyHitters should be(Set.empty[Seq[Byte]])
    }
  }
}

class CMSShortTest extends CMSRingTest[Short]
class CMSIntTest extends CMSRingTest[Int]
class CMSLongTest extends CMSRingTest[Long]
class CMSBigIntTest extends CMSRingTest[BigInt]
class CMSBigDecimalTest extends CMSRingTest[BigDecimal]
class CMSStringTest extends CMSTest[String](_.toString)
class CMSBytesTest extends CMSTest[Bytes](CmsLaws.int2Bytes(_))

abstract class CmsProperty[K] extends ApproximateProperty

object CmsProperty {
  val delta = 1e-10
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
  def exactGenerator: Gen[Vector[K]] =
    Gen.nonEmptyListOf[K](implicitly[Gen[K]]).map(_.toVector)
}

class CmsLargeFrequencyProperty[K: CMSHasher: Gen] extends CmsFrequencyProperty[K] {
  def exactGenerator: Gen[Vector[K]] =
    Gen.containerOfN[Vector, K](100000, implicitly[Gen[K]])
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
    (counts1.keys.toSet & counts2.keys.toSet).toSeq.map { k =>
      counts1(k) * counts2(k)
    }.sum
  }

  def approximateResult(cmses: (CMS[K], CMS[K]), input: Unit) =
    cmses._1.innerProduct(cmses._2)
}

class CmsTotalCountProperty[K: CMSHasher: Gen] extends CmsProperty[K] {
  type Exact = Vector[K]
  type Approx = CMS[K]

  type Input = Unit
  type Result = Long

  def makeApproximate(exact: Vector[K]) = CmsProperty.makeApproximate(exact)

  def exactGenerator: Gen[Vector[K]] =
    Gen.containerOfN[Vector, K](10000, implicitly[Gen[K]])

  def inputGenerator(e: Vector[K]): Gen[Unit] = Gen.const(())

  def exactResult(list: Vector[K], input: Unit) = list.length

  def approximateResult(cms: CMS[K], input: Unit) =
    Approximate.exact(cms.totalCount)
}

class CmsProperties extends ApproximateProperties("CountMinSketch") {
  import ApproximateProperty.toProp

  implicit val intGen = Gen.choose(1, 100)

  property("CMS works for small lists") = toProp(new CmsSmallFrequencyProperty[Int](), 10, 10, 0.01)
  property("CMS works for large lists") = toProp(new CmsLargeFrequencyProperty[Int](), 10, 10, 0.01)
  property("CMS inner product works") = toProp(new CmsInnerProductProperty[Int](), 10, 10, 0.01)
  property("CMS counts total count") = toProp(new CmsTotalCountProperty[Int](), 10, 10, 0.01)
}

abstract class CMSRingTest[K: CMSHasher: Ring] extends CMSTest[K]((x: Int) => implicitly[Ring[K]].fromInt(x))

abstract class CMSTest[K: CMSHasher](toK: Int => K)
    extends WordSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {

  val DELTA = 1e-10
  val EPS = 0.001
  val SEED = 1

  private[this] val maxDepth = 70
  private[this] val maxWidth = 1000

  def pairToK(pair: (Int, Int)): (K, K) = (toK(pair._1), toK(pair._2))

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
    (1 to size).map { _ =>
      toK(rnd.nextInt(range))
    }
  }

  "A Count-Min sketch implementing CMSCounting" should {

    "work as an Aggregator when created from a single, small stream" in {
      val data1 = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5).map(toK)

      val cms = CMS.aggregator[K](EPS, DELTA, SEED).apply(data1)
      cms.frequency(toK(1)).estimate should be(1L)
      cms.frequency(toK(2)).estimate should be(2L)
      cms.frequency(toK(3)).estimate should be(3L)
      cms.frequency(toK(4)).estimate should be(4L)
      cms.frequency(toK(5)).estimate should be(5L)

      val topPctCMS = {
        val anyHeavyHittersPct = 0.1 // exact setting not relevant for this test
        TopPctCMS
          .aggregator[K](EPS, DELTA, SEED, anyHeavyHittersPct)
          .apply(data1)
      }
      topPctCMS.frequency(toK(1)).estimate should be(1L)
      topPctCMS.frequency(toK(2)).estimate should be(2L)
      topPctCMS.frequency(toK(3)).estimate should be(3L)
      topPctCMS.frequency(toK(4)).estimate should be(4L)
      topPctCMS.frequency(toK(5)).estimate should be(5L)

      val topNCMS = {
        val anyHeavyHittersN = 1 // exact setting not relevant for this test
        TopNCMS.aggregator[K](EPS, DELTA, SEED, anyHeavyHittersN).apply(data1)
      }
      topNCMS.frequency(toK(1)).estimate should be(1L)
      topNCMS.frequency(toK(2)).estimate should be(2L)
      topNCMS.frequency(toK(3)).estimate should be(3L)
      topNCMS.frequency(toK(4)).estimate should be(4L)
      topNCMS.frequency(toK(5)).estimate should be(5L)
    }

  }

  "A Top-% Count-Min sketch implementing CMSHeavyHitters" should {

    "create correct sketches out of a single item" in {
      forAll { (x: Int) =>
        val data = toK(x)
        val cmsMonoid = {
          val anyHeavyHittersPct = 0.1 // exact setting not relevant for this test
          TopPctCMS.monoid[K](EPS, DELTA, SEED, anyHeavyHittersPct)
        }
        val topCms = cmsMonoid.create(data)
        topCms.totalCount should be(1)
        topCms.cms.totalCount should be(1)
        topCms.frequency(data).estimate should be(1)
        // Poor man's way to come up with an item that is not x and that is very unlikely to hash to the same slot.
        val otherItem = x + 1
        topCms.frequency(toK(otherItem)).estimate should be(0)
        // The following assert indirectly verifies whether the counting table is not all-zero (cf. GH-393).
        topCms.innerProduct(topCms).estimate should be(1)
      }
    }

    "create correct sketches out of a single-item stream" in {
      forAll { (x: Int) =>
        val data = Seq(toK(x))
        val cmsMonoid = {
          val anyHeavyHittersPct = 0.1 // exact setting not relevant for this test
          TopPctCMS.monoid[K](EPS, DELTA, SEED, anyHeavyHittersPct)
        }
        val topCms = cmsMonoid.create(data)
        topCms.totalCount should be(1)
        topCms.cms.totalCount should be(1)
        topCms.frequency(toK(x)).estimate should be(1)
        // Poor man's way to come up with an item that is not x and that is very unlikely to hash to the same slot.
        val otherItem = x + 1
        topCms.frequency(toK(otherItem)).estimate should be(0)
        // The following assert indirectly verifies whether the counting table is not all-zero (cf. GH-393).
        topCms.innerProduct(topCms).estimate should be(1)
      }
    }

    "estimate heavy hitters" in {
      // Simple way of making some elements appear much more often than others.
      val data1 = (1 to 3000).map { _ =>
        toK(RAND.nextInt(3))
      }
      val data2 = (1 to 3000).map { _ =>
        toK(RAND.nextInt(10))
      }
      val data3 = (1 to 1450).map { _ =>
        toK(-1)
      } // element close to being a 20% heavy hitter
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
      val infrequent = data
        .groupBy { x =>
          x
        }
        .mapValues { _.size }
        .filter { _._2 < minHhCount }
        .keys
        .toSet
      infrequent.intersect(estimatedHhs) should be('empty)
    }

    "(when adding CMS instances) drop old heavy hitters when new heavy hitters replace them" in {
      val monoid = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.3)
      val cms1 = monoid.create(Seq(1, 2, 2).map(toK))
      cms1.heavyHitters should be(Set(1, 2).map(toK))

      val cms2 = cms1 ++ monoid.create(toK(2))
      cms2.heavyHitters should be(Set(toK(2)))

      val cms3 = cms2 ++ monoid.create(toK(1))
      cms3.heavyHitters should be(Set(toK(1), toK(2)))

      val cms4 = cms3 ++ monoid.create(Seq(0, 0, 0, 0, 0, 0).map(toK))
      cms4.heavyHitters should be(Set(toK(0)))
    }

    "(when adding individual items) drop old heavy hitters when new heavy hitters replace them" in {
      val monoid = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.3)
      val cms1 = monoid.create(Seq(1, 2, 2).map(toK))
      cms1.heavyHitters should be(Set(1, 2).map(toK))

      val cms2 = cms1 + toK(2)
      cms2.heavyHitters should be(Set(toK(2)))

      val cms3 = cms2 + toK(1)
      cms3.heavyHitters should be(Set(toK(1), toK(2)))

      val heaviest = toK(0)
      val cms4 = cms3 + heaviest + heaviest + heaviest + heaviest + heaviest + heaviest
      cms4.heavyHitters should be(Set(heaviest))
    }

    "(when adding CMS instances) merge heavy hitters correctly [GH-353 regression test]" in {
      // See https://github.com/twitter/algebird/issues/353
      val monoid = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.1)

      val data1 = Seq(1, 1, 1, 2, 2, 3).map(toK)
      val data2 = Seq(3, 4, 4, 4, 5, 5).map(toK)
      val data3 = Seq(3, 6, 6, 6, 7, 7).map(toK)
      val data4 = Seq(3, 8, 8, 8, 9, 9).map(toK)
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
      aggregated.heavyHitters contains (toK(3)) // C=3 is global top 1 heavy hitter
    }

    "exactly compute heavy hitters when created from a single, small stream" in {
      val data1 = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5).map(toK)

      val minDepth = 2 // Use 2 to be on the safe side in case we happen to run into hash collisions
      val minWidth = data1.distinct.size

      forAll(
        (Gen.choose(minDepth, maxDepth), "depth"),
        (Gen.choose(minWidth, maxWidth), "width"),
        (Gen.choose(Int.MinValue, Int.MaxValue), "seed")
      ) { (depth: Int, width: Int, seed: Int) =>
        val cms1 = TopPctCMS.monoid[K](depth, width, seed, 0.01).create(data1)
        cms1.heavyHitters should be(Set(1, 2, 3, 4, 5).map(toK))

        val cms2 = TopPctCMS.monoid[K](depth, width, seed, 0.1).create(data1)
        cms2.heavyHitters should be(Set(2, 3, 4, 5).map(toK))

        val cms3 = TopPctCMS.monoid[K](depth, width, seed, 0.3).create(data1)
        cms3.heavyHitters should be(Set(5).map(toK))

        val cms4 = TopPctCMS.monoid[K](depth, width, seed, 0.9).create(data1)
        cms4.heavyHitters should be(Set.empty[K])
      }
    }

    "work as an Aggregator when created from a single, small stream" in {
      val data1 = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5).map(toK)

      val minDepth = 2 // Use 2 to be on the safe side in case we happen to run into hash collisions
      val minWidth = data1.distinct.size * 4
      val SEED = 1

      forAll((Gen.choose(minDepth, maxDepth), "depth"), (Gen.choose(minWidth, maxWidth), "width")) {
        (depth: Int, width: Int) =>
          val cms1 =
            TopPctCMS.aggregator[K](depth, width, SEED, 0.01).apply(data1)
          cms1.heavyHitters should be(Set(1, 2, 3, 4, 5).map(toK))

          val cms2 =
            TopPctCMS.aggregator[K](depth, width, SEED, 0.1).apply(data1)
          cms2.heavyHitters should be(Set(2, 3, 4, 5).map(toK))

          val cms3 =
            TopPctCMS.aggregator[K](depth, width, SEED, 0.3).apply(data1)
          cms3.heavyHitters should be(Set(5).map(toK))

          val cms4 =
            TopPctCMS.aggregator[K](depth, width, SEED, 0.9).apply(data1)
          cms4.heavyHitters should be(Set.empty[K])
      }
    }

  }

  "A Top-N Count-Min sketch implementing CMSHeavyHitters" should {

    // Note: As described in https://github.com/twitter/algebird/issues/353, a top-N CMS is, in general, not able to
    // merge heavy hitters correctly.  This is because merging top-N based heavy hitters is not an associative
    // operation.

    "create correct sketches out of a single item" in {
      forAll { (x: Int) =>
        val data = toK(x)
        val cmsMonoid = {
          val anyHeavyHittersN = 2 // exact setting not relevant for this test
          TopNCMS.monoid[K](EPS, DELTA, SEED, anyHeavyHittersN)
        }
        val topCms = cmsMonoid.create(data)
        topCms.totalCount should be(1)
        topCms.cms.totalCount should be(1)
        topCms.frequency(toK(x)).estimate should be(1)
        // Poor man's way to come up with an item that is not x and that is very unlikely to hash to the same slot.
        val otherItem = x + 1
        topCms.frequency(toK(otherItem)).estimate should be(0)
        // The following assert indirectly verifies whether the counting table is not all-zero (cf. GH-393).
        topCms.innerProduct(topCms).estimate should be(1)
      }
    }

    "create correct sketches out of a single-item stream" in {
      forAll { (x: Int) =>
        val data = Seq(toK(x))
        val cmsMonoid = {
          val anyHeavyHittersN = 2 // exact setting not relevant for this test
          TopNCMS.monoid[K](EPS, DELTA, SEED, anyHeavyHittersN)
        }
        val topCms = cmsMonoid.create(data)
        topCms.totalCount should be(1)
        topCms.cms.totalCount should be(1)
        topCms.frequency(toK(x)).estimate should be(1)
        // Poor man's way to come up with an item that is not x and that is very unlikely to hash to the same slot.
        val otherItem = x + 1
        topCms.frequency(toK(otherItem)).estimate should be(0)
        // The following assert indirectly verifies whether the counting table is not all-zero (cf. GH-393).
        topCms.innerProduct(topCms).estimate should be(1)
      }
    }

    // This test involves merging of top-N CMS instances, which is not an associative operation.  This means that the
    // success or failure of this test depends on the merging order and/or the test data characteristics.
    "(when adding CMS instances) drop old heavy hitters when new heavy hitters replace them, if merge order matches data" in {
      val heavyHittersN = 2
      val monoid = TopNCMS.monoid[K](EPS, DELTA, SEED, heavyHittersN)
      val cms1 = monoid.create(Seq(1, 2, 2).map(toK))
      cms1.heavyHitters should be(Set(1, 2).map(toK))
      val cms2 = cms1 ++ monoid.create(Seq(3, 3, 3).map(toK))
      cms2.heavyHitters should be(Set(2, 3).map(toK))
      val cms3 = cms2 ++ monoid.create(Seq(1, 1, 1).map(toK))
      cms3.heavyHitters should be(Set(3, 1).map(toK))
      val cms4 = cms3 ++ monoid.create(Seq(6, 6, 6, 6, 6, 6).map(toK))
      cms4.heavyHitters should be(Set(1, 6).map(toK))
    }

    "(when adding individual items) drop old heavy hitters when new heavy hitters replace them" in {
      val monoid = TopPctCMS.monoid[K](EPS, DELTA, SEED, 0.3)
      val cms1 = monoid.create(Seq(1, 2, 2).map(toK))
      cms1.heavyHitters should be(Set(1, 2).map(toK))

      val cms2 = cms1 + toK(2)
      cms2.heavyHitters should be(Set(toK(2)))

      val cms3 = cms2 + toK(1)
      cms3.heavyHitters should be(Set(toK(1), toK(2)))

      val heaviest = toK(0)
      val cms4 = cms3 + heaviest + heaviest + heaviest + heaviest + heaviest + heaviest
      cms4.heavyHitters should be(Set(heaviest))
    }

    // This test documents the order bias of top-N CMS, i.e. it's a negative test case.
    // See https://github.com/twitter/algebird/issues/353
    "(when adding CMS instances) generally FAIL to merge heavy hitters correctly due to order bias" in {
      val topN = 2
      val monoid = TopNCMS.monoid[K](EPS, DELTA, SEED, topN)

      val data1 = Seq(1, 1, 1, 2, 2, 3).map(toK)
      val data2 = Seq(3, 4, 4, 4, 5, 5).map(toK)
      val data3 = Seq(3, 6, 6, 6, 7, 7).map(toK)
      val data4 = Seq(3, 8, 8, 8, 9, 9).map(toK)
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
      aggregated.heavyHitters shouldNot contain(toK(3)) // C=3 is global top 1 heavy hitter
    }

    // Compared to adding top-N CMS instances, which is generally unsafe because of order bias (see test cases above),
    // adding individual items to a top-N CMS is a safe operation.
    // See https://github.com/twitter/algebird/issues/353
    "(when adding individual items) merge heavy hitters correctly [GH-353 regression test]" in {
      val topN = 2
      val monoid = TopNCMS.monoid[K](EPS, DELTA, SEED, topN)

      val data1 = Seq(1, 1, 1, 2, 2, 3).map(toK)
      val data2 = Seq(3, 4, 4, 4, 5, 5).map(toK)
      val data3 = Seq(3, 6, 6, 6, 7, 7).map(toK)
      val data4 = Seq(3, 8, 8, 8, 9, 9).map(toK)
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
      val cms2 = cms1 + toK(3) + toK(4) + toK(4) + toK(4) + toK(5) + toK(5) // effectively "++ data2"
      val cms3 = cms2 + toK(3) + toK(6) + toK(6) + toK(6) + toK(7) + toK(7) // "++ data3"
      val aggregated = cms3 + toK(3) + toK(8) + toK(8) + toK(8) + toK(9) + toK(9) // "++ data4"

      val single = monoid.create(singleData)
      aggregated.heavyHitters should be(single.heavyHitters)
      aggregated.heavyHitters should contain(toK(3)) // C=3 is global top 1 heavy hitter
    }

    val seed = 500
    val size = 5
    val hh = (1 to 5).toSet.map(toK)

    "exactly compute heavy hitters when created a from single, small stream" in {
      val data1 = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5).map(toK)

      val minDepth = 2 // Use 2 to be on the safe side in case we happen to run into hash collisions
      val minWidth = data1.distinct.size

      forAll((Gen.choose(minDepth, maxDepth), "depth"), (Gen.choose(minWidth, maxWidth), "width")) {
        (depth: Int, width: Int) =>
          val cms = TopNCMS.monoid[K](depth, width, seed, size).create(data1)
          cms.heavyHitters should be(hh)
      }
    }

    "work as an Aggregator when created from a single, small stream" in {
      val data1 = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5).map(toK)

      val minDepth = 2 // Use 2 to be on the safe side in case we happen to run into hash collisions
      val minWidth = data1.distinct.size * 2

      forAll((Gen.choose(minDepth, maxDepth), "depth"), (Gen.choose(minWidth, maxWidth), "width")) {
        (depth: Int, width: Int) =>
          val cms = TopNCMS.aggregator[K](depth, width, seed, size).apply(data1)
          cms.heavyHitters should be(hh)
      }
    }

  }

  "A Scoped Top-N Count-Min sketch implementing CMSHeavyHitters" should {

    "create correct sketches out of a single item" in {
      forAll { (x: Int, y: Int) =>
        val data = (toK(x), toK(y))
        val cmsMonoid = {
          val heavyHittersN = 2
          ScopedTopNCMS.monoid[K, K](EPS, DELTA, SEED, heavyHittersN)
        }
        val topCms = cmsMonoid.create(data)
        topCms.totalCount should be(1)
        topCms.cms.totalCount should be(1)
        topCms.frequency((toK(x), toK(y))).estimate should be(1)
        // Poor man's way to come up with an item that is not x and that is very unlikely to hash to the same slot.
        val otherItem = (x + 1, y)
        topCms.frequency(pairToK(otherItem)).estimate should be(0)
        // The following assert indirectly verifies whether the counting table is not all-zero (cf. GH-393).
        topCms.innerProduct(topCms).estimate should be(1)
      }
    }

    "(when adding CMS instances) keep all heavy hitters keys" in {
      val heavyHittersN = 1
      val monoid = ScopedTopNCMS.monoid[K, K](EPS, DELTA, SEED, heavyHittersN)
      val cms1 = monoid.create(Seq((1, 1), (2, 3), (2, 3)).map(pairToK))
      cms1.heavyHitters should be(Set((1, 1), (2, 3)).map(pairToK))
      val cms2 = cms1 ++ monoid.create(Seq((3, 8), (3, 8), (3, 8)).map(pairToK))
      cms2.heavyHitters should be(Set((1, 1), (2, 3), (3, 8)).map(pairToK))
      val cms3 = cms2 ++ monoid.create(Seq((1, 1), (1, 1), (1, 1)).map(pairToK))
      cms3.heavyHitters should be(Set((1, 1), (2, 3), (3, 8)).map(pairToK))
      val cms4 = cms3 ++ monoid.create(Seq((6, 2), (6, 2), (6, 2), (6, 2), (6, 2), (6, 2)).map(pairToK))
      cms4.heavyHitters should be(Set((1, 1), (2, 3), (3, 8), (6, 2)).map(pairToK))
    }

    "(when adding CMS instances) drop old heavy hitters for the same key when new heavy hitters replace them" in {
      val heavyHittersN = 2
      val monoid = ScopedTopNCMS.monoid[K, K](EPS, DELTA, SEED, heavyHittersN)
      val cms1 = monoid.create(Seq((4, 1), (4, 2), (4, 2)).map(pairToK))
      cms1.heavyHitters should be(Set((4, 1), (4, 2)).map(pairToK))
      val cms2 = cms1 ++ monoid.create(Seq((4, 3), (4, 3), (4, 3)).map(pairToK))
      cms2.heavyHitters should be(Set((4, 2), (4, 3)).map(pairToK))
      val cms3 = cms2 ++ monoid.create(Seq((4, 1), (4, 1), (4, 1)).map(pairToK))
      cms3.heavyHitters should be(Set((4, 3), (4, 1)).map(pairToK))
      val cms4 = cms3 ++ monoid.create(Seq((4, 6), (4, 6), (4, 6), (4, 6), (4, 6), (4, 6)).map(pairToK))
      cms4.heavyHitters should be(Set((4, 1), (4, 6)).map(pairToK))
    }

    "trim multiple keys at once" in {
      val heavyHittersN = 2
      val data =
        Seq(1, 2, 2, 3, 3, 3, 6, 6, 6, 6, 6, 6)
          .flatMap { i =>
            Seq((4, i), (7, i + 2))
          }
          .map(pairToK)
      val monoid = ScopedTopNCMS.monoid[K, K](EPS, DELTA, SEED, heavyHittersN)
      val cms = monoid.create(data)
      cms.heavyHitters should be(Set((4, 3), (4, 6), (7, 5), (7, 8)).map(pairToK))
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
    val maxValidDelta = 745
    forAll(Gen.choose(maxValidDelta + 1, 10000)) { (invalidDepth: Int) =>
      // due to shrinking we can shrink to an invalid size, double check the size:
      // this is a known issue in scalacheck
      if (invalidDepth > maxValidDelta) {
        val exception = intercept[IllegalArgumentException] {
          CMSFunctions.delta(invalidDepth)
        }
        (exception.getMessage should fullyMatch).regex(
          """requirement failed: depth must be smaller as it causes precision errors when computing delta \(\d+ led to an invalid delta of 0.0\)"""
        )
      }
    }
  }

  property("throw IAE when deriving depth from invalid delta values") {
    val invalidDeltas = Table("invalidDelta", 0.0, 1.0, 2.0, -1.0)
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
  val AnyDelta = 1e-5
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
    val invalidDeltas = Table("invalidDelta", 0.0, 1.0, 2.0, 100.0, -0.1)
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
    (exception.getMessage should fullyMatch)
      .regex("""requirement failed: we require at least (\d+) hash functions""")
  }

}

class CMSHasherShortSpec extends CMSHasherRingSpec[Short]
class CMSHasherIntSpec extends CMSHasherRingSpec[Int]
class CMSHasherLongSpec extends CMSHasherRingSpec[Long]
class CMSHasherBigIntSpec extends CMSHasherRingSpec[BigInt]
class CMSHasherBigDecimalSpec extends CMSHasherRingSpec[BigDecimal]
class CMSHasherStringSpec extends CMSHasherSpec[String](_.toString)
class CMSHasherBytesSpec extends CMSHasherSpec[Bytes](CmsLaws.int2Bytes(_))

abstract class CMSHasherRingSpec[K: CMSHasher: Ring] extends CMSHasherSpec[K](implicitly[Ring[K]].fromInt(_))

abstract class CMSHasherSpec[K: CMSHasher](toK: Int => K) extends PropSpec with PropertyChecks with Matchers {

  property("returns hashes (i.e. slots) in the range [0, width)") {
    forAll { (a: Int, b: Int, width: Int, x: Int) =>
      whenever(width > 0) {
        val hash = CMSHash[K](a, b, width)
        val hashValue = hash(toK(x))

        hashValue should be >= 0
        hashValue should be < width
      }
    }
  }

  property("returns scoped hashes in the range [0, width)") {
    forAll { (a: Int, b: Int, width: Int, x: Int, y: Int) =>
      whenever(width > 0) {
        val hasher = ScopedTopNCMS.scopedHasher[K, K]
        val hashValue = hasher.hash(a, b, width)((toK(x), toK(y)))

        hashValue should be >= 0
        hashValue should be < width
      }
    }
  }

}
