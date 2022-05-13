package com.twitter.algebird.immutable

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.twitter.algebird.{
  ApproximateProperties,
  ApproximateProperty,
  Bytes,
  CheckProperties,
  Hash128,
  Monoid,
  MurmurHash128
}

object BloomFilterTestUtils {
  def toDense[A](bloomFilter: BloomFilter[A])(bf: bloomFilter.Hash): bloomFilter.Hash = bf match {
    case bloomFilter.Item(item) =>
      val bs = bloomFilter.hashToArray(item).foldLeft(BitSet.empty)(_ + _)
      bloomFilter.Instance(bs)
    case bfi => bfi
  }
}

class ImmutableBloomFilterLaws extends CheckProperties {

  import com.twitter.algebird.BaseProperties._
  import BloomFilterTestUtils._

  val bf: BloomFilter[String] = BloomFilter[String](6, 12)
  import bf._

  implicit val bfGen: Arbitrary[bf.Hash] =
    Arbitrary {
      val item = Gen.choose(0, 10000).map(v => bf.create(v.toString))
      val zero = Gen.const(Monoid.zero)
      val sparse = Gen.listOf(item).map { its =>
        Monoid.sum(its)
      }
      val dense = Gen.listOf(item).map { its =>
        toDense(bf)(Monoid.sum(its))
      }
      Gen.frequency((1, zero), (5, item), (10, sparse), (10, dense))
    }

  property("BloomFilter is a Monoid") {
    commutativeMonoidLaws[bf.Hash]
  }

  property("++ is the same as plus") {
    forAll((a: bf.Hash, b: bf.Hash) => Equiv[bf.Hash].equiv(a ++ b, Monoid.plus(a, b)))
  }

  property("the distance between a filter and itself should be 0") {
    forAll((a: bf.Hash) => a.hammingDistance(a) == 0)
  }

  property(
    "the distance between a filter and an empty filter should be the number of bits" +
      "set in the existing filter"
  ) {
    forAll((a: bf.Hash) => a.hammingDistance(Monoid.zero) == a.numBits)
  }

  property("all equivalent filters should have 0 Hamming distance") {
    forAll { (a: bf.Hash, b: bf.Hash) =>
      if (Equiv[bf.Hash].equiv(a, b))
        a.hammingDistance(b) == 0
      else {
        val dist = a.hammingDistance(b)
        dist > 0 && dist <= a.width
      }
    }
  }

  property("distance between filters should be symmetrical") {
    forAll((a: bf.Hash, b: bf.Hash) => a.hammingDistance(b) == b.hammingDistance(a))
  }

  property("+ is the same as adding with create") {
    forAll { (a: bf.Hash, b: String) =>
      Equiv[bf.Hash].equiv(a + b, Monoid.plus(a, bf.create(b)))
    }
  }

  property("maybeContains is consistent with contains") {
    forAll((a: bf.Hash, b: String) => a.maybeContains(b) == a.contains(b).isTrue)
  }

  property("after + maybeContains is true") {
    forAll((a: bf.Hash, b: String) => (a + b).maybeContains(b))
  }

  property("checkAndAdd works like check the add") {
    forAll { (a: bf.Hash, b: String) =>
      val (next, check) = a.checkAndAdd(b)
      val next1 = a + b

      Equiv[bf.Hash].equiv(next, next1) &&
      check == a.contains(b)
    }
  }

  property("a ++ a = a for BF") {
    forAll((a: bf.Hash) => Equiv[bf.Hash].equiv(a ++ a, a))
  }

  property("BF Instance has 1 or more BitSet") {
    forAll { (a: bf.Hash) =>
      a match {
        case bf.Instance(bs) => bs.size >= 1
        case _               => true
      }
    }
  }

}

class ImmutableBloomFilterHashIndices extends CheckProperties {

  implicit val bf: Arbitrary[BloomFilter[String]] =
    Arbitrary {
      for {
        hashes <- Gen.choose(1, 10)
        width <- Gen.choose(100, 5000000)
      } yield BloomFilter[String](hashes, width)
    }

  property("Indices are non negative") {
    forAll((bf: BloomFilter[String], v: Long) => bf.hashToArray(v.toString).forall(e => e >= 0))
  }

  /**
   * This is the version of the Hash as of before the "negative values fix"
   */
  case class NegativeHash(numHashes: Int, width: Int) {
    val size = numHashes

    def apply(s: String): Stream[Int] = nextHash(s.getBytes, numHashes)

    private def splitLong(x: Long) = {
      val upper = math.abs(x >> 32).toInt
      val lower = math.abs(x << 32 >> 32).toInt
      (upper, lower)
    }

    private def nextHash(bytes: Array[Byte], k: Int, digested: Seq[Int] = Seq.empty): Stream[Int] =
      if (k == 0)
        Stream.empty
      else {
        val d = if (digested.isEmpty) {
          val (a, b) = MurmurHash128(k)(bytes)
          val (x1, x2) = splitLong(a)
          val (x3, x4) = splitLong(b)
          Seq(x1, x2, x3, x4)
        } else
          digested

        Stream.cons(d(0) % width, nextHash(bytes, k - 1, d.drop(1)))
      }
  }

  implicit val pairOfHashes: Arbitrary[(BloomFilter[String], NegativeHash)] =
    Arbitrary {
      for {
        hashes <- Gen.choose(1, 10)
        width <- Gen.choose(100, 5000000)
      } yield (BloomFilter[String](hashes, width), NegativeHash(hashes, width))
    }

  property(
    "Indices of the two versions of Hashes are the same, unless the first one contains negative index"
  ) {
    forAll { (pair: (BloomFilter[String], NegativeHash), v: Long) =>
      val s = v.toString
      val (bf, negativeHash) = pair
      val indices = negativeHash.apply(s)
      indices == bf.hashToArray(s).toStream || indices.exists(_ < 0)
    }
  }
}

class BloomFilterFalsePositives[T: Gen: Hash128](falsePositiveRate: Double) extends ApproximateProperty {

  type Exact = Set[T]
  type Approx = BloomFilter[T]#Hash

  type Input = T
  type Result = Boolean

  val maxNumEntries = 1000

  def exactGenerator =
    for {
      numEntries <- Gen.choose(1, maxNumEntries)
      set <- Gen.containerOfN[Set, T](numEntries, implicitly[Gen[T]])
    } yield set

  def makeApproximate(set: Set[T]) = {
    val bfMonoid = BloomFilter[T](set.size, falsePositiveRate)

    val values = set.toSeq
    bfMonoid.create(values: _*)
  }

  def inputGenerator(set: Set[T]) =
    for {
      randomValues <- Gen.listOfN[T](set.size, implicitly[Gen[T]])
      x <- Gen.oneOf((set ++ randomValues).toSeq)
    } yield x

  def exactResult(s: Set[T], t: T) = s.contains(t)

  def approximateResult(bf: BloomFilter[T]#Hash, t: T) = bf.contains(t)
}

class BloomFilterCardinality[T: Gen: Hash128] extends ApproximateProperty {

  type Exact = Set[T]
  type Approx = BloomFilter[T]#Hash

  type Input = Unit
  type Result = Long

  val maxNumEntries = 10000
  val falsePositiveRate = 0.01

  def exactGenerator =
    for {
      numEntries <- Gen.choose(1, maxNumEntries)
      set <- Gen.containerOfN[Set, T](numEntries, implicitly[Gen[T]])
    } yield set

  def makeApproximate(set: Set[T]) = {
    val bfMonoid = BloomFilter[T](set.size, falsePositiveRate)

    val values = set.toSeq
    bfMonoid.create(values: _*)
  }

  def inputGenerator(set: Set[T]) = Gen.const(())

  def exactResult(s: Set[T], u: Unit) = s.size
  def approximateResult(bf: BloomFilter[T]#Hash, u: Unit) = bf.size
}

class ImmutableBloomFilterProperties extends ApproximateProperties("BloomFilter") {
  import ApproximateProperty.toProp

  for (falsePositiveRate <- List(0.1, 0.01, 0.001)) {
    property(s"has small false positive rate with false positive rate = $falsePositiveRate") = {
      implicit val intGen = Gen.choose(1, 1000)
      toProp(new BloomFilterFalsePositives[Int](falsePositiveRate), 50, 50, 0.01)
    }
  }

  property("approximate cardinality") = {
    implicit val intGen = Gen.choose(1, 1000)
    toProp(new BloomFilterCardinality[Int], 50, 1, 0.01)
  }
}

class ImmutableBloomFilterTest extends AnyWordSpec with Matchers {

  val RAND = new scala.util.Random

  "BloomFilter" should {

    "be possible to create from an iterator" in {
      val bloomFilter = BloomFilter[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
      val entries = (0 until 100).map(_ => RAND.nextInt.toString)
      val bf = bloomFilter.create(entries.iterator)
      assert(bf.isInstanceOf[bloomFilter.Hash])
    }

    "be possible to create from a sequence" in {
      val bloomFilter = BloomFilter[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
      val entries = (0 until 100).map(_ => RAND.nextInt.toString)
      val bf = bloomFilter.create(entries: _*)
      assert(bf.isInstanceOf[bloomFilter.Hash])
    }

    "be possible to create from a BitSet" in {
      val bloomFilter = BloomFilter[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
      val entries = (0 until 100).map(_ => RAND.nextInt.toString)
      val bf = bloomFilter.create(entries: _*)

      val instance = bloomFilter.fromBitSet(bf.toBitSet)
      assert(instance.isSuccess)
    }

    "be possible to create from a empty BitSet" in {
      val bloomFilter = BloomFilter[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
      val instance = bloomFilter.fromBitSet(BitSet.empty)
      assert(instance.isSuccess)
    }

    "fail to create from a larger BitSet" in {
      val bloomFilter = BloomFilter[String](6, 0.01)
      val entries = (0 until 6).map(_ => RAND.nextInt.toString)
      val bf = bloomFilter.create(entries: _*)

      val instance = BloomFilter[String](6, 0.1).fromBitSet(bf.toBitSet)
      assert(instance.isFailure)
    }

    "identify all true positives" in {
      (0 to 100).foreach { _ =>
        val bloomFilter = BloomFilter[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
        val numEntries = 5
        val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
        val bf = bloomFilter.create(entries: _*)

        entries.foreach { i =>
          assert(bf.contains(i).isTrue)
        }
      }
    }

    "have small false positive rate" in {
      val iter = 10000

      Seq(0.1, 0.01, 0.001).foreach { fpProb =>
        val fps = (0 until iter).map { _ =>
          val numEntries = RAND.nextInt(10) + 1

          val bfMonoid = BloomFilter[String](numEntries, fpProb)

          val entries = RAND
            .shuffle((0 until 1000).toList)
            .take(numEntries + 1)
            .map(_.toString)
          val bf = bfMonoid.create(entries.drop(1): _*)

          if (bf.contains(entries(0)).isTrue) 1.0 else 0.0
        }

        val observedFpProb = fps.sum / fps.size

        // the 2.5 is a fudge factor to make the probability of it low
        // in tests
        assert(observedFpProb <= 2.5 * fpProb)
      }
    }

    "approximate cardinality" in {
      val bloomFilter = BloomFilter[String](10, 100000)

      Seq(10, 100, 1000, 10000).foreach { exactCardinality =>
        val items = (1 until exactCardinality).map(_.toString)
        val bf = bloomFilter.create(items: _*)
        val size = bf.size

        assert(size ~ exactCardinality)
        assert(size.min <= size.estimate)
        assert(size.max >= size.estimate)
      }
    }

    "work as an Aggregator" in {
      (0 to 10).foreach { _ =>
        val bloomFilter = BloomFilter[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
        import bloomFilter.aggregator

        val numEntries = 5
        val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
        val bf = aggregator(entries)

        entries.foreach(i => assert(bf.contains(i.toString).isTrue))
      }
    }

    "not serialize @transient dense Instance" in {
      val bloomFilter = BloomFilter[String](10, 0.1)

      def serialize(bf: bloomFilter.Hash): Array[Byte] = {
        val stream = new ByteArrayOutputStream
        val out = new ObjectOutputStream(stream)
        out.writeObject(bf)
        out.close()
        stream.close()
        stream.toByteArray
      }

      val bf = bloomFilter.create((1 until 10).map(_.toString): _*)
      val bytesBeforeSizeCalled = Bytes(serialize(bf))
      val beforeSize = bf.size
      assert(bf.contains("1").isTrue)
      val bytesAfterSizeCalled = Bytes(serialize(bf))
      assert(bytesBeforeSizeCalled.size == bytesAfterSizeCalled.size)
      assert(beforeSize == bf.size)
    }

    /**
     * this test failed before the fix for https://github.com/twitter/algebird/issues/229
     */
    "not have negative hash values" in {
      val bf = BloomFilter[String](2, 4752800)
      val s = "7024497610539761509"
      val index = bf.hashToArray(s).head

      assert(index >= 0)
    }
  }

  "BloomFilter method `checkAndAdd`" should {

    "be identical to method `+`" in {
      (0 to 100).foreach { _ =>
        val bloomFilter = BloomFilter[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
        import bloomFilter._

        val numEntries = 5
        val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
        val bf = bloomFilter.create(entries: _*)
        entries
          .map(entry => (entry, bloomFilter.create(entry)))
          .foldLeft((Monoid.zero, Monoid.zero)) { case ((left, leftAlt), (entry, _)) =>
            val (newLeftAlt, contained) = leftAlt.checkAndAdd(entry)
            left.contains(entry) shouldBe contained
            (left + entry, newLeftAlt)
          }

        entries.foreach(i => assert(bf.contains(i.toString).isTrue))
      }
    }
  }

  "BloomFilters" should {
    "be able to compute Hamming distance to each other" in {
      import BloomFilterTestUtils._

      val bf = BloomFilter[String](3, 64)

      val firstBloomFilter = bf.create(Seq("A").iterator)
      val secondBloomFilter = bf.create(Seq("C").iterator)

      val distance1 = firstBloomFilter.hammingDistance(secondBloomFilter)
      assert(distance1 === 4)

      val thirdBloomFilter = bf.create(Seq("A", "B", "C").iterator)
      // Make it dense to make sure that that case is also covered
      // even though these examples are small and thus sparse.
      val forthBloomFilter = toDense(bf)(bf.create(Seq("C", "D", "E").iterator))

      val distance2 = thirdBloomFilter.hammingDistance(forthBloomFilter)
      assert(distance2 === 8)

      val emptyBloomFilter = bf.create(Iterator.empty)
      val distanceToEmpty = thirdBloomFilter.hammingDistance(emptyBloomFilter)
      assert(distanceToEmpty === thirdBloomFilter.numBits)

    }
  }

}
