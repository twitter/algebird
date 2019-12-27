package com.twitter.algebird

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object BloomFilterTestUtils {
  def toSparse[A](bf: BF[A]): BFSparse[A] = bf match {
    case BFZero(hashes, width) => BFSparse(hashes, RichCBitSet(), width)
    case BFItem(item, hashes, width) =>
      BFSparse(hashes, RichCBitSet.fromArray(hashes(item)), width)
    case bfs @ BFSparse(_, _, _) => bfs
    case BFInstance(hashes, bitset, width) =>
      BFSparse(hashes, RichCBitSet.fromBitSet(bitset), width)
  }

  def toDense[A](bf: BF[A]): BFInstance[A] = bf match {
    case BFZero(hashes, width) => BFInstance.empty[A](hashes, width)
    case BFItem(item, hashes, width) =>
      val bs = LongBitSet.empty(width)
      bs += hashes(item)
      BFInstance(hashes, bs.toBitSetNoCopy, width)
    case bfs @ BFSparse(_, _, _)   => bfs.dense
    case bfi @ BFInstance(_, _, _) => bfi
  }
}

class BloomFilterLaws extends CheckProperties {

  import com.twitter.algebird.BaseProperties._
  import BloomFilterTestUtils._

  val NUM_HASHES = 6
  val WIDTH = 32

  implicit val bfMonoid = new BloomFilterMonoid[String](NUM_HASHES, WIDTH)

  implicit val bfGen: Arbitrary[BF[String]] =
    Arbitrary {
      val item = Gen.choose(0, 10000).map { v =>
        bfMonoid.create(v.toString)
      }
      val zero = Gen.const(bfMonoid.zero)
      val sparse = Gen.listOf(item).map { its =>
        toSparse(bfMonoid.sum(its))
      }
      val dense = Gen.listOf(item).map { its =>
        toDense(bfMonoid.sum(its))
      }
      Gen.frequency((1, zero), (5, item), (10, sparse), (10, dense))
    }

  property("BloomFilter is a Monoid") {
    commutativeMonoidLaws[BF[String]]
  }

  property("++ is the same as plus") {
    forAll { (a: BF[String], b: BF[String]) =>
      Equiv[BF[String]].equiv(a ++ b, bfMonoid.plus(a, b))
    }
  }

  property("the distance between a filter and itself should be 0") {
    forAll { (a: BF[String]) =>
      a.hammingDistance(a) == 0
    }
  }

  property(
    "the distance between a filter and an empty filter should be the number of bits" +
      "set in the existing filter"
  ) {
    forAll { (a: BF[String]) =>
      a.hammingDistance(bfMonoid.zero) == a.numBits
    }
  }

  property("all equivalent filters should have 0 Hamming distance") {
    forAll { (a: BF[String], b: BF[String]) =>
      if (Equiv[BF[String]].equiv(a, b))
        a.hammingDistance(b) == 0
      else {
        val dist = a.hammingDistance(b)
        (dist > 0) && (dist <= a.width)
      }
    }
  }

  property("distance between filters should be symmetrical") {
    forAll { (a: BF[String], b: BF[String]) =>
      a.hammingDistance(b) == b.hammingDistance(a)
    }
  }

  property("+ is the same as adding with create") {
    forAll { (a: BF[String], b: String) =>
      Equiv[BF[String]].equiv(a + b, bfMonoid.plus(a, bfMonoid.create(b)))
    }
  }

  property("maybeContains is consistent with contains") {
    forAll { (a: BF[String], b: String) =>
      a.maybeContains(b) == a.contains(b).isTrue
    }
  }

  property("after + maybeContains is true") {
    forAll { (a: BF[String], b: String) =>
      (a + b).maybeContains(b)
    }
  }

  property("checkAndAdd works like check the add") {
    forAll { (a: BF[String], b: String) =>
      val (next, check) = a.checkAndAdd(b)
      val next1 = a + b

      Equiv[BF[String]].equiv(next, next1) &&
      (check == a.contains(b))
    }
  }

  property(".dense returns an equivalent BF") {
    forAll { (a: BF[String]) =>
      Equiv[BF[String]].equiv(toSparse(a).dense, a)
    }
  }
  property("a ++ a = a for BF") {
    forAll { (a: BF[String]) =>
      Equiv[BF[String]].equiv(a ++ a, a)
    }
  }
}

class BFHashIndices extends CheckProperties {

  val NUM_HASHES = 10
  val WIDTH = 4752800

  implicit val bfHash: Arbitrary[BFHash[String]] =
    Arbitrary {
      for {
        hashes <- Gen.choose(1, 10)
        width <- Gen.choose(100, 5000000)
      } yield BFHash[String](hashes, width)
    }

  property("Indices are non negative") {
    forAll { (hash: BFHash[String], v: Long) =>
      hash.apply(v.toString).forall { e =>
        e >= 0
      }
    }
  }

  /**
   *   This is the version of the BFHash as of before the "negative values fix"
   */
  case class NegativeBFHash(numHashes: Int, width: Int) {
    val size = numHashes

    def apply(s: String): Stream[Int] = nextHash(s.getBytes, numHashes)

    private def splitLong(x: Long) = {
      val upper = math.abs(x >> 32).toInt
      val lower = math.abs((x << 32) >> 32).toInt
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

  implicit val pairOfHashes: Arbitrary[(BFHash[String], NegativeBFHash)] =
    Arbitrary {
      for {
        hashes <- Gen.choose(1, 10)
        width <- Gen.choose(100, 5000000)
      } yield (BFHash[String](hashes, width), NegativeBFHash(hashes, width))
    }

  property(
    "Indices of the two versions of BFHashes are the same, unless the first one contains negative index"
  ) {
    forAll { (pair: (BFHash[String], NegativeBFHash), v: Long) =>
      val s = v.toString
      val (hash, negativeHash) = pair
      val indices = negativeHash.apply(s)
      (indices == (hash.apply(s).toStream)) || indices.exists(_ < 0)
    }
  }
}

class BloomFilterFalsePositives[T: Gen: Hash128](falsePositiveRate: Double) extends ApproximateProperty {

  type Exact = Set[T]
  type Approx = BF[T]

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

  def approximateResult(bf: BF[T], t: T) = bf.contains(t)
}

class BloomFilterCardinality[T: Gen: Hash128] extends ApproximateProperty {

  type Exact = Set[T]
  type Approx = BF[T]

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
  def approximateResult(bf: BF[T], u: Unit) = bf.size
}

class BloomFilterProperties extends ApproximateProperties("BloomFilter") {
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

class BloomFilterTest extends AnyWordSpec with Matchers {

  val RAND = new scala.util.Random

  "BloomFilter" should {

    "be possible to create from an iterator" in {
      val bfMonoid = new BloomFilterMonoid[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
      val entries = (0 until 100).map(_ => RAND.nextInt.toString)
      val bf = bfMonoid.create(entries.iterator)
      assert(bf.isInstanceOf[BF[String]])
    }

    "be possible to create from a sequence" in {
      val bfMonoid = new BloomFilterMonoid[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
      val entries = (0 until 100).map(_ => RAND.nextInt.toString)
      val bf = bfMonoid.create(entries: _*)
      assert(bf.isInstanceOf[BF[String]])
    }

    "identify all true positives" in {
      (0 to 100).foreach { _ =>
        val bfMonoid = new BloomFilterMonoid[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
        val numEntries = 5
        val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
        val bf = bfMonoid.create(entries: _*)

        entries.foreach { i =>
          assert(bf.contains(i.toString).isTrue)
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
      val bfMonoid = BloomFilterMonoid[String](10, 100000)
      Seq(10, 100, 1000, 10000).foreach { exactCardinality =>
        val items = (1 until exactCardinality).map { _.toString }
        val bf = bfMonoid.create(items: _*)
        val size = bf.size

        assert(size ~ exactCardinality)
        assert(size.min <= size.estimate)
        assert(size.max >= size.estimate)
      }
    }

    "work as an Aggregator" in {
      (0 to 10).foreach { _ =>
        val aggregator = BloomFilterAggregator[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
        val numEntries = 5
        val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
        val bf = aggregator(entries)

        entries.foreach { i =>
          assert(bf.contains(i.toString).isTrue)
        }
      }
    }

    "not serialize @transient dense BFInstance" in {
      def serialize(bf: BF[String]): Array[Byte] = {
        val stream = new ByteArrayOutputStream()
        val out = new ObjectOutputStream(stream)
        out.writeObject(bf)
        out.close()
        stream.close()
        stream.toByteArray
      }

      val items = (1 until 10).map(_.toString)
      val bf = BloomFilter[String](10, 0.1).create(items: _*)
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
      val NUM_HASHES = 2
      val WIDTH = 4752800
      val bfHash = BFHash[String](NUM_HASHES, WIDTH)
      val s = "7024497610539761509"
      val index = bfHash.apply(s).head

      assert(index >= 0)
    }
  }

  "BloomFilter method `checkAndAdd`" should {

    "be identical to method `+`" in {
      (0 to 100).foreach { _ =>
        val bfMonoid = new BloomFilterMonoid[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
        val numEntries = 5
        val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
        val bf = bfMonoid.create(entries: _*)
        entries
          .map { entry =>
            (entry, bfMonoid.create(entry))
          }
          .foldLeft((bfMonoid.zero, bfMonoid.zero)) {
            case ((left, leftAlt), (entry, _)) =>
              val (newLeftAlt, contained) = leftAlt.checkAndAdd(entry)
              left.contains(entry) shouldBe contained
              (left + entry, newLeftAlt)
          }

        entries.foreach { i =>
          assert(bf.contains(i.toString).isTrue)
        }
      }
    }
  }

  "BloomFilters" should {
    "be able to compute Hamming distance to each other" in {
      import BloomFilterTestUtils._

      def createBFWithItems(entries: Seq[String]): BF[String] = {
        val numOfHashes = 3
        val width = 64
        val bfMonoid = new BloomFilterMonoid[String](numOfHashes, width)
        bfMonoid.create(entries.iterator)
      }

      val firstBloomFilter = createBFWithItems(Seq("A"))
      val secondBloomFilter = createBFWithItems(Seq("C"))

      val distance1 = firstBloomFilter.hammingDistance(secondBloomFilter)
      assert(distance1 === 4)

      val thirdBloomFilter = createBFWithItems(Seq("A", "B", "C"))
      // Make it dense to make sure that that case is also covered
      // even though these examples are small and thus sparse.
      val forthBloomFilter = toDense(createBFWithItems(Seq("C", "D", "E")))

      val distance2 = thirdBloomFilter.hammingDistance(forthBloomFilter)
      assert(distance2 === 8)

      val emptyBloomFilter = createBFWithItems(List())
      val distanceToEmpty = thirdBloomFilter.hammingDistance(emptyBloomFilter)
      assert(distanceToEmpty === thirdBloomFilter.numBits)

    }
  }

}
