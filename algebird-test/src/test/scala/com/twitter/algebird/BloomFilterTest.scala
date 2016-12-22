package com.twitter.algebird

import java.io.{ ByteArrayOutputStream, ObjectOutputStream }

import org.scalacheck.{ Arbitrary, Gen, Properties }
import org.scalatest.{ Matchers, WordSpec }
import org.scalacheck.Prop._

class BloomFilterLaws extends CheckProperties {

  import com.twitter.algebird.BaseProperties._

  val NUM_HASHES = 6
  val WIDTH = 32

  implicit val bfMonoid = new BloomFilterMonoid[String](NUM_HASHES, WIDTH)
  implicit val bfGen =
    Arbitrary {
      for (v <- Gen.choose(0, 10000)) yield (bfMonoid.create(v.toString))
    }

  property("BloomFilter is a Monoid") {
    monoidLaws[BF[String]]
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

    private def nextHash(bytes: Array[Byte], k: Int, digested: Seq[Int] = Seq.empty): Stream[Int] = {
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
  }

  implicit val pairOfHashes: Arbitrary[(BFHash[String], NegativeBFHash)] =
    Arbitrary {
      for {
        hashes <- Gen.choose(1, 10)
        width <- Gen.choose(100, 5000000)
      } yield (BFHash[String](hashes, width), NegativeBFHash(hashes, width))
    }

  property("Indices of the two versions of BFHashes are the same, unless the first one contains negative index") {
    forAll { (pair: (BFHash[String], NegativeBFHash), v: Long) =>
      val s = v.toString
      val (hash, negativeHash) = pair
      val indices = negativeHash.apply(s)
      indices == hash.apply(s) || indices.exists(_ < 0)
    }
  }
}

class BloomFilterFalsePositives[T: Gen: Hash128](falsePositiveRate: Double) extends ApproximateProperty {

  type Exact = Set[T]
  type Approx = BF[T]

  type Input = T
  type Result = Boolean

  val maxNumEntries = 1000

  def exactGenerator = for {
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

  def exactGenerator = for {
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

class BloomFilterTest extends WordSpec with Matchers {

  val RAND = new scala.util.Random

  "BloomFilter" should {

    "identify all true positives" in {
      (0 to 100).foreach{
        _ =>
          {
            val bfMonoid = new BloomFilterMonoid[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
            val numEntries = 5
            val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
            val bf = bfMonoid.create(entries: _*)

            entries.foreach{ i =>
              assert(bf.contains(i.toString).isTrue)
            }
          }
      }
    }

    "have small false positive rate" in {
      val iter = 10000

      Seq(0.1, 0.01, 0.001).foreach { fpProb =>
        {
          val fps = (0 until iter).par.map{
            _ =>
              {
                val numEntries = RAND.nextInt(10) + 1

                val bfMonoid = BloomFilter[String](numEntries, fpProb)

                val entries = RAND.shuffle((0 until 1000).toList).take(numEntries + 1).map(_.toString)
                val bf = bfMonoid.create(entries.drop(1): _*)

                if (bf.contains(entries(0)).isTrue) 1.0 else 0.0
              }
          }

          val observedFpProb = fps.sum / fps.size

          assert(observedFpProb <= 2 * fpProb)
        }
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
      (0 to 10).foreach{
        _ =>
          {
            val aggregator = BloomFilterAggregator[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
            val numEntries = 5
            val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
            val bf = aggregator(entries)

            entries.foreach { i =>
              assert(bf.contains(i.toString).isTrue)
            }
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
      (0 to 100).foreach {
        _ =>
          {
            val bfMonoid = new BloomFilterMonoid[String](RAND.nextInt(5) + 1, RAND.nextInt(64) + 32)
            val numEntries = 5
            val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
            val bf = bfMonoid.create(entries: _*)
            val bfWithCheckAndAdd = entries
              .map { entry => (entry, bfMonoid.create(entry)) }
              .foldLeft((bfMonoid.zero, bfMonoid.zero)) {
                case ((left, leftAlt), (entry, right)) =>
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
  }
}
