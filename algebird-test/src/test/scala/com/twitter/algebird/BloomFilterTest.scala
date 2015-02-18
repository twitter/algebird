package com.twitter.algebird

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary }
import java.io.{ ObjectOutputStream, ByteArrayOutputStream }

class BloomFilterLaws extends PropSpec with PropertyChecks with ShouldMatchers {
  import BaseProperties._

  val NUM_HASHES = 6
  val WIDTH = 32
  val SEED = 1

  implicit val bfMonoid = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)
  implicit val bfGen =
    Arbitrary {
      for (v <- Gen.choose(0, 10000)) yield (bfMonoid.create(v.toString))
    }

  property("BloomFilter is a Monoid") {
    monoidLaws[BF]
  }
}

class BFHashIndices extends PropSpec with PropertyChecks with ShouldMatchers {
  val NUM_HASHES = 10
  val WIDTH = 4752800

  val SEED = 1

  implicit val bfHash: Arbitrary[BFHash] =
    Arbitrary {
      for {
        hashes <- Gen.choose(1, 10)
        width <- Gen.choose(100, 5000000)
      } yield BFHash(hashes, width, SEED)
    }

  property("Indices are non negative") {
    forAll { (hash: BFHash, v: Long) =>
      hash.apply(v.toString).foreach { e =>
        assert(e >= 0)
      }
    }
  }

  /**
   *   This is the version of the BFHash as of before the "negative values fix"
   */
  case class NegativeBFHash(numHashes: Int, width: Int, seed: Long = 0L) {
    val size = numHashes

    def apply(s: String) = nextHash(s.getBytes, numHashes)

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

  implicit val pairOfHashes: Arbitrary[(BFHash, NegativeBFHash)] =
    Arbitrary {
      for {
        hashes <- Gen.choose(1, 10)
        width <- Gen.choose(100, 5000000)
      } yield (BFHash(hashes, width, SEED), NegativeBFHash(hashes, width, SEED))
    }

  property("Indices of the two versions of BFHashes are the same, unless the first one contains negative index") {
    forAll { (pair: (BFHash, NegativeBFHash), v: Long) =>
      val s = v.toString
      val (hash, negativeHash) = pair
      val indices = negativeHash.apply(s)
      assert(indices == hash.apply(s) || indices.exists(_ < 0))
    }
  }
}

class BloomFilterTest extends WordSpec with ShouldMatchers {

  val SEED = 1
  val RAND = new scala.util.Random

  "BloomFilter" should {

    "identify all true positives" in {
      (0 to 100).foreach{
        _ =>
          {
            val bfMonoid = new BloomFilterMonoid(RAND.nextInt(5) + 1, RAND.nextInt(64) + 32, SEED)
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

                val bfMonoid = BloomFilter(numEntries, fpProb, SEED)

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
      val bfMonoid = BloomFilterMonoid(10, 100000, SEED)
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
            val aggregator = BloomFilterAggregator(RAND.nextInt(5) + 1, RAND.nextInt(64) + 32, SEED)
            val numEntries = 5
            val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
            val bf = aggregator(entries)

            entries.foreach { i =>
              assert(bf.contains(i.toString).isTrue)
            }
          }
      }
    }

    "not serialize BFInstance" in {
      def serialize(bf: BF) = {
        val stream = new ByteArrayOutputStream()
        val out = new ObjectOutputStream(stream)
        out.writeObject(bf)
        out.close()
        stream.close()
        stream.toByteArray
      }

      val items = (1 until 10).map { _.toString }
      val bf = BloomFilter(10, 0.1, SEED).create(items: _*)
      val bytesBeforeSizeCalled = new String(serialize(bf))
      bf.size

      assert(bf.contains("1").isTrue)

      val bytesAfterSizeCalled = new String(serialize(bf))
      assert(bytesBeforeSizeCalled == bytesAfterSizeCalled)
    }

    /**
     * this test failed before the fix for https://github.com/twitter/algebird/issues/229
     */
    "not have negative hash values" in {
      val NUM_HASHES = 2
      val WIDTH = 4752800
      val bfHash = BFHash(NUM_HASHES, WIDTH, SEED)
      val s = "7024497610539761509"
      val index = bfHash.apply(s).head

      assert(index >= 0)
    }
  }
}
