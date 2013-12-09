package com.twitter.algebird

import org.specs2.mutable._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose
import java.io.{ObjectOutputStream, ByteArrayOutputStream}

object BloomFilterLaws extends Properties("BloomFilter") {
  import BaseProperties._

  val NUM_HASHES = 6
  val WIDTH = 32
  val SEED = 1

  implicit val bfMonoid = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)
  implicit val bfGen =
    Arbitrary {
      for (v <- choose(0, 10000)) yield (bfMonoid.create(v.toString))
    }

  property("BloomFilter is a Monoid") = monoidLaws[BF]
}

object BFHashIndices extends Properties("BFHash") {
  import org.scalacheck.Prop.forAll
  
  val NUM_HASHES = 10
  val WIDTH = 4752800
  
  val SEED = 1

  implicit val bfHashIndices: Arbitrary[Stream[Int]] =
    Arbitrary {
      for {
        hashes <- choose(1, 10)
        width <- choose(100, 5000000)
        v <- choose(0, 100000)
      } yield {BFHash(hashes, width, SEED).apply(v.toString)}
    }
   
  property("Indices are non negative") = forAll{ hashIndices: Stream[Int] => hashIndices.forall(_ >= 0)} 
   
  /**
   *   This is the version of the BFHash before the negative values fix 
   */
  case class NegativeBFHash(numHashes: Int, width: Int, seed: Long = 0L) extends Function1[String, Iterable[Int]]{
    val size = numHashes

    def apply(s: String) = nextHash(s.getBytes, numHashes)

    private def splitLong(x: Long) = {
      val upper = math.abs(x >> 32).toInt
      val lower = math.abs((x << 32) >> 32).toInt
      (upper, lower)
    }

    private def nextHash(bytes: Array[Byte], k: Int, digested: Seq[Int] = Seq.empty): Stream[Int] = {
      if(k == 0)
        Stream.empty
      else{
        val d = if(digested.isEmpty){
          val (a, b) = MurmurHash128(k)(bytes)
          val (x1, x2) = splitLong(a)
          val (x3, x4) = splitLong(b)
          Seq(x1, x2, x3, x4)
        }else
          digested

        Stream.cons(d(0) % width, nextHash(bytes, k - 1, d.drop(1)))
      }
    }
  }
  
  val negativeBFHash = NegativeBFHash(NUM_HASHES, WIDTH, SEED)
  val bfHash = BFHash(NUM_HASHES, WIDTH, SEED)

  property("Indices of the two versions of BFHashes are the same, unless the first one contains negative index") = forAll{ long: Long =>
    val s = long.toString
    val indices = negativeBFHash.apply(s)
    indices == bfHash.apply(s) || indices.exists(_ < 0)
  }
}


class BloomFilterTest extends Specification {

  val SEED = 1
  val RAND = new scala.util.Random

  "BloomFilter" should {

    "identify all true positives" in {
      (0 to 100).foreach{
        _ => {
          val bfMonoid = new BloomFilterMonoid(RAND.nextInt(5)+1, RAND.nextInt(64)+32, SEED)
          val numEntries = 5
          val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
          val bf = bfMonoid.create(entries: _*)

          entries.foreach{
            i => bf.contains(i.toString).isTrue must be_==(true)
          }
        }
      }
    }

    "have small false positive rate" in {
      val iter = 10000

      Seq(0.1, 0.01, 0.001).foreach{
        fpProb => {
          val fps = (0 until iter).par.map{
            _ => {
              val numEntries = RAND.nextInt(10) + 1

              val bfMonoid = BloomFilter(numEntries, fpProb, SEED)

              val entries = RAND.shuffle((0 until 1000).toList).take(numEntries + 1).map(_.toString)
              val bf = bfMonoid.create(entries.drop(1): _*)

              if(bf.contains(entries(0)).isTrue) 1.0 else 0.0
            }
          }

          val observedFpProb = fps.sum / fps.size

          observedFpProb must be_<=(2 * fpProb)
        }
      }
    }

    "approximate cardinality" in {
      val bfMonoid = BloomFilterMonoid(10, 100000, SEED)
      Seq(10, 100, 1000, 10000).foreach { exactCardinality =>
        val items = (1 until exactCardinality).map { _.toString }
        val bf = bfMonoid.create(items: _*)
        val size = bf.size

        (size ~ exactCardinality) must be_==(true)
        size.min must be_<=(size.estimate)
        size.max must be_>=(size.estimate)
      }
    }

    "work as an Aggregator" in {
      (0 to 10).foreach{
        _ => {
          val aggregator = BloomFilterAggregator(RAND.nextInt(5)+1, RAND.nextInt(64)+32, SEED)
          val numEntries = 5
          val entries = (0 until numEntries).map(_ => RAND.nextInt.toString)
          val bf = aggregator(entries)

          entries.foreach{
            i => bf.contains(i.toString).isTrue must be_==(true)
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
      bf.contains("1").isTrue must be_==(true)
      val bytesAfterSizeCalled = new String(serialize(bf))
      bytesBeforeSizeCalled mustEqual bytesAfterSizeCalled
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

      index must be_>=(0)
    }
  }
}
