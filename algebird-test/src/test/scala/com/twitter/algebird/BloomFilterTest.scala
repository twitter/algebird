package com.twitter.algebird

import org.specs._

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


class BloomFilterTest extends Specification {
  noDetailedDiffs()

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
  }
}
