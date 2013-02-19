package com.twitter.algebird

import org.specs._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose

import java.lang.AssertionError
import java.util.Arrays

object HyperLogLogLaws extends Properties("HyperLogLog") {
  import BaseProperties._
  import HyperLogLog._

  implicit val hllMonoid = new HyperLogLogMonoid(5) //5 bits
  implicit val hllGen = Arbitrary { for(
      v <- choose(0,10000)
    ) yield (hllMonoid(v))
  }

  property("HyperLogLog is a Monoid") = monoidLaws[HLL]
}

class HyperLogLogTest extends Specification {
  noDetailedDiffs()
  import HyperLogLog._ //Get the implicit int2bytes, long2Bytes

  val r = new java.util.Random

  def exactCount[T](it : Iterable[T]) : Int = it.toSet.size
  def approxCount[T <% Array[Byte]](bits : Int, it : Iterable[T]) = {
    implicit val hll = new HyperLogLogMonoid(bits)
    hll.sizeOf(Monoid.sum(it.map { hll(_) })).estimate.toDouble
  }

  def aveErrorOf(bits : Int) : Double = 1.04/scala.math.sqrt(1 << bits)

  def exactIntersect[T](it : Seq[Iterable[T]]) : Int = {
    it.foldLeft(Set[T]()) { (old, newS) => old ++ (newS.toSet) }.size
  }
  def approxIntersect[T <% Array[Byte]](bits : Int, it : Seq[Iterable[T]]) : Double = {
    implicit val hll = new HyperLogLogMonoid(bits)
    //Map each iterable to a HLL instance:
    val seqHlls = it.map { iter => Monoid.sum(iter.view.map { hll(_) }) }
    hll.intersectionSize(seqHlls).estimate.toDouble
  }

  def test(bits : Int) {
    val data = (0 to 10000).map { i => r.nextInt(1000) }
    val exact = exactCount(data).toDouble
    scala.math.abs(exact - approxCount(bits, data)) / exact must be_<(3.5 * aveErrorOf(bits))
  }
  def testLong(bits : Int) {
    val data = (0 to 10000).map { i => r.nextLong }
    val exact = exactCount(data).toDouble
    scala.math.abs(exact - approxCount(bits, data)) / exact must be_<(3.5 * aveErrorOf(bits))
  }
  def testLongIntersection(bits : Int, sets : Int) {
    val data : Seq[Iterable[Int]] = (0 until sets).map { idx =>
      (0 to 1000).map { i => r.nextInt(100) }
    }.toSeq
    val exact = exactIntersect(data)
    val errorMult = scala.math.pow(2.0, sets) - 1.0
    scala.math.abs(exact - approxIntersect(bits, data)) / exact must be_<(errorMult *
      aveErrorOf(bits))
  }

  "HyperLogLog" should {
     "count with 5-bits" in {
        test(5)
        testLong(5)
     }
     "count with 6-bits" in {
        test(6)
        testLong(6)
     }
     "count with 7-bits" in {
        test(7)
        testLong(7)
     }
     "count with 10-bits" in {
        test(10)
        testLong(10)
     }
     "count intersections of 2" in { testLongIntersection(10,2) }
     "count intersections of 3" in { testLongIntersection(10,3) }
     "count intersections of 4" in { testLongIntersection(10,4) }

     "throw error for differently sized HLL instances" in {
        val bigMon = new HyperLogLogMonoid(5)
        val smallMon = new HyperLogLogMonoid(4)
        val larger = bigMon(1) // uses implicit long2Bytes to make 8 byte array
        val smaller = smallMon(1) // uses implicit int2Bytes to make 4 byte array
        (larger + smaller) must throwA[AssertionError]
     }
     "Correctly serialize" in {
       (4 to 20).foreach { bits =>
         val mon = new HyperLogLogMonoid(bits)
         // Zero
         verifySerialization(mon.zero)
         // One j
         verifySerialization(mon(12))
         // Two j's
         verifySerialization(mon(12) + mon(13))
         // Many j's
         val manyJ = Monoid.sum((1 to 1000 by 77).map(mon(_)))(mon)
         verifySerialization(manyJ)
         // Explicitly dense
         verifySerialization(manyJ.toDenseHLL)
       }
     }
    "be consistent for sparse vs. dense" in {
      val mon = new HyperLogLogMonoid(12)
      val data = (1 to 100).map { _ => r.nextLong }
      val partialSums = data.foldLeft(Seq(mon.zero)) { (seq,value) => seq :+ (seq.last + mon(value)) }
      // Now the ith entry of partialSums (0-based) is an HLL structure for i underlying elements
      partialSums.foreach { hll =>
        hll.isInstanceOf[SparseHLL] must beTrue
        hll.size must be_==(hll.toDenseHLL.size)
        hll.zeroCnt must be_==(hll.toDenseHLL.zeroCnt)
        hll.z must be_==(hll.toDenseHLL.z)
        hll.approximateSize must be_==(hll.toDenseHLL.approximateSize)
        hll.estimatedSize must be_==(hll.toDenseHLL.estimatedSize)
      }
    }
    "properly convert to dense" in {
      val mon = new HyperLogLogMonoid(10)
      val data = (1 to 200).map { _ => r.nextLong }
      val partialSums = data.foldLeft(Seq(mon.zero)) { (seq,value) => seq :+ (seq.last + mon(value)) }
      partialSums.foreach { hll =>
        if (hll.size - hll.zeroCnt <= 64) {
          hll.isInstanceOf[SparseHLL] must beTrue
        } else {
          hll.isInstanceOf[DenseHLL] must beTrue
        }
      }
    }
    "properly do a batch create" in {
      val mon = new HyperLogLogMonoid(10)
      val data = (1 to 200).map { _ => r.nextLong }
      val partialSums = data.foldLeft(IndexedSeq(mon.zero)) { (seq,value) => seq :+ (seq.last + mon(value)) }
      (1 to 200).map { n =>
        partialSums(n) must be_==(mon.batchCreate(data.slice(0, n)))
      }
    }

    def verifySerialization(h : HLL) {
      fromBytes(toBytes(h)) must be_==(h)
      fromByteBuffer(java.nio.ByteBuffer.wrap(toBytes(h))) must be_==(h)
    }
  }
}
