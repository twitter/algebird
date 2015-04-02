package com.twitter.algebird

import org.scalatest._

import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary }

import scala.collection.BitSet

import java.lang.AssertionError
import java.util.Arrays

object ReferenceHyperLogLog {

  /** Reference implementation of jRhoW to compare optimizations against */
  def bytesToBitSet(in: Array[Byte]): BitSet = {
    BitSet(in.zipWithIndex.map { bi => (bi._1, bi._2 * 8) }
      .flatMap { byteToIndicator(_) }: _*)
  }
  def byteToIndicator(bi: (Byte, Int)): Seq[Int] = {
    (0 to 7).flatMap { i =>
      if (((bi._1 >> (7 - i)) & 1) == 1) {
        Vector(bi._2 + i)
      } else {
        Vector[Int]()
      }
    }
  }

  def jRhoW(in: Array[Byte], bits: Int): (Int, Byte) = {
    val onBits = bytesToBitSet(in)
    (onBits.filter { _ < bits }.map { 1 << _ }.sum,
      (onBits.filter { _ >= bits }.min - bits + 1).toByte)
  }

}

class HyperLogLogLaws extends CheckProperties {
  import BaseProperties._
  import HyperLogLog._

  implicit val hllHasAdditionOperatorAndZero = new HyperLogLogHasAdditionOperatorAndZero(5) //5 bits

  implicit val hllGen = Arbitrary {
    for (
      v <- Gen.choose(0, 10000)
    ) yield (hllHasAdditionOperatorAndZero(v))
  }

  property("HyperLogLog is a HasAdditionOperatorAndZero") {
    monoidLawsEq[HLL]{ _.toDenseHLL == _.toDenseHLL }
  }

}

/* Ensure jRhoW matches referenceJRhoW */
class jRhoWMatchTest extends PropSpec with PropertyChecks with Matchers {
  import HyperLogLog._

  implicit val hashGen = Arbitrary { Gen.containerOfN[Array, Byte](16, Arbitrary.arbitrary[Byte]) }
  /* For some reason choose in this version of scalacheck
  is bugged so I need the suchThat clause */
  implicit val bitsGen = Arbitrary { Gen.choose(4, 31) suchThat (x => x >= 4 && x <= 31) }

  property("jRhoW matches referenceJRhoW") {
    forAll { (in: Array[Byte], bits: Int) =>
      assert(jRhoW(in, bits) == ReferenceHyperLogLog.jRhoW(in, bits))
    }
  }
}

class HyperLogLogTest extends WordSpec with Matchers {

  import HyperLogLog._ //Get the implicit int2bytes, long2Bytes

  val r = new java.util.Random

  def exactCount[T](it: Iterable[T]): Int = it.toSet.size
  def approxCount[T <% Array[Byte]](bits: Int, it: Iterable[T]) = {
    val hll = new HyperLogLogHasAdditionOperatorAndZero(bits)
    hll.sizeOf(hll.sum(it.map { hll(_) })).estimate.toDouble
  }

  def aveErrorOf(bits: Int): Double = 1.04 / scala.math.sqrt(1 << bits)

  def exactIntersect[T](it: Seq[Iterable[T]]): Int = {
    it.foldLeft(Set[T]()) { (old, newS) => old ++ (newS.toSet) }.size
  }
  def approxIntersect[T <% Array[Byte]](bits: Int, it: Seq[Iterable[T]]): Double = {
    val hll = new HyperLogLogHasAdditionOperatorAndZero(bits)
    //Map each iterable to a HLL instance:
    val seqHlls = it.map { iter => hll.sum(iter.view.map { hll(_) }) }
    hll.intersectionSize(seqHlls).estimate.toDouble
  }

  def test(bits: Int) {
    val data = (0 to 10000).map { i => r.nextInt(1000) }
    val exact = exactCount(data).toDouble
    assert(scala.math.abs(exact - approxCount(bits, data)) / exact < 3.5 * aveErrorOf(bits))
  }
  def testLong(bits: Int) {
    val data = (0 to 10000).map { i => r.nextLong }
    val exact = exactCount(data).toDouble
    assert(scala.math.abs(exact - approxCount(bits, data)) / exact < 3.5 * aveErrorOf(bits))
  }
  def testLongIntersection(bits: Int, sets: Int) {
    val data: Seq[Iterable[Int]] = (0 until sets).map { idx =>
      (0 to 1000).map { i => r.nextInt(100) }
    }.toSeq
    val exact = exactIntersect(data)
    val errorMult = scala.math.pow(2.0, sets) - 1.0
    assert(scala.math.abs(exact - approxIntersect(bits, data)) / exact < errorMult *
      aveErrorOf(bits))
  }
  def testDownsize(dataSize: Int)(oldBits: Int, newBits: Int) {
    val data = (0 until dataSize).map { i => r.nextLong }
    val exact = exactCount(data).toDouble
    val hll = new HyperLogLogHasAdditionOperatorAndZero(oldBits)
    val oldHll = hll.sum(data.map { hll(_) })
    val newHll = oldHll.downsize(newBits)
    assert(scala.math.abs(exact - newHll.estimatedSize) / exact < 3.5 * aveErrorOf(newBits))
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
    "count intersections of 2" in { testLongIntersection(10, 2) }
    "count intersections of 3" in { testLongIntersection(10, 3) }
    "count intersections of 4" in { testLongIntersection(10, 4) }

    "throw error for differently sized HLL instances" in {
      val bigMon = new HyperLogLogHasAdditionOperatorAndZero(5)
      val smallMon = new HyperLogLogHasAdditionOperatorAndZero(4)
      val larger = bigMon(1) // uses implicit long2Bytes to make 8 byte array
      val smaller = smallMon(1) // uses implicit int2Bytes to make 4 byte array
      intercept[AssertionError] {
        (larger + smaller)
      }
    }
    "Correctly serialize" in {
      (4 to 20).foreach { bits =>
        val mon = new HyperLogLogHasAdditionOperatorAndZero(bits)
        // Zero
        verifySerialization(mon.zero)
        // One j
        verifySerialization(mon(12))
        // Two j's
        verifySerialization(mon(12) + mon(13))
        // Many j's
        val manyJ = HasAdditionOperatorAndZero.sum((1 to 1000 by 77).map(mon(_)))(mon)
        verifySerialization(manyJ)
        // Explicitly dense
        verifySerialization(manyJ.toDenseHLL)
      }
    }
    "be consistent for sparse vs. dense" in {
      val mon = new HyperLogLogHasAdditionOperatorAndZero(12)
      val data = (1 to 100).map { _ => r.nextLong }
      val partialSums = data.foldLeft(Seq(mon.zero)) { (seq, value) => seq :+ (seq.last + mon(value)) }
      // Now the ith entry of partialSums (0-based) is an HLL structure for i underlying elements
      partialSums.foreach { hll =>
        assert(hll.isInstanceOf[SparseHLL])
        assert(hll.size == hll.toDenseHLL.size)
        assert(hll.zeroCnt == hll.toDenseHLL.zeroCnt)
        assert(hll.z == hll.toDenseHLL.z)
        assert(hll.approximateSize == hll.toDenseHLL.approximateSize)
        assert(hll.estimatedSize == hll.toDenseHLL.estimatedSize)
      }
    }
    "properly convert to dense" in {
      val mon = new HyperLogLogHasAdditionOperatorAndZero(10)
      val data = (1 to 200).map { _ => r.nextLong }
      val partialSums = data.foldLeft(Seq(mon.zero)) { (seq, value) => seq :+ (seq.last + mon(value)) }
      partialSums.foreach { hll =>
        if (hll.size - hll.zeroCnt <= 64) {
          assert(hll.isInstanceOf[SparseHLL])
        } else {
          assert(hll.isInstanceOf[DenseHLL])
        }
      }
    }
    "properly do a batch create" in {
      val mon = new HyperLogLogHasAdditionOperatorAndZero(10)
      val data = (1 to 200).map { _ => r.nextLong }
      val partialSums = data.foldLeft(IndexedSeq(mon.zero)) { (seq, value) => seq :+ (seq.last + mon(value)) }
      (1 to 200).map { n =>
        assert(partialSums(n) == mon.batchCreate(data.slice(0, n)))
      }
    }

    "work as an Aggregator and return a HLL" in {
      List(5, 7, 10).foreach(bits => {
        val aggregator = HyperLogLogAggregator(bits)
        val data = (0 to 10000).map { i => r.nextInt(1000) }
        val exact = exactCount(data).toDouble

        val approxCount = aggregator(data.map(int2Bytes(_))).approximateSize.estimate.toDouble
        assert(scala.math.abs(exact - approxCount) / exact < 3.5 * aveErrorOf(bits))
      })
    }

    "work as an Aggregator and return size" in {
      List(5, 7, 10).foreach(bits => {
        val aggregator = HyperLogLogAggregator.sizeAggregator(bits)
        val data = (0 to 10000).map { i => r.nextInt(1000) }
        val exact = exactCount(data).toDouble

        val estimate = aggregator(data.map(int2Bytes(_)))
        assert(scala.math.abs(exact - estimate) / exact < 3.5 * aveErrorOf(bits))
      })
    }

    "correctly downsize sparse HLL" in {
      testDownsize(10)(10, 7)
      testDownsize(10)(14, 4)
      testDownsize(10)(12, 12)

      intercept[IllegalArgumentException] {
        testDownsize(10)(9, 13)
      }
      intercept[IllegalArgumentException] {
        testDownsize(10)(15, 3)
      }
    }

    "correctly downsize dense HLL" in {
      testDownsize(10000)(10, 7)
      testDownsize(10000)(14, 4)
      testDownsize(10000)(12, 12)

      intercept[IllegalArgumentException] {
        testDownsize(10000)(9, 13)
      }
      intercept[IllegalArgumentException] {
        testDownsize(10000)(15, 3)
      }
    }

    def verifySerialization(h: HLL) {
      assert(fromBytes(toBytes(h)) == h)
      fromByteBuffer(java.nio.ByteBuffer.wrap(toBytes(h))) shouldEqual h
    }
  }
}
