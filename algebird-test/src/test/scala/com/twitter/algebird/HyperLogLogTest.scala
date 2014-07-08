package com.twitter.algebird

import org.specs2.mutable._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.{ arbitrary, arbByte }
import org.scalacheck.{ Prop, Properties }
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen
import org.scalacheck.Gen.{ choose, containerOfN }

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

object HyperLogLogLaws extends Properties("HyperLogLog") {
  import BaseProperties._
  import HyperLogLog._

  implicit val hllMonoid = new HyperLogLogMonoid(5) //5 bits

  implicit val hllGen = Arbitrary {
    for (
      v <- choose(0, 10000)
    ) yield (hllMonoid(v))
  }

  property("HyperLogLog is a Monoid") = monoidLawsEq[HLL]{ _.toDenseHLL == _.toDenseHLL }
}

/* Ensure jRhoW matches referenceJRhoW */
object jRhoWMatchTest extends Properties("jRhoWMatch") {
  import HyperLogLog._

  implicit val hashGen = Arbitrary { containerOfN[Array, Byte](16, arbitrary[Byte]) }
  /* For some reason choose in this version of scalacheck
  is bugged so I need the suchThat clause */
  implicit val bitsGen = Arbitrary { choose(4, 31) suchThat (x => x >= 4 && x <= 31) }

  property("jRhoW matches referenceJRhoW") = forAll { (in: Array[Byte], bits: Int) =>
    jRhoW(in, bits) == ReferenceHyperLogLog.jRhoW(in, bits)
  }
}

class HyperLogLogTest extends Specification {

  import HyperLogLog._ //Get the implicit int2bytes, long2Bytes

  val r = new java.util.Random

  def exactCount[T](it: Iterable[T]): Int = it.toSet.size
  def approxCount[T <% Array[Byte]](bits: Int, it: Iterable[T]) = {
    val hll = new HyperLogLogMonoid(bits)
    hll.sizeOf(hll.sum(it.map { hll(_) })).estimate.toDouble
  }

  // This makes a list of sets that overlap in exactly overlap items
  def makeNwithOverlapK(sets: Int, overlap: Int, sizeEach: Int): List[Set[Int]] = {
    val overlapSet = (0 until overlap).toSet
    (overlap until (sets * sizeEach))
      .iterator
      .grouped(sizeEach - overlap)
      .take(sets)
      .map(s => s.toSet ++ overlapSet)
      .toList
   }

  def aveErrorOf(bits: Int): Double = 1.04 / scala.math.sqrt(1 << bits)

  def exactIntersect[T](it: Seq[Iterable[T]]): Int = {
    it.foldLeft(Set[T]()) { (old, newS) => old ++ (newS.toSet) }.size
  }
  def toHLL[T <% Array[Byte]](bits: Int): (Iterable[T] => HLL) = {
    val hll = new HyperLogLogMonoid(bits)

    { iter: Iterable[T] =>
      //Map each iterable to a HLL instance:
      hll.sum(iter.iterator.map { hll(_) })
    };
  }
  def approxIntersect[T <% Array[Byte]](bits: Int, it: Seq[Iterable[T]]): Double = {
    //Map each iterable to a HLL instance:
    val seqHlls = it.map(toHLL(bits))
    val hll = new HyperLogLogMonoid(bits)
    hll.intersectionSize(seqHlls).estimate.toDouble
  }

  def test(bits: Int) {
    val data = (0 to 10000).map { i => r.nextInt(1000) }
    val exact = exactCount(data).toDouble
    scala.math.abs(exact - approxCount(bits, data)) / exact must be_<(3.5 * aveErrorOf(bits))
  }
  def testLong(bits: Int) {
    val data = (0 to 10000).map { i => r.nextLong }
    val exact = exactCount(data).toDouble
    scala.math.abs(exact - approxCount(bits, data)) / exact must be_<(3.5 * aveErrorOf(bits))
  }
  def testLongIntersection(bits: Int, sets: Int) {
    // make sets of size 100, with between 1 and 10 overlap
    val intersectSize = r.nextInt(10) + 1
    val data: Seq[Iterable[Int]] = makeNwithOverlapK(sets, intersectSize, 100)
    val seqHlls = data.map(toHLL(bits))

    val hll = new HyperLogLogMonoid(bits)
    hll.intersectionSize(seqHlls).contains(intersectSize)
  }
  /*
   * Totally unprincipled test, and rather large error for small overlaps
   */
  def testLongIntersection2(bits: Int, sets: Int) {
    // make sets of size 100, with between 1 and 10 overlap
    val intersectSize = r.nextInt(99) + 1
    val data: Seq[Iterable[Int]] = makeNwithOverlapK(sets, intersectSize, 100)
    // This s
    val errorMult = 100.0
    scala.math.abs(intersectSize - approxIntersect2(bits, data)) / intersectSize must be_<(errorMult *
      aveErrorOf(bits))
  }
  // This is an unproven tecphnique
  def intersect(h1: HLL, h2: HLL): HLL = {
    assert(h1.bits == h2.bits, "bits mismatch: " + h1.toString + h2.toString)
    val newBuf = new Array[Byte](1 << h1.bits)
    h1.toDenseHLL.v.zip(h2.toDenseHLL.v).zipWithIndex.foreach { case ((l, r), idx) =>
      newBuf(idx) = l min r
    }
    DenseHLL(h1.bits, new IndexedSeqArrayByte(newBuf))
  }
  def approxIntersect2[T <% Array[Byte]](bits: Int, it: Seq[Iterable[T]]): Double = {
    val seqHlls = it.map(toHLL(bits))
    seqHlls.reduce(intersect).estimatedSize
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
    "count min-intersections of 2" in { testLongIntersection2(10, 2) }
    "count min-intersections of 3" in { testLongIntersection2(10, 3) }
    "count min-intersections of 4" in { testLongIntersection2(10, 4) }
    "count min-intersections of 5" in { testLongIntersection2(10, 5) }

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
      val partialSums = data.foldLeft(Seq(mon.zero)) { (seq, value) => seq :+ (seq.last + mon(value)) }
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
      val partialSums = data.foldLeft(Seq(mon.zero)) { (seq, value) => seq :+ (seq.last + mon(value)) }
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
      val partialSums = data.foldLeft(IndexedSeq(mon.zero)) { (seq, value) => seq :+ (seq.last + mon(value)) }
      (1 to 200).map { n =>
        partialSums(n) must be_==(mon.batchCreate(data.slice(0, n)))
      }
    }

    "work as an Aggregator and return a HLL" in {
      List(5, 7, 10).foreach(bits => {
        val aggregator = HyperLogLogAggregator(bits)
        val data = (0 to 10000).map { i => r.nextInt(1000) }
        val exact = exactCount(data).toDouble

        val approxCount = aggregator(data.map(int2Bytes(_))).approximateSize.estimate.toDouble
        scala.math.abs(exact - approxCount) / exact must be_<(3.5 * aveErrorOf(bits))
      })
    }

    "work as an Aggregator and return size" in {
      List(5, 7, 10).foreach(bits => {
        val aggregator = HyperLogLogAggregator.sizeAggregator(bits)
        val data = (0 to 10000).map { i => r.nextInt(1000) }
        val exact = exactCount(data).toDouble

        val estimate = aggregator(data.map(int2Bytes(_)))
        scala.math.abs(exact - estimate) / exact must be_<(3.5 * aveErrorOf(bits))
      })
    }

    def verifySerialization(h: HLL) {
      fromBytes(toBytes(h)) must be_==(h)
      fromByteBuffer(java.nio.ByteBuffer.wrap(toBytes(h))) must be_==(h)
    }
  }
}
