package com.twitter.algebird

import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.{Arbitrary, Gen, Prop}

import scala.collection.BitSet

import java.lang.AssertionError
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatest.wordspec.AnyWordSpec
import java.{util => ju}

object ReferenceHyperLogLog {

  /** Reference implementation of jRhoW to compare optimizations against */
  def bytesToBitSet(in: Array[Byte]): BitSet =
    BitSet(
      in.zipWithIndex
        .map(bi => (bi._1, bi._2 * 8))
        .flatMap(byteToIndicator(_)): _*
    )
  def byteToIndicator(bi: (Byte, Int)): Seq[Int] =
    (0 to 7).flatMap { i =>
      if ((bi._1 >> 7 - i & 1) == 1) {
        Vector(bi._2 + i)
      } else {
        Vector[Int]()
      }
    }

  def jRhoW(in: Array[Byte], bits: Int): (Int, Byte) = {
    val onBits = bytesToBitSet(in)
    val j = onBits.filter(_ < bits).map(1 << _).sum
    val rhow = onBits.find(_ >= bits).map(_ - bits + 1).getOrElse(0)
    (j, rhow.toByte)
  }

  def hash(input: Array[Byte]): Array[Byte] = {
    val seed = 12345678
    val (l0, l1) = MurmurHash128(seed)(input)
    val buf = new Array[Byte](16)
    java.nio.ByteBuffer
      .wrap(buf)
      .putLong(l0)
      .putLong(l1)
    buf
  }
}

class HyperLogLogLaws extends CheckProperties {
  import BaseProperties._
  import HyperLogLog._

  val bits: Int = 8
  implicit val hllMonoid: HyperLogLogMonoid = new HyperLogLogMonoid(bits)

  implicit val hllGen: Arbitrary[HLL] =
    Arbitrary(Gen.choose(0L, 1000000L).map(v => hllMonoid.create(long2Bytes(v))))

  property("HyperLogLog is a Monoid") {
    commutativeMonoidLaws[HLL]
  }

  property("bitsForError and error match") {
    Prop.forAll(Gen.choose(0.0001, 0.999)) { err =>
      val bits = HyperLogLog.bitsForError(err)
      HyperLogLog.error(bits) <= err && err < HyperLogLog.error(bits - 1)
    }
  }

  /**
   * We can't change the way Array[Byte] was hashed without breaking serialized HLLs
   */
  property("HyperLogLog.hash matches reference") {
    Prop.forAll { a: Array[Byte] => HyperLogLog.hash(a).toSeq == ReferenceHyperLogLog.hash(a).toSeq }
  }

  property("HyperLogLog.j and rhow match reference") {
    Prop.forAll { (bytes: Array[Byte]) =>
      val (j0, rhow0) = ReferenceHyperLogLog.jRhoW(bytes, bits)
      val j1 = HyperLogLog.j(bytes, bits)
      val rhow1 = HyperLogLog.rhoW(bytes, bits)
      j0 == j1 && rhow0 == rhow1
    }
  }
}

/* Ensure jRhoW matches referenceJRhoW */
class jRhoWMatchTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers {
  import HyperLogLog._

  /* Generate input arrays whose size is proportional to the bits (n) */
  val bitsGen: Gen[(Array[Byte], Int)] = for {
    bits <- Gen.choose(4, 31)
    in <- Gen.containerOfN[Array, Byte](4 * bits, Arbitrary.arbitrary[Byte])
  } yield (in, bits)

  property("jRhoW matches referenceJRhoW") {
    forAll(bitsGen) { case (in: Array[Byte], bits: Int) =>
      assert(jRhoW(in, bits) == ReferenceHyperLogLog.jRhoW(in, bits))
    }
  }
}

abstract class HyperLogLogProperty(bits: Int) extends ApproximateProperty {
  val monoid: HyperLogLogMonoid = new HyperLogLogMonoid(bits)

  def iterableToHLL[T: Hash128](it: Iterable[T]): HLL =
    monoid.sum(it.map(monoid.toHLL(_)))
}

class HLLCountProperty[T: Hash128: Gen](bits: Int) extends HyperLogLogProperty(bits) {
  type Exact = Iterable[T]
  type Approx = HLL

  type Input = Unit
  type Result = Long

  def makeApproximate(it: Iterable[T]): HLL = iterableToHLL(it)

  def exactGenerator: Gen[Vector[T]] = Gen.containerOf[Vector, T](implicitly[Gen[T]])

  def inputGenerator(it: Exact): Gen[Unit] = Gen.const(())
  def approximateResult(a: HLL, i: Unit): Approximate[Long] = a.approximateSize
  def exactResult(it: Iterable[T], i: Unit): Long = it.toSet.size
}

class HLLDownsizeCountProperty[T: Hash128: Gen](numItems: Int, oldBits: Int, newBits: Int)
    extends HLLCountProperty[T](oldBits) {

  override def exactGenerator: Gen[Vector[T]] =
    Gen.containerOfN[Vector, T](numItems, implicitly[Gen[T]])

  override def approximateResult(a: HLL, i: Unit): Approximate[Long] =
    a.downsize(newBits).approximateSize
}

class HLLIntersectionProperty[T: Hash128: Gen](bits: Int, numHlls: Int) extends HyperLogLogProperty(bits) {
  type Exact = Seq[Seq[T]]
  type Approx = Seq[HLL]

  type Input = Unit
  type Result = Long

  def makeApproximate(it: Seq[Seq[T]]): Approx = it.map(iterableToHLL(_))

  def exactGenerator: Gen[Seq[Seq[T]]] = {
    val vectorGenerator: Gen[Seq[T]] =
      Gen.containerOfN[Vector, T](1000, implicitly[Gen[T]])
    Gen.containerOfN[Vector, Seq[T]](numHlls, vectorGenerator)
  }

  def inputGenerator(it: Exact): Gen[Unit] = Gen.const(())

  def approximateResult(hlls: Seq[HLL], i: Unit): Approximate[Long] = monoid.intersectionSize(hlls)

  def exactResult(it: Seq[Seq[T]], i: Unit): Long =
    it.map(_.toSet).reduce(_.intersect(_)).size
}

/**
 * SetSizeAggregator should work as an aggregator and return approximate size when > maxSetSize
 */
abstract class SetSizeAggregatorProperty[T] extends ApproximateProperty {
  type Exact = Set[T]
  type Approx = Long

  type Input = Unit
  type Result = Double

  val maxSetSize: Int = 10000

  def inputGenerator(it: Exact): Gen[Unit] = Gen.const(())

  def exactResult(set: Set[T], i: Unit): Double = set.size
}

abstract class SmallSetSizeAggregatorProperty[T: Gen] extends SetSizeAggregatorProperty[T] {
  def exactGenerator: Gen[Set[T]] =
    for {
      size <- Gen.choose(maxSetSize + 1, maxSetSize * 2)
      set <- Gen.containerOfN[Set, T](size, implicitly[Gen[T]])
    } yield set

  def approximateResult(aggResult: Long, i: Unit): Approximate[Double] =
    Approximate.exact(aggResult.toDouble)
}

abstract class LargeSetSizeAggregatorProperty[T: Gen](bits: Int) extends SetSizeAggregatorProperty[T] {
  def exactGenerator: Gen[Set[T]] =
    for {
      size <- Gen.choose(1, maxSetSize)
      set <- Gen.containerOfN[Set, T](size, implicitly[Gen[T]])
    } yield set

  def approximateResult(aggResult: Long, i: Unit): Approximate[Double] = {
    val error = 1.04 / scala.math.sqrt(1 << bits)
    Approximate[Double](aggResult - error, aggResult, aggResult + error, 0.9972)
  }
}

class SmallBytesSetSizeAggregatorProperty[T <% Array[Byte]: Gen](bits: Int)
    extends SmallSetSizeAggregatorProperty[T] {
  def makeApproximate(s: Set[T]): Long =
    SetSizeAggregator[T](bits, maxSetSize).apply(s)
}

class LargeBytesSetSizeAggregatorProperty[T <% Array[Byte]: Gen](bits: Int)
    extends LargeSetSizeAggregatorProperty[T](bits) {
  def makeApproximate(s: Set[T]): Long =
    SetSizeAggregator[T](bits, maxSetSize).apply(s)
}

class SmallSetSizeHashAggregatorProperty[T: Hash128: Gen](bits: Int)
    extends SmallSetSizeAggregatorProperty[T] {
  def makeApproximate(s: Set[T]): Long =
    SetSizeHashAggregator[T](bits, maxSetSize).apply(s)
}

class LargeSetSizeHashAggregatorProperty[T: Hash128: Gen](bits: Int)
    extends LargeSetSizeAggregatorProperty[T](bits) {
  def makeApproximate(s: Set[T]): Long =
    SetSizeHashAggregator[T](bits, maxSetSize).apply(s)
}

class HLLProperties extends ApproximateProperties("HyperLogLog") {
  import ApproximateProperty.toProp

  implicit val intGen: Gen[Int] = Gen.chooseNum(Int.MinValue, Int.MaxValue)
  implicit val longGen: Gen[Long] = Gen.chooseNum(Long.MinValue, Long.MaxValue)

  for (bits <- List(5, 6, 7, 8, 10)) {
    property(s"Count ints with $bits bits") = toProp(new HLLCountProperty[Int](bits), 100, 1, 0.01)

    property(s"Count longs with $bits bits") = toProp(new HLLCountProperty[Long](bits), 100, 1, 0.01)
  }

  for (numHLLs <- List(1, 2, 3, 4)) {
    property(s"Intersect $numHLLs HLLs with 10 bits") =
      toProp(new HLLIntersectionProperty[Int](10, numHLLs), 100, 1, 0.01)
  }

  for {
    sparse <- List(true, false)
    (oldBits, newBits) <- List((10, 7), (14, 4), (12, 12))
  } {
    val sparseOrDense = if (sparse) "sparse" else "dense"
    val numElements = if (sparse) 10 else 10000
    property(s"Downsize $sparseOrDense HLLs from $oldBits bits to $newBits bits") =
      toProp(new HLLDownsizeCountProperty[Long](numElements, oldBits, newBits), 10, 10, 0.01)
  }

}

class SetSizeAggregatorProperties extends ApproximateProperties("SetSizeAggregator") {
  import ApproximateProperty.toProp
  import HyperLogLog.int2Bytes

  implicit val intGen: Gen[Int] = Gen.chooseNum(Int.MinValue, Int.MaxValue)
  implicit val longGen: Gen[Long] = Gen.chooseNum(Long.MinValue, Long.MaxValue)

  for (bits <- List(5, 7, 8, 10)) {
    property(
      s"work as an Aggregator and return exact size when <= maxSetSize for $bits bits, using conversion to Array[Byte]"
    ) = toProp(new SmallBytesSetSizeAggregatorProperty[Int](bits), 100, 1, 0.01)
    property(
      s"work as an Aggregator and return exact size when <= maxSetSize for $bits bits, using Hash128"
    ) = toProp(new SmallSetSizeHashAggregatorProperty[Int](bits), 100, 1, 0.01)
  }

  for (bits <- List(5, 7, 8, 10)) {
    property(
      s"work as an Aggregator and return approximate size when > maxSetSize for $bits bits, using conversion to Array[Byte]"
    ) = toProp(new LargeBytesSetSizeAggregatorProperty[Int](bits), 100, 1, 0.01)
    property(
      s"work as an Aggregator and return approximate size when > maxSetSize for $bits bits, using Hash128"
    ) = toProp(new LargeSetSizeHashAggregatorProperty[Int](bits), 100, 1, 0.01)
  }
}

class HyperLogLogTest extends AnyWordSpec with Matchers {

  import HyperLogLog._ // Get the implicit int2bytes, long2Bytes

  val r: ju.Random = new java.util.Random

  def exactCount[T](it: Iterable[T]): Int = it.toSet.size
  def approxCount[T <% Array[Byte]](bits: Int, it: Iterable[T]): Double = {
    val hll = new HyperLogLogMonoid(bits)
    hll.sizeOf(hll.sum(it.map(hll.create(_)))).estimate.toDouble
  }

  def aveErrorOf(bits: Int): Double = 1.04 / scala.math.sqrt(1 << bits)

  def testDownsize(dataSize: Int)(oldBits: Int, newBits: Int): Unit = {
    val data = (0 until dataSize).map(_ => r.nextLong)
    val exact = exactCount(data).toDouble
    val hll = new HyperLogLogMonoid(oldBits)
    val oldHll = hll.sum(data.map(hll.create(_)))
    val newHll = oldHll.downsize(newBits)
    assert(scala.math.abs(exact - newHll.estimatedSize) / exact < 3.5 * aveErrorOf(newBits))
  }

  "HyperLogLog" should {
    "throw error for differently sized HLL instances" in {
      val bigMon = new HyperLogLogMonoid(5)
      val smallMon = new HyperLogLogMonoid(4)
      val larger = bigMon.create(1) // uses implicit long2Bytes to make 8 byte array
      val smaller = smallMon.create(1) // uses implicit int2Bytes to make 4 byte array
      intercept[AssertionError] {
        (larger + smaller)
      }
    }
    "Correctly serialize" in {
      (4 to 20).foreach { bits =>
        val mon = new HyperLogLogMonoid(bits)
        // Zero
        verifySerialization(mon.zero)
        // One j
        verifySerialization(mon.create(12))
        // Two j's
        verifySerialization(mon.create(12) + mon.create(13))
        // Many j's
        val manyJ = Monoid.sum((1 to 1000 by 77).map(mon.create(_)))(mon)
        verifySerialization(manyJ)
        // Explicitly dense
        verifySerialization(manyJ.toDenseHLL)
      }
    }
    "be consistent for sparse vs. dense" in {
      val mon = new HyperLogLogMonoid(12)
      val data = (1 to 100).map(_ => r.nextLong)
      val partialSums = data.foldLeft(Seq(mon.zero))((seq, value) => seq :+ seq.last + mon.create(value))
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
      val mon = new HyperLogLogMonoid(10)
      val data = (1 to 200).map(_ => r.nextLong)
      val partialSums = data.foldLeft(Seq(mon.zero))((seq, value) => seq :+ seq.last + mon.create(value))
      partialSums.foreach { hll =>
        if (hll.size - hll.zeroCnt <= 64) {
          assert(hll.isInstanceOf[SparseHLL])
        } else {
          assert(hll.isInstanceOf[DenseHLL])
        }
      }
    }
    "properly do a batch create" in {
      val mon = new HyperLogLogMonoid(10)
      val data = (1 to 200).map(_ => r.nextLong)
      val partialSums = data.foldLeft(IndexedSeq(mon.zero)) { (seq, value) =>
        seq :+ seq.last + mon.create(value)
      }
      (1 to 200).map { n =>
        val bc = mon.sum(data.slice(0, n).map(mon.toHLL(_)))
        assert(partialSums(n) == bc)
      }
    }

    "work as an Aggregator and return a HLL" in {
      List(5, 7, 8, 10).foreach { bits =>
        val aggregator = HyperLogLogAggregator(bits)
        val data = (0 to 10000).map(_ => r.nextInt(1000))
        val exact = exactCount(data).toDouble

        val approxCount =
          aggregator(data.map(int2Bytes(_))).approximateSize.estimate.toDouble
        assert(scala.math.abs(exact - approxCount) / exact < 3.5 * aveErrorOf(bits))
      }
    }

    "work as an Aggregator and return size" in {
      List(5, 7, 8, 10).foreach { bits =>
        val aggregator = HyperLogLogAggregator.sizeAggregator(bits)
        val data = (0 to 10000).map(_ => r.nextInt(1000))
        val exact = exactCount(data).toDouble

        val estimate = aggregator(data.map(int2Bytes(_)))
        assert(scala.math.abs(exact - estimate) / exact < 3.5 * aveErrorOf(bits))
      }
    }

    "correctly downsize sparse HLL" in {
      intercept[IllegalArgumentException] {
        testDownsize(10)(9, 13)
      }
      intercept[IllegalArgumentException] {
        testDownsize(10)(15, 3)
      }
    }

    "correctly downsize dense HLL" in {
      intercept[IllegalArgumentException] {
        testDownsize(10000)(9, 13)
      }
      intercept[IllegalArgumentException] {
        testDownsize(10000)(15, 3)
      }
    }

    def verifySerialization(h: HLL): Unit = {
      assert(fromBytes(toBytes(h)) == h)
      fromByteBuffer(java.nio.ByteBuffer.wrap(toBytes(h))) shouldEqual h
    }
  }
}
