/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.algebird

import scala.collection.BitSet
import scala.util.control.Breaks._

import java.nio.ByteBuffer

/** A super lightweight (hopefully) version of BitSet */
case class BitSetLite(in: Array[Byte]) {
  def contains(x: Int): Boolean = {
    /* Assume in is big endian. So smallest byte is at idx 15 */
    val arrayIdx = x/8 /* Get the byte from the end */
    val remainder = x%8 /* Index into the byte */
    ((in(arrayIdx) >> (7 - remainder)) & 1) == 1
  }
}

/** Implementation of the HyperLogLog approximate counting as a Monoid
 * @link http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 *
 * HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm
 * Philippe Flajolet and Éric Fusy and Olivier Gandouet and Frédéric Meunier
 */
object HyperLogLog {

  /* Size of the hash in bits */
  val hashSize = 128

  def hash(input : Array[Byte]) : Array[Byte] = {
    val seed = 12345678
    val (l0, l1) = MurmurHash128(seed)(input)
    val buf = new Array[Byte](16)
    ByteBuffer
      .wrap(buf)
      .putLong(l0)
      .putLong(l1)
    buf
  }

  implicit def int2Bytes(i : Int) = {
    val buf = new Array[Byte](4)
    ByteBuffer
      .wrap(buf)
      .putInt(i)
    buf
  }

  implicit def long2Bytes(i : Long) = {
    val buf = new Array[Byte](8)
    ByteBuffer
      .wrap(buf)
      .putLong(i)
    buf
  }

  def twopow(i : Int) : Double = scala.math.pow(2.0, i)

  /** the value 'j' is equal to <w_0, w_1 ... w_(bits-1)> */
  def j(bsw: BitSetLite, bits: Int): Int = {
    var accum = 0
    var i = 0
    while (i < bits) {
      if (bsw.contains(i)) {
        accum += (1 << i)
      }
      i += 1
    }
    accum
  }

  /** The value 'w' is equal to <w_bits ... w_n>. The function rho counts the number of leading 
   *  zeroes in 'w'. We can calculate rho(w) at once with the method rhoW.
   */
  def rhoW(bsw: BitSetLite, bits: Int): Byte = {
    var consecutiveZeroes: Int = 1
    var i = bits
    while(i < hashSize) {
      if (bsw.contains(i)) {
        i = hashSize + 1 /* Hack to break out of loop */
      } else {
        consecutiveZeroes += 1
      }
      i += 1
    }
    consecutiveZeroes.toByte
  }

  /** We are computing j and \rho(w) from the paper,
   *  sorry for the name, but it allows someone to compare to the paper extremely low probability 
   *  rhow (position of the leftmost one bit) is > 127, so we use a Byte to store it
   *  Given a hash <w_0, w_1, w_2 ... w_n> the value 'j' is equal to <w_0, w_1 ... w_(bits-1)> and 
   *  the value 'w' is equal to <w_bits ... w_n>. The function rho counts the number of leading 
   *  zeroes in 'w'. We can calculate rho(w) at once with the method rhoW.
   */ 
  def jRhoW(in: Array[Byte], bits: Int): (Int, Byte) = {
    val onBits = BitSetLite(in)
    (j(onBits, bits), rhoW(onBits, bits))
  }

  /** Reference implementation of jRhoW */
  def bytesToBitSet(in : Array[Byte]) : BitSet = {
    BitSet(in.zipWithIndex.map { bi => (bi._1, bi._2 * 8) }
      .flatMap { byteToIndicator(_) } : _*)
  }
  def byteToIndicator(bi : (Byte,Int)) : Seq[Int] = {
    (0 to 7).flatMap { i =>
      if (((bi._1 >> (7 - i)) & 1) == 1) {
        Vector(bi._2 + i)
      }
      else {
        Vector[Int]()
      }
    }
  }

  def referenceJRhoW(in : Array[Byte], bits: Int) : (Int,Byte) = {
    val onBits = HyperLogLog.bytesToBitSet(in)
    (onBits.filter { _ < bits }.map { 1 << _ }.sum,
     (onBits.filter { _ >= bits }.min - bits + 1).toByte)
  }

  def toBytes(h : HLL) : Array[Byte] = {
    h match {
      case SparseHLL(bits,maxRhow) =>
        val jLen = (bits+7)/8
        assert(jLen >= 1)
        assert(jLen <= 3)
        val buf = new Array[Byte](1 + 1 + (jLen+1)*maxRhow.size)
        val byteBuf = ByteBuffer
          .wrap(buf)
          .put(3 : Byte)
          .put(bits.toByte)
        maxRhow.foldLeft(byteBuf) { (bb, jrhow) =>
          val (j,rhow) = jrhow
          bb.put((j & 0xff).toByte)
          if (jLen >= 2) bb.put(((j >> 8) & 0xff).toByte)
          if (jLen >= 3) bb.put(((j >> 16) & 0xff).toByte)
          bb.put(rhow.get)
        }
        buf

      case DenseHLL(bits,v) => ((2 : Byte) +: bits.toByte +: v).toArray
    }
  }

  // Make sure to be reversible so fromBytes(toBytes(x)) == x
  def fromBytes(bytes : Array[Byte]) : HLL = {
    val bb = ByteBuffer.wrap(bytes)
    bb.get.toInt match {
      case 2 => DenseHLL(bb.get, bytes.toIndexedSeq.tail.tail)
      case 3 => sparseFromByteBuffer(bb)
      case n => throw new Exception("Unrecognized HLL type: " + n)
    }
  }

  def fromByteBuffer(bb : ByteBuffer) : HLL = {
    bb.get.toInt match {
      case 2 =>
        val bits = bb.get
        val buf = new Array[Byte](bb.remaining)
        bb.get(buf)
        DenseHLL(bits, buf)
      case 3 => sparseFromByteBuffer(bb)
      case n => throw new Exception("Unrecognized HLL type: " + n)
    }
  }

  private def sparseFromByteBuffer(bb : ByteBuffer) : SparseHLL = {
    val bits = bb.get
    val jLen = (bits+7)/8
    assert(bb.remaining % (jLen+1) == 0, "Invalid byte array")
    val maxRhow = (1 to bb.remaining/(jLen+1)).map { _ =>
      val j = jLen match {
        case 1 => (bb.get.toInt & 0xff)
        case 2 => (bb.get.toInt & 0xff) + ((bb.get.toInt & 0xff) << 8)
        case 3 => (bb.get.toInt & 0xff) + ((bb.get.toInt & 0xff) << 8) + ((bb.get.toInt & 0xff) << 16)
      }
      val rhow = bb.get
      j -> Max(rhow)
    }.toMap
    SparseHLL(bits, maxRhow)
  }

  def alpha(bits: Int) = bits match {

    case 4 => 0.673
    case 5 => 0.697
    case 6 => 0.709
    case _ => 0.7213 / (1.0 + 1.079/(1<<bits).toDouble)

  }

  def error(bits: Int): Double = 1.04/scala.math.sqrt(twopow(bits))

  // Some constants from the algorithm:
  private[algebird] val fourBillionSome = twopow(32)
  private[algebird] val largeE = fourBillionSome/30.0
}

sealed abstract class HLL extends java.io.Serializable {
  import HyperLogLog._

  def bits : Int
  def size : Int
  def zeroCnt : Int
  def z : Double
  def +(other : HLL) : HLL
  def toDenseHLL : DenseHLL

  def approximateSize = asApprox(initialEstimate)

  def estimatedSize = approximateSize.estimate.toDouble

  private def initialEstimate = {

    val e = factor * z
    // There are large and small value corrections from the paper
    if (e > largeE) {
      -fourBillionSome * scala.math.log1p(-e/fourBillionSome)
    }
    else if (e <= smallE) {
      smallEstimate(e)
    }
    else {
      e
    }
  }

  private def asApprox(v: Double): Approximate[Long] = {
    val stdev = error(bits)
    val lowerBound = math.floor(math.max(v*(1.0 - 3*stdev), 0.0)).toLong
    val upperBound = math.ceil(v*(1.0 + 3*stdev)).toLong
    // Lower bound. see: http://en.wikipedia.org/wiki/68-95-99.7_rule
    val prob3StdDev = 0.9972
    Approximate(lowerBound, v.toLong, upperBound, prob3StdDev)
  }

  private def factor = alpha(bits) * size.toDouble * size.toDouble

  private def smallE = 5 * size / 2.0

  private def smallEstimate(e : Double) : Double = {
    if (zeroCnt == 0) {
      e
    }
    else {
      size * scala.math.log(size.toDouble / zeroCnt)
    }
  }
  /**
   * Set each item in the given buffer to the max of this and the buffer
   */
  def updateInto(buffer: Array[Byte]): Unit
}

case class SparseHLL(bits : Int, maxRhow : Map[Int,Max[Byte]]) extends HLL {

  assert(bits > 3, "Use at least 4 bits (2^(bits) = bytes consumed)")

  val size = 1 << bits

  lazy val zeroCnt = size - maxRhow.size

  lazy val z = 1.0 / (zeroCnt.toDouble + maxRhow.values.map { mj => HyperLogLog.twopow(-mj.get) }.sum)

  def +(other : HLL) = {

    other match {

      case sparse@SparseHLL(_, oMaxRhow) =>
        assert(sparse.size == size, "Incompatible HLL size: " + sparse.size + " != " + size)
        val allMaxRhow = Monoid.plus(maxRhow, oMaxRhow)
        if (allMaxRhow.size * 16 <= size) {
          SparseHLL(bits, allMaxRhow)
        } else {
          DenseHLL(bits, sparseMapToSequence(allMaxRhow))
        }

      case DenseHLL(bits, oldV) =>
        assert(oldV.size == size, "Incompatible HLL size: " + oldV.size + " != " + size)
        val newV = maxRhow.foldLeft(oldV) { case (v,(j,rhow)) =>
          if (rhow.get > v(j)) {
            v.updated(j, rhow.get)
          } else {
            v
          }
        }
        DenseHLL(bits, newV)

    }
  }

  def sparseMapToSequence(values : Map[Int,Max[Byte]]) : IndexedSeq[Byte] = {
    val array = Array.fill[Byte](size)(0:Byte)
    values.foreach { case (j,rhow) => array.update(j, rhow.get) }
    array.toIndexedSeq
  }

  lazy val toDenseHLL = DenseHLL(bits, sparseMapToSequence(maxRhow))
  def updateInto(buffer: Array[Byte]): Unit = {
    assert(buffer.length == size, "Length mismatch")
    maxRhow.foreach { case (idx, maxb) =>
      buffer.update(idx, buffer(idx) max (maxb.get))
    }
  }
}

/**
 * These are the individual instances which the Monoid knows how to add
 */
case class DenseHLL(bits : Int, v : IndexedSeq[Byte]) extends HLL {

  assert(v.size == (1<<bits), "Invalid size for dense vector: " + size + " != (1 << " + bits + ")")

  def size = v.size

  lazy val zeroCnt = v.count { _ == 0 }

  // Named from the parameter in the paper, probably never useful to anyone
  // except HyperLogLogMonoid
  lazy val z = 1.0 / (v.map { mj => HyperLogLog.twopow(-mj) }.sum)

  def +(other : HLL) : HLL = {

    other match {

      case SparseHLL(_,_) => (other + this)

      case DenseHLL(_,ov) =>
        assert(ov.size == v.size, "Incompatible HLL size: " + ov.size + " != " + v.size)
        DenseHLL(bits,
          v
          .view
          .zip(ov)
          .map { case (rhow,oRhow) => rhow max oRhow }
          .toIndexedSeq
        )

    }
  }

  val toDenseHLL = this
  def updateInto(buffer: Array[Byte]): Unit = {
    assert(buffer.length == size, "Length mismatch")
    var idx = 0
    v.foreach { maxb =>
      buffer.update(idx, (buffer(idx)) max maxb)
      idx += 1
    }
  }
}

/*
 * Error is about 1.04/sqrt(2^{bits}), so you want something like 12 bits for 1% error
 * which means each HLLInstance is about 2^{12} = 4kb per instance.
 */
class HyperLogLogMonoid(val bits : Int) extends Monoid[HLL] {
  import HyperLogLog._

  assert(bits > 3, "Use at least 4 bits (2^(bits) = bytes consumed)")

  val size = 1 << bits

  def apply[T <% Array[Byte]](t : T) = create(t)

  val zero : HLL = SparseHLL(bits, Monoid.zero[Map[Int,Max[Byte]]])

  def plus(left : HLL, right : HLL) = left + right

  override def sumOption(items: TraversableOnce[HLL]): Option[HLL] =
    if(items.isEmpty) None
    else {
      val buffer = new Array[Byte](size)
      items.foreach { _.updateInto(buffer) }
      Some(DenseHLL(bits, buffer.toIndexedSeq))
    }

  def create(example : Array[Byte]) : HLL = {
    val hashed = hash(example)
    val (j,rhow) = jRhoW(hashed, bits)
    SparseHLL(bits, Map(j -> Max(rhow)))
  }

  def batchCreate[T <% Array[Byte]](instances: Iterable[T]) : HLL = {
    val allMaxRhow = instances
      .map { x => jRhoW(hash(x), bits) }
      .groupBy { case (j, rhow) => j }
      .mapValues { _.maxBy { case (j, rhow) => rhow} }
      .mapValues { case (j, rhow) => Max(rhow) }
    if (allMaxRhow.size * 16 <= size) {
      SparseHLL(bits, allMaxRhow)
    } else {
      SparseHLL(bits, allMaxRhow).toDenseHLL
    }
  }

  final def estimateSize(hll : HLL) : Double = {
    hll.estimatedSize
  }

  final def sizeOf(hll: HLL): Approximate[Long] = {
    hll.approximateSize
  }

  final def estimateIntersectionSize(his : Seq[HLL]) : Double = {
    intersectionSize(his).estimate.toDouble
  }

  final def intersectionSize(his: Seq[HLL]): Approximate[Long] = {
    his.headOption.map { head =>
      val tail = his.tail
      /*
       * |A n B| = |A| + |B| - |A u B|
       * in the below, we set A = head, and B = tail.
       * then note that A u (B0 n B1 n ...) = (B0 u A) n (B1 u A) n ...
       * the latter we can compute with tail.map { _ + A } using the HLLInstance +
       * since + on HLLInstance creates the instance for the union.
       */
      sizeOf(head) + intersectionSize(tail) -
        intersectionSize(tail.map { _ + head })
    }
    .map { _.withMin(0L) } //We always know the insection is >= 0
    .getOrElse(Approximate.exact(0L)) //Empty lists have no intersection
  }
}

object HyperLogLogAggregator {
  def apply(bits: Int): HyperLogLogAggregator = {
    val monoid = new HyperLogLogMonoid(bits)
    new HyperLogLogAggregator(monoid)
  }

  def sizeAggregator(bits: Int): Aggregator[Array[Byte], HLL, Double] = {
    apply(bits).andThenPresent(_.estimatedSize)
  }
}

case class HyperLogLogAggregator(val hllMonoid: HyperLogLogMonoid) extends MonoidAggregator[Array[Byte], HLL, HLL] {
  val monoid = hllMonoid

  def prepare(value: Array[Byte]) = monoid.create(value)
  def present(hll: HLL) = hll
}
