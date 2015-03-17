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

import java.nio.ByteBuffer

case class ReadOnlyBoxedArrayByte(private val inArray: Array[Byte]) {
  def apply(idx: Int) = inArray.apply(idx)
  def size = inArray.size
  private[algebird] def getArray = inArray
  // Need to do underlying array equality sensibly
  override def equals(o: Any) = {
    if (o.isInstanceOf[ReadOnlyBoxedArrayByte]) {
      val oAsTpe = o.asInstanceOf[ReadOnlyBoxedArrayByte]
      java.util.Arrays.equals(inArray, oAsTpe.inArray)
    } else false
  }
}

/** A super lightweight (hopefully) version of BitSet */
case class BitSetLite(in: Array[Byte]) {
  def contains(x: Int): Boolean = {
    /**
     * Pretend 'in' is little endian so that the bitstring b0b1b2b3 is such that if b0 == 1, then
     *  0 is in the bitset, if b1 == 1, then 1 is in the bitset.
     */
    val arrayIdx = x / 8
    val remainder = x % 8
    ((in(arrayIdx) >> (7 - remainder)) & 1) == 1
  }
}

/**
 * Implementation of the HyperLogLog approximate counting as a Monoid
 * @link http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 *
 * HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm
 * Philippe Flajolet and Éric Fusy and Olivier Gandouet and Frédéric Meunier
 */
object HyperLogLog {

  /* Size of the hash in bits */
  val hashSize = 128

  def hash(input: Array[Byte]): Array[Byte] = {
    val seed = 12345678
    val (l0, l1) = MurmurHash128(seed)(input)
    val buf = new Array[Byte](16)
    ByteBuffer
      .wrap(buf)
      .putLong(l0)
      .putLong(l1)
    buf
  }

  implicit def int2Bytes(i: Int) = {
    val buf = new Array[Byte](4)
    ByteBuffer
      .wrap(buf)
      .putInt(i)
    buf
  }

  implicit def long2Bytes(i: Long) = {
    val buf = new Array[Byte](8)
    ByteBuffer
      .wrap(buf)
      .putLong(i)
    buf
  }

  @inline
  def twopow(i: Int): Double = java.lang.Math.pow(2.0, i)

  /**
   * the value 'j' is equal to <w_0, w_1 ... w_(bits-1)>
   *  TODO: We could read in a byte at a time.
   */
  def j(bsl: BitSetLite, bits: Int): Int = {
    @annotation.tailrec
    def loop(pos: Int, accum: Int): Int = {
      if (pos >= bits) {
        accum
      } else if (bsl.contains(pos)) {
        loop(pos + 1, accum + (1 << pos))
      } else {
        loop(pos + 1, accum)
      }
    }
    loop(0, 0)
  }

  /**
   * The value 'w' is equal to <w_bits ... w_n>. The function rho counts the number of leading
   *  zeroes in 'w'. We can calculate rho(w) at once with the method rhoW.
   */
  def rhoW(bsl: BitSetLite, bits: Int): Byte = {
    @annotation.tailrec
    def loop(pos: Int, zeros: Int): Int =
      if (bsl.contains(pos)) zeros
      else loop(pos + 1, zeros + 1)
    loop(bits, 1).toByte
  }

  /**
   * We are computing j and \rho(w) from the paper,
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

  def toBytes(h: HLL): Array[Byte] = {
    h match {
      case SparseHLL(bits, maxRhow) =>
        val jLen = (bits + 7) / 8
        assert(jLen >= 1)
        assert(jLen <= 3)
        val buf = new Array[Byte](1 + 1 + (jLen + 1) * maxRhow.size)
        val byteBuf = ByteBuffer
          .wrap(buf)
          .put(3: Byte)
          .put(bits.toByte)
        maxRhow.foldLeft(byteBuf) { (bb, jrhow) =>
          val (j, rhow) = jrhow
          bb.put((j & 0xff).toByte)
          if (jLen >= 2) bb.put(((j >> 8) & 0xff).toByte)
          if (jLen >= 3) bb.put(((j >> 16) & 0xff).toByte)
          bb.put(rhow.get)
        }
        buf

      case DenseHLL(bits, v) =>
        val bb = ByteBuffer.allocate(v.size + 2)
        bb.put(2: Byte)
        bb.put(bits.toByte)
        bb.put(v.getArray)
        bb.array
    }
  }

  // Make sure to be reversible so fromBytes(toBytes(x)) == x
  def fromBytes(bytes: Array[Byte]): HLL =
    fromByteBuffer(ByteBuffer.wrap(bytes))

  def fromByteBuffer(bb: ByteBuffer): HLL = {
    bb.get.toInt match {
      case 2 =>
        val bits = bb.get
        val buf = new Array[Byte](bb.remaining)
        bb.get(buf)
        DenseHLL(bits, ReadOnlyBoxedArrayByte(buf))
      case 3 => sparseFromByteBuffer(bb)
      case n => throw new Exception("Unrecognized HLL type: " + n)
    }
  }

  private def sparseFromByteBuffer(bb: ByteBuffer): SparseHLL = {
    val bits = bb.get
    val jLen = (bits + 7) / 8
    assert(bb.remaining % (jLen + 1) == 0, "Invalid byte array")
    val maxRhow = (1 to bb.remaining / (jLen + 1)).map { _ =>
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
    case _ => 0.7213 / (1.0 + 1.079 / (1 << bits).toDouble)

  }

  def error(bits: Int): Double = 1.04 / scala.math.sqrt(twopow(bits))
}

sealed abstract class HLL extends java.io.Serializable {
  import HyperLogLog._

  def bits: Int
  def size: Int
  def zeroCnt: Int
  def z: Double
  def +(other: HLL): HLL
  def toDenseHLL: DenseHLL

  def approximateSize = asApprox(initialEstimate)

  def estimatedSize: Double = initialEstimate.toDouble

  lazy val initialEstimate: Double = {

    val sizeDouble: Double = size.toDouble
    val smallE = 5 * sizeDouble / 2.0
    val factor = alpha(bits) * sizeDouble * sizeDouble

    val e: Double = factor * z
    // There are large and small value corrections from the paper
    // We stopped using the large value corrections since when using Long's
    // there was pathalogically bad results. See https://github.com/twitter/algebird/issues/284
    if (e <= smallE) {
      smallEstimate(e)
    } else {
      e
    }
  }

  private def asApprox(v: Double): Approximate[Long] = {
    val stdev = error(bits)
    val lowerBound = math.floor(math.max(v * (1.0 - 3 * stdev), 0.0)).toLong
    val upperBound = math.ceil(v * (1.0 + 3 * stdev)).toLong
    // Lower bound. see: http://en.wikipedia.org/wiki/68-95-99.7_rule
    val prob3StdDev = 0.9972
    Approximate(lowerBound, v.toLong, upperBound, prob3StdDev)
  }

  private def smallEstimate(e: Double): Double = {
    if (zeroCnt == 0) {
      e
    } else {
      size * scala.math.log(size.toDouble / zeroCnt)
    }
  }
  /**
   * Set each item in the given buffer to the max of this and the buffer
   */
  def updateInto(buffer: Array[Byte]): Unit

  /**
   * Returns the modified value of rhoW at j, taking into account the
   * extra run of bits added to rho due to reduction in the length of j.
   *
   * @param currentJ    j for which modified rhoW is needed
   * @param currentRhoW Current rhoW value for j
   * @param reducedBits New length of j
   * @param reducedSize New size (passed in to avoid repeated computation)
   * @param bitMask     Mask to force early termination of HyperLogLog.rhoW (passed in to avoid repeated computation)
   * @param buf         Byte array (passed in to avoid repeated allocation)
   *
   * @return  New value of rhoW
   */
  protected def getModifiedRhoW(currentJ: Int, currentRhoW: Byte,
    reducedBits: Int, reducedSize: Int,
    bitMask: Int, buf: Array[Byte]): Byte = {
    if (currentRhoW == 0)
      // rhoW not set, skip
      currentRhoW
    else if (currentJ < reducedSize)
      // the MSBs are guaranteed to be all zero, so we can just add the difference to rhoW
      (currentRhoW + (bits - reducedBits)).toByte
    else {
      // find the number of leading zeros
      // we use bitMask to force rhoW to stop after going through (bits - reducedBits) bits
      ByteBuffer.wrap(buf).putInt(Integer.reverse(currentJ | bitMask))
      HyperLogLog.rhoW(BitSetLite(buf), reducedBits)
    }
  }

  /**
   * Returns a new HLL instance with reduced size
   *
   * [[http://research.neustar.biz/2012/09/12/set-operations-on-hlls-of-different-sizes/]]
   *
   * [[http://research.neustar.biz/2013/03/25/hyperloglog-engineering-choosing-the-right-bits/]]
   *
   * @param reducedBits The new number of bits to use
   *
   * @return  New HLL instance with reduced size
   */
  def downsize(reducedBits: Int): HLL = {
    require(reducedBits > 3 && reducedBits <= bits, s"Use at least 4, and at most $bits bits")
    if (reducedBits == bits)
      this
    else {
      val reducedSize = 1 << reducedBits
      // bit mask to set MSBs to 1. Makes rhoW exit fast
      val bitMask = 0xFFFFFFFF << bits
      val buf = new Array[Byte](4)

      downsize(reducedBits, reducedSize, bitMask, buf)
    }
  }

  /**
   * Returns a new HLL instance with reduced size
   *
   * @param reducedBits The new number of bits to use (for the length of j)
   * @param reducedSize New size (passed in to avoid repeated computation)
   * @param bitMask     Mask to force early termination of HyperLogLog.rhoW (passed in to avoid repeated computation)
   * @param buf         Byte array (passed in to avoid repeated allocation)
   *
   * @return  New HLL instance with reduced size
   */
  protected def downsize(reducedBits: Int, reducedSize: Int, bitMask: Int, buf: Array[Byte]): HLL
}

case class SparseHLL(bits: Int, maxRhow: Map[Int, Max[Byte]]) extends HLL {

  assert(bits > 3, "Use at least 4 bits (2^(bits) = bytes consumed)")

  val size = 1 << bits

  lazy val zeroCnt = size - maxRhow.size

  lazy val z = 1.0 / (zeroCnt.toDouble + maxRhow.values.map { mj => HyperLogLog.twopow(-mj.get) }.sum)

  def +(other: HLL) = {

    other match {

      case sparse @ SparseHLL(_, oMaxRhow) =>
        assert(sparse.size == size, "Incompatible HLL size: " + sparse.size + " != " + size)
        val allMaxRhow = Monoid.plus(maxRhow, oMaxRhow)
        if (allMaxRhow.size * 16 <= size) {
          SparseHLL(bits, allMaxRhow)
        } else {
          DenseHLL(bits, sparseMapToArray(allMaxRhow))
        }

      case DenseHLL(bits, oldV) =>
        assert(oldV.size == size, "Incompatible HLL size: " + oldV.size + " != " + size)
        val newContents: Array[Byte] = oldV.getArray.clone
        val siz = newContents.size

        maxRhow.foreach {
          case (idx, maxB) =>
            val existing: Byte = newContents(idx)
            val other: Byte = maxRhow(idx).get

            if (other > existing)
              newContents.update(idx, other)
        }

        DenseHLL(bits, ReadOnlyBoxedArrayByte(newContents))
    }
  }

  def sparseMapToArray(values: Map[Int, Max[Byte]]): ReadOnlyBoxedArrayByte = {
    val array = Array.fill[Byte](size)(0: Byte)
    values.foreach { case (j, rhow) => array.update(j, rhow.get) }
    ReadOnlyBoxedArrayByte(array)
  }

  lazy val toDenseHLL = DenseHLL(bits, sparseMapToArray(maxRhow))

  def updateInto(buffer: Array[Byte]): Unit = {
    assert(buffer.length == size, "Length mismatch")
    maxRhow.foreach {
      case (idx, maxb) =>
        buffer.update(idx, buffer(idx) max (maxb.get))
    }
  }

  override protected def downsize(reducedBits: Int, reducedSize: Int, bitMask: Int, buf: Array[Byte]): HLL = {
    val reducedMaxRhoW = collection.mutable.Map.empty[Int, Byte]
    maxRhow.foreach {
      case (j, rhoW) =>
        val modifiedRhoW = getModifiedRhoW(j, rhoW.get, reducedBits, reducedSize, bitMask, buf)
        val newJ = j % reducedSize
        val newRhoW = reducedMaxRhoW.getOrElse(newJ, 0: Byte)
        reducedMaxRhoW += (newJ -> (newRhoW max modifiedRhoW))
    }
    SparseHLL(reducedBits, reducedMaxRhoW.toMap.mapValues(Max(_)))
  }
}

/**
 * These are the individual instances which the Monoid knows how to add
 */
case class DenseHLL(bits: Int, v: ReadOnlyBoxedArrayByte) extends HLL {

  assert(v.size == (1 << bits), "Invalid size for dense vector: " + size + " != (1 << " + bits + ")")

  def size = v.size

  // Named from the parameter in the paper, probably never useful to anyone
  // except HyperLogLogMonoid

  val (zeroCnt, z) = {
    var count: Int = 0
    var res: Double = 0
    v.getArray.foreach { mj: Byte =>
      if (mj == 0) {
        count += 1
        res += 1.0
      } else {
        res += java.lang.Math.pow(2.0, -mj)
      }
    }
    (count, 1.0 / res)
  }

  def +(other: HLL): HLL = {

    other match {

      case SparseHLL(_, _) => (other + this)

      case DenseHLL(_, ov) =>
        assert(ov.size == v.size, "Incompatible HLL size: " + ov.size + " != " + v.size)

        val siz: Int = ov.size
        val newContents: Array[Byte] = new Array[Byte](siz)

        val other: Array[Byte] = ov.getArray
        val thisArray: Array[Byte] = v.getArray

        var indx: Int = 0
        while (indx < siz) {
          val rhow = thisArray(indx)
          val oRhow = other(indx)
          newContents.update(indx, rhow max oRhow)
          indx += 1
        }

        DenseHLL(bits, ReadOnlyBoxedArrayByte(newContents))
    }
  }

  val toDenseHLL = this
  def updateInto(buffer: Array[Byte]): Unit = {
    assert(buffer.length == size, "Length mismatch")
    var idx = 0
    v.getArray.foreach { maxb =>
      buffer.update(idx, (buffer(idx)) max maxb)
      idx += 1
    }
  }

  override def downsize(reducedBits: Int, reducedSize: Int, bitMask: Int, buf: Array[Byte]): HLL = {
    val reducedV = Array.fill[Byte](reducedSize)(0: Byte)
    for (i <- 0 until size) {
      val modifiedRhoW = getModifiedRhoW(i, v(i), reducedBits, reducedSize, bitMask, buf)
      val newJ = i % reducedSize
      val newRhoW = reducedV(newJ)
      reducedV.update(newJ, modifiedRhoW max newRhoW)
    }
    DenseHLL(reducedBits, ReadOnlyBoxedArrayByte(reducedV))
  }
}

/*
 * Error is about 1.04/sqrt(2^{bits}), so you want something like 12 bits for 1% error
 * which means each HLLInstance is about 2^{12} = 4kb per instance.
 */
class HyperLogLogMonoid(val bits: Int) extends Monoid[HLL] {
  import HyperLogLog._

  assert(bits > 3, "Use at least 4 bits (2^(bits) = bytes consumed)")

  val size = 1 << bits

  def apply[T <% Array[Byte]](t: T) = create(t)

  val zero: HLL = SparseHLL(bits, Monoid.zero[Map[Int, Max[Byte]]])

  def plus(left: HLL, right: HLL) = left + right

  private[this] final def denseUpdate(existing: HLL, iter: Iterator[HLL]): HLL = {
    val buffer = new Array[Byte](size)
    existing.updateInto(buffer)
    iter.foreach { _.updateInto(buffer) }

    DenseHLL(bits, ReadOnlyBoxedArrayByte(buffer))
  }

  override def sumOption(items: TraversableOnce[HLL]): Option[HLL] =
    if (items.isEmpty) {
      None
    } else {
      val iter = items.toIterator.buffered
      var curValue = iter.next
      while (iter.hasNext) {
        curValue = (curValue, iter.head) match {
          case (DenseHLL(_, _), _) => denseUpdate(curValue, iter)
          case (_, DenseHLL(_, _)) => denseUpdate(curValue, iter)
          case _ => curValue + iter.next
        }
      }
      Some(curValue)
    }

  def create(example: Array[Byte]): HLL = {
    val hashed = hash(example)
    val (j, rhow) = jRhoW(hashed, bits)
    SparseHLL(bits, Map(j -> Max(rhow)))
  }

  def batchCreate[T <% Array[Byte]](instances: Iterable[T]): HLL = {
    val allMaxRhow = instances
      .map { x => jRhoW(hash(x), bits) }
      .groupBy { case (j, rhow) => j }
      .map { case (j, iter) => (j, Max(iter.maxBy(_._2)._2)) }
    if (allMaxRhow.size * 16 <= size) {
      SparseHLL(bits, allMaxRhow)
    } else {
      SparseHLL(bits, allMaxRhow).toDenseHLL
    }
  }

  final def estimateSize(hll: HLL): Double = {
    hll.estimatedSize
  }

  final def sizeOf(hll: HLL): Approximate[Long] = {
    hll.approximateSize
  }

  final def estimateIntersectionSize(his: Seq[HLL]): Double = {
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
      .map { _.withMin(0L) } // We always know the intersection is >= 0
      .getOrElse(Approximate.exact(0L)) // Empty lists have no intersection
  }
}

object HyperLogLogAggregator {
  def apply(bits: Int): HyperLogLogAggregator = {
    val monoid = new HyperLogLogMonoid(bits)
    new HyperLogLogAggregator(monoid)
  }

  def sizeAggregator(bits: Int): MonoidAggregator[Array[Byte], HLL, Double] =
    apply(bits).andThenPresent(_.estimatedSize)
}

case class HyperLogLogAggregator(val hllMonoid: HyperLogLogMonoid) extends MonoidAggregator[Array[Byte], HLL, HLL] {
  val monoid = hllMonoid

  def prepare(value: Array[Byte]) = monoid.create(value)
  def present(hll: HLL) = hll
}
