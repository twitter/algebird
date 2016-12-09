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

import scala.collection.immutable.BitSet
import scala.collection.JavaConverters._

import com.googlecode.javaewah.{ EWAHCompressedBitmap => CBitSet }

object RichCBitSet {
  def apply(x: Int*) = {
    CBitSet.bitmapOf(x.sorted: _*)
  }
  implicit def cb2rcb(cb: CBitSet): RichCBitSet = new RichCBitSet(cb)
}

// An enrichment to give some scala-like operators to the compressed
// bit set.
class RichCBitSet(val cb: CBitSet) {
  def ++(b: CBitSet): CBitSet = cb.or(b)

  def ==(b: CBitSet): Boolean = cb.equals(b)

  def toBitSet(width: Int): BitSet = {
    val a = new Array[Long]((width + 63) / 64)
    cb.asScala.foreach{ i: java.lang.Integer => a(i.intValue / 64) |= 1L << (i.intValue % 64) }
    BitSet.fromBitMask(a)
  }
}

object BloomFilter {

  def apply[A](numEntries: Int, fpProb: Double)(implicit toBytes: A => Array[Byte]) = {
    val width = BloomFilter.optimalWidth(numEntries, fpProb)
    val numHashes = BloomFilter.optimalNumHashes(numEntries, width)
    BloomFilterMonoid[A](numHashes, width)(toBytes)
  }

  // Compute optimal number of hashes: k = m/n ln(2)
  def optimalNumHashes(numEntries: Int, width: Int): Int = math.ceil(width / numEntries * math.log(2)).toInt

  // Compute optimal width: m = - n ln(p) / (ln(2))^2
  def optimalWidth(numEntries: Int, fpProb: Double): Int = {
    val widthEstimate = math.ceil(-1 * numEntries * math.log(fpProb) / math.log(2) / math.log(2)).toInt

    if (widthEstimate == Int.MaxValue) {
      throw new java.lang.IllegalArgumentException(
        "BloomFilter cannot guarantee the specified false positive probability for the number of entries!")
    }

    return widthEstimate
  }
}

/**
 * Bloom Filter - a probabilistic data structure to test presence of an element.
 *
 * Operations
 *   1) insert: hash the value k times, updating the bitfield at the index equal to each hashed value
 *   2) query: hash the value k times.  If there are k collisions, then return true; otherwise false.
 *
 * http://en.wikipedia.org/wiki/Bloom_filter
 *
 */
case class BloomFilterMonoid[A](numHashes: Int, width: Int)(implicit toBytes: A => Array[Byte]) extends Monoid[BF[A]] {
  val hashes: BFHash[A] = BFHash[A](numHashes, width)(toBytes)

  val zero: BF[A] = BFZero[A](hashes, width)

  /**
   * Assume the bloom filters are compatible (same width and same hashing functions).  This
   * is the union of the 2 bloom filters.
   */
  def plus(left: BF[A], right: BF[A]): BF[A] = left ++ right

  /**
   * Create a bloom filter with one item.
   */
  def create(item: A): BF[A] = BFItem(item, hashes, width)

  /**
   * Create a bloom filter with multiple items.
   */
  def create(data: A*): BF[A] = {
    data.foldLeft(zero) { case (acc, x) => plus(acc, create(x)) }
  }
}

/**
 * Bloom Filter data structure
 */
sealed abstract class BF[A] extends java.io.Serializable {
  val numHashes: Int

  val width: Int

  def ++(other: BF[A]): BF[A]

  def +(other: A): BF[A]

  def checkAndAdd(item: A): (BF[A], ApproximateBoolean)

  def contains(item: A): ApproximateBoolean

  // Estimates the cardinality of the set of elements that have been
  // inserted into the Bloom Filter.
  def size: Approximate[Long]
}

/**
 * Empty bloom filter.
 */
case class BFZero[A](hashes: BFHash[A], width: Int) extends BF[A] {
  lazy val numHashes: Int = hashes.size

  def ++(other: BF[A]) = other

  def +(other: A) = BFItem[A](other, hashes, width)

  def checkAndAdd(other: A): (BF[A], ApproximateBoolean) = (this + other, ApproximateBoolean.exactFalse)

  def contains(item: A) = ApproximateBoolean.exactFalse

  def size = Approximate.exact[Long](0)
}

/**
 * Bloom Filter with 1 value.
 */
case class BFItem[A](item: A, hashes: BFHash[A], width: Int) extends BF[A] {
  lazy val numHashes: Int = hashes.size

  def ++(other: BF[A]): BF[A] = {
    other match {
      case bf @ BFZero(_, _) => this
      case bf @ BFItem(otherItem, _, _) => BFSparse[A](hashes, RichCBitSet(hashes(item): _*), width) + otherItem
      case _ => other + item
    }
  }

  def +(other: A) = this ++ BFItem(other, hashes, width)

  def checkAndAdd(other: A): (BF[A], ApproximateBoolean) = {
    if (other == item) {
      (this, ApproximateBoolean.exactTrue)
    } else {
      (this + other, ApproximateBoolean.exactFalse)
    }
  }

  def contains(x: A) = ApproximateBoolean.exact(item == x)

  def size = Approximate.exact[Long](1)
}

case class BFSparse[A](hashes: BFHash[A], bits: CBitSet, width: Int) extends BF[A] {
  import RichCBitSet._

  lazy val numHashes: Int = hashes.size

  @transient lazy val dense: BFInstance[A] = BFInstance[A](hashes, bits.toBitSet(width), width)

  def ++(other: BF[A]): BF[A] = {
    require(this.width == other.width)
    require(this.numHashes == other.numHashes)

    other match {
      case bf @ BFZero(_, _) => this
      case bf @ BFItem(item, _, _) => this + item
      case bf @ BFSparse(_, otherBits, _) => {
        // assume same hashes used
        BFSparse(hashes,
          bits ++ otherBits,
          width)
      }
      case _ => other ++ this
    }
  }

  def +(item: A): BF[A] = {
    val bitsToActivate = RichCBitSet(hashes(item): _*)

    BFSparse(hashes,
      bits ++ bitsToActivate,
      width)
  }

  def checkAndAdd(other: A): (BF[A], ApproximateBoolean) = dense.checkAndAdd(other)

  def contains(item: A): ApproximateBoolean = dense.contains(item)

  def size: Approximate[Long] = dense.size
}

/*
 * Bloom filter with multiple values
 */
case class BFInstance[A](hashes: BFHash[A], bits: BitSet, width: Int) extends BF[A] {

  lazy val numHashes: Int = hashes.size
  lazy val numBits: Int = bits.size

  def ++(other: BF[A]) = {
    require(this.width == other.width)
    require(this.numHashes == other.numHashes)

    other match {
      case bf @ BFZero(_, _) => this
      case bf @ BFItem(item, _, _) => this + item
      case bf @ BFSparse(_, _, _) => this ++ bf.dense
      case bf @ BFInstance(_, otherBits, _) => {
        // assume same hashes used
        BFInstance(hashes,
          bits ++ otherBits,
          width)
      }
    }
  }

  def +(item: A): BFInstance[A] = {
    val itemHashes = hashes(item)
    this.+(itemHashes: _*)
  }

  private def +(itemHashes: Int*): BFInstance[A] = {
    val bitsToActivate = BitSet(itemHashes: _*)

    BFInstance[A](hashes,
      bits ++ bitsToActivate,
      width)
  }

  def checkAndAdd(item: A): (BF[A], ApproximateBoolean) = {
    val itemHashes = hashes(item)
    val contained = this.contains(itemHashes: _*)
    (this.+(itemHashes: _*), contained)
  }

  private def bitSetContains(bs: BitSet, il: Int*): Boolean = {
    il.foreach { i => if (!bs.contains(i)) return false }
    true
  }

  def contains(item: A): ApproximateBoolean = {
    val itemHashes = hashes(item)
    contains(itemHashes: _*)
  }

  private[algebird] def contains(itemHashes: Int*): ApproximateBoolean = {
    if (bitSetContains(bits, itemHashes: _*)) {
      // The false positive probability (the probability that the Bloom filter erroneously
      // claims that an element x is in the set when x is not) is roughly
      // p = (1 - e^(-numHashes * setCardinality / width))^numHashes
      // See: http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives
      //
      // However, the true set cardinality may not be known. From empirical evidence, though,
      // it is upper bounded with high probability by 1.1 * estimatedCardinality (as long as the
      // Bloom filter is not too full), so we plug this into the formula instead.
      // TODO: investigate this upper bound and density more closely (or derive a better formula).
      val fpProb =
        if (density > 0.95) 1.0 // No confidence in the upper bound on cardinality.
        else scala.math.pow(1 - scala.math.exp(-numHashes * size.estimate * 1.1 / width), numHashes)

      ApproximateBoolean(true, 1 - fpProb)
    } else {
      // False negatives are not possible.
      ApproximateBoolean.exactFalse
    }
  }

  // Proportion of bits that are set to true.
  def density = numBits.toDouble / width

  /**
   * Cardinality estimates are taken from Theorem 1 on page 15 of
   * "Cardinality estimation and dynamic length adaptation for Bloom filters"
   * by Papapetrou, Siberski, and Nejdl:
   * http://www.softnet.tuc.gr/~papapetrou/publications/Bloomfilters-DAPD.pdf
   *
   * Roughly, by using bounds on the expected number of true bits after n elements
   * have been inserted into the Bloom filter, we can go from the actual number of
   * true bits (which is known) to an estimate of the cardinality.
   *
   * approximationWidth defines an interval around the maximum-likelihood cardinality
   * estimate. Namely, the approximation returned is of the form
   * (min, estimate, max) =
   *   ((1 - approxWidth) * estimate, estimate, (1 + approxWidth) * estimate)
   */
  lazy val size: Approximate[Long] = size(approximationWidth = 0.05)
  def size(approximationWidth: Double = 0.05): Approximate[Long] = {
    assert(0 <= approximationWidth && approximationWidth < 1, "approximationWidth must lie in [0, 1)")

    // Variable names correspond to those used in the paper.
    val t = numBits
    val n = sInverse(t).round.toInt
    // Take the min and max because the probability formula assumes
    // nl <= sInverse(t - 1) and sInverse(t + 1) <= nr
    val nl = scala.math.min(sInverse(t - 1).floor, (1 - approximationWidth) * n).toInt
    val nr = scala.math.max(sInverse(t + 1).ceil, (1 + approximationWidth) * n).toInt
    val prob =
      1 -
        scala.math.exp(t - 1 - s(nl)) *
        scala.math.pow(s(nl) / (t - 1), t - 1) -
        scala.math.exp(-scala.math.pow(t + 1 - s(nr), 2) / (2 * s(nr)))

    Approximate[Long](nl, n, nr, scala.math.max(0, prob))
  }

  /**
   * s(n) is the expected number of bits that have been set to true after
   * n elements have been inserted into the Bloom filter.
   * This is \hat{S}(n) in the cardinality estimation paper used above.
   */
  private def s(n: Int): Double = {
    width * (1 - scala.math.pow(1 - 1.0 / width, numHashes * n))
  }

  /**
   * sInverse(t) is the maximum likelihood value for the number of elements
   * that have been inserted into the Bloom filter when it has t bits set to true.
   * This is \hat{S}^{-1}(t) in the cardinality estimation paper used above.
   */
  private def sInverse(t: Int): Double = {
    scala.math.log(1 - t.toDouble / width) / (numHashes * scala.math.log(1 - 1.0 / width))
  }

}

object BFInstance {
  def apply[A](hashes: BFHash[A], width: Int): BFInstance[A] =
    BFInstance(hashes, BitSet.empty, width)
}

case class BFHash[A](numHashes: Int, width: Int)(implicit toBytes: A => Array[Byte]) extends Function1[A, Iterable[Int]] {
  val size = numHashes

  def apply(s: A) = nextHash(toBytes(s), numHashes)

  private def splitLong(x: Long) = {
    def toNonNegativeInt(x: Long) = {
      val y = math.abs(x).toInt // y may be negative (Integer.MIN_VALUE)
      y & 0x7fffffff // no change for positive numbers, converts Integer.MIN_VALUE to positive number
    }

    val upper = toNonNegativeInt(x >> 32)
    val lower = toNonNegativeInt((x << 32) >> 32)
    (upper, lower)
  }

  private def nextHash(bytes: Array[Byte], hashIndex: Int, digested: Seq[Int] = Seq.empty): Stream[Int] = {
    if (hashIndex == 0)
      Stream.empty
    else {
      val d = if (digested.isEmpty) {
        val (a, b) = MurmurHash128(hashIndex)(bytes)
        val (x1, x2) = splitLong(a)
        val (x3, x4) = splitLong(b)
        Seq(x1, x2, x3, x4)
      } else
        digested

      Stream.cons(d(0) % width, nextHash(bytes, hashIndex - 1, d.drop(1)))
    }
  }
}

case class BloomFilterAggregator[A](bfMonoid: BloomFilterMonoid[A]) extends MonoidAggregator[A, BF[A], BF[A]] {
  val monoid = bfMonoid

  def prepare(value: A) = monoid.create(value)
  def present(bf: BF[A]) = bf
}

object BloomFilterAggregator {
  def apply[A](numHashes: Int, width: Int)(implicit toBytes: A => Array[Byte]): BloomFilterAggregator[A] =
    BloomFilterAggregator[A](BloomFilterMonoid[A](numHashes, width))
}
