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

import algebra.BoundedSemilattice
import com.twitter.algebird.immutable.BitSet

import scala.collection.compat._

object BloomFilter {

  def apply[A](numEntries: Int, fpProb: Double)(implicit hash: Hash128[A]): BloomFilterMonoid[A] =
    BloomFilter.optimalWidth(numEntries, fpProb) match {
      case None =>
        throw new java.lang.IllegalArgumentException(
          s"BloomFilter cannot guarantee the specified false positive probability for the number of entries! (numEntries: $numEntries, fpProb: $fpProb)"
        )
      case Some(width) =>
        val numHashes = BloomFilter.optimalNumHashes(numEntries, width)
        BloomFilterMonoid[A](numHashes, width)(hash)
    }

  // Compute optimal number of hashes: k = m/n ln(2)
  def optimalNumHashes(numEntries: Int, width: Int): Int =
    math.ceil(width / numEntries * math.log(2)).toInt

  // Compute optimal width: m = - n ln(p) / (ln(2))^2
  // return None if we can't meet this false positive probability
  def optimalWidth(numEntries: Int, fpProb: Double): Option[Int] = {
    val widthEstimate = math
      .ceil(-1 * numEntries * math.log(fpProb) / math.log(2) / math.log(2))
      .toInt

    if (widthEstimate == Int.MaxValue) None
    else Some(widthEstimate)
  }

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
  def sizeEstimate(
      numBits: Int,
      numHashes: Int,
      width: Int,
      approximationWidth: Double = 0.05
  ): Approximate[Long] = {
    assert(0 <= approximationWidth && approximationWidth < 1, "approximationWidth must lie in [0, 1)")

    /**
     * s(n) is the expected number of bits that have been set to true after
     * n elements have been inserted into the Bloom filter.
     * This is \hat{S}(n) in the cardinality estimation paper used above.
     */
    def s(n: Int): Double =
      width * (1 - scala.math.pow(1 - 1.0 / width, numHashes * n))

    /**
     * sInverse(t) is the maximum likelihood value for the number of elements
     * that have been inserted into the Bloom filter when it has t bits set to true.
     * This is \hat{S}^{-1}(t) in the cardinality estimation paper used above.
     */
    def sInverse(t: Int): Double =
      scala.math.log1p(-t.toDouble / width) / (numHashes * scala.math.log1p(-1.0 / width))

    // Variable names correspond to those used in the paper.
    val t = numBits
    val n = sInverse(t).round.toInt
    // Take the min and max because the probability formula assumes
    // nl <= sInverse(t - 1) and sInverse(t + 1) <= nr
    val nl =
      scala.math.min(sInverse(t - 1).floor, (1 - approximationWidth) * n).toInt
    val nr =
      scala.math.max(sInverse(t + 1).ceil, (1 + approximationWidth) * n).toInt
    val prob =
      1 -
        scala.math.exp(t - 1 - s(nl)) *
          scala.math.pow(s(nl) / (t - 1), t - 1) -
        scala.math.exp(-scala.math.pow(t + 1 - s(nr), 2) / (2 * s(nr)))

    Approximate[Long](nl, n, nr, scala.math.max(0, prob))
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
case class BloomFilterMonoid[A](numHashes: Int, width: Int)(implicit hash: Hash128[A])
    extends Monoid[BF[A]]
    with BoundedSemilattice[BF[A]] {
  val hashes: BFHash[A] = BFHash[A](numHashes, width)(hash)

  override val zero: BF[A] = BFZero[A](hashes, width)

  /**
   * Assume the bloom filters are compatible (same width and same hashing functions).  This
   * is the union of the 2 bloom filters.
   */
  override def plus(left: BF[A], right: BF[A]): BF[A] = left ++ right

  override def sumOption(as: TraversableOnce[BF[A]]): Option[BF[A]] =
    if (as.iterator.isEmpty) None
    else {
      var bfItem: BFItem[A] = null
      val iter = as.iterator
      var counter = 0
      var bs = BitSet.newEmpty(0)

      while (iter.hasNext) {
        iter.next() match {
          case _: BFZero[A] => ()
          case bi @ BFItem(item, _, _) =>
            bfItem = bi
            val array = hashes(item)
            counter += array.length
            bs = bs.mutableAdd(array)
          case BFInstance(_, bitset, _) =>
            val iter = bitset.iterator
            while (iter.hasNext) {
              counter += 1
              bs = bs.mutableAdd(iter.next())
            }
        }
      }

      if (counter == 0) Some(zero)
      else if (counter == numHashes && (bfItem != null)) Some(bfItem)
      else Some(BFInstance(hashes, bs, width))
    }

  /**
   * Create a bloom filter with one item.
   */
  def create(item: A): BF[A] = BFItem(item, hashes, width)

  /**
   * Create a bloom filter with multiple items.
   */
  def create(data: A*): BF[A] = create(data.iterator)

  /**
   * Create a bloom filter with multiple items from an iterator
   */
  def create(data: Iterator[A]): BF[A] = sum(data.map(BFItem(_, hashes, width)))
}

object BF {
  implicit def equiv[A]: Equiv[BF[A]] =
    new Equiv[BF[A]] {
      def equiv(a: BF[A], b: BF[A]): Boolean =
        (a eq b) || ((a.numHashes == b.numHashes) &&
          (a.width == b.width) &&
          a.toBitSet.equals(b.toBitSet))
    }
}

/**
 * Bloom Filter data structure
 */
sealed abstract class BF[A] extends java.io.Serializable {
  def numHashes: Int
  def width: Int

  /**
   * The number of bits set to true in the bloom filter
   */
  def numBits: Int

  /**
   * Proportion of bits that are set to true.
   */
  def density: Double = numBits.toDouble / width

  def ++(other: BF[A]): BF[A]

  def +(other: A): BF[A]

  def checkAndAdd(item: A): (BF[A], ApproximateBoolean)

  def contains(item: A): ApproximateBoolean =
    if (maybeContains(item)) {
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
        if (density > 0.95)
          1.0 // No confidence in the upper bound on cardinality.
        else
          scala.math.pow(1 - scala.math.exp(-numHashes * size.estimate * 1.1 / width), numHashes)

      ApproximateBoolean(true, 1 - fpProb)
    } else {
      // False negatives are not possible.
      ApproximateBoolean.exactFalse
    }

  /**
   * This may be faster if you don't care about evaluating
   * the false positive probability
   */
  def maybeContains(item: A): Boolean

  // Estimates the cardinality of the set of elements that have been
  // inserted into the Bloom Filter.
  def size: Approximate[Long]

  def toBitSet: BitSet

  /**
   * Compute the Hamming distance between the two Bloom filters
   * `a` and `b`. The distance is defined as the number of bits that
   * need to change to in order to transform one filter into the other.
   */
  def hammingDistance(that: BF[A]): Int =
    (this, that) match {
      // Comparing with empty filter should give number
      // of bits in other set
      case (_: BFZero[A], _: BFZero[A]) => 0
      case (_: BFZero[A], y: BF[A])     => y.numBits
      case (x: BF[A], _: BFZero[A])     => x.numBits

      // Otherwise compare as bit sets
      case (_, _) => (this.toBitSet ^ that.toBitSet).size.toInt
    }

}

/**
 * Empty bloom filter.
 */
case class BFZero[A](hashes: BFHash[A], override val width: Int) extends BF[A] {

  override def toBitSet: BitSet = BitSet()

  override def numHashes: Int = hashes.size

  override def numBits = 0

  override def ++(other: BF[A]): BF[A] = other

  override def +(other: A): BFItem[A] = BFItem[A](other, hashes, width)

  override def checkAndAdd(other: A): (BF[A], ApproximateBoolean) =
    (this + other, ApproximateBoolean.exactFalse)

  override def contains(item: A): ApproximateBoolean = ApproximateBoolean.exactFalse

  override def maybeContains(item: A): Boolean = false

  override def size: Approximate[Long] = Approximate.exact[Long](0)
}

/**
 * Bloom Filter with 1 value.
 */
case class BFItem[A](item: A, hashes: BFHash[A], override val width: Int) extends BF[A] {
  override def numHashes: Int = hashes.size
  override def numBits: Int = numHashes

  override def toBitSet: BitSet = BitSet(hashes(item))

  override def ++(other: BF[A]): BF[A] =
    other match {
      case BFZero(_, _) =>
        this
      case BFItem(otherItem, _, _) =>
        BFInstance[A](hashes, BitSet(hashes(item) ++ hashes(otherItem)), width)
      case _ =>
        other + item
    }

  override def +(other: A): BF[A] = this ++ BFItem(other, hashes, width)

  override def checkAndAdd(other: A): (BF[A], ApproximateBoolean) =
    if (other == item) {
      (this, ApproximateBoolean.exactTrue)
    } else {
      (this + other, ApproximateBoolean.exactFalse)
    }

  override def contains(x: A): ApproximateBoolean = ApproximateBoolean.exact(item == x)

  override def maybeContains(x: A): Boolean =
    item == x

  override def size: Approximate[Long] = Approximate.exact[Long](1)
}

/*
 * Bloom filter with multiple values
 */
case class BFInstance[A](hashes: BFHash[A], bits: BitSet, override val width: Int) extends BF[A] {

  override def numHashes: Int = hashes.size

  /**
   * The number of bits set to true
   */
  override def numBits: Int = bits.size.toInt

  override def toBitSet: BitSet = bits

  override def ++(other: BF[A]): BF[A] = {
    require(this.width == other.width)
    require(this.numHashes == other.numHashes)

    other match {
      case BFZero(_, _)                => this
      case BFItem(item, _, _)          => this + item
      case BFInstance(_, otherBits, _) =>
        // assume same hashes used
        BFInstance(hashes, bits | otherBits, width)
    }
  }

  override def +(item: A): BFInstance[A] =
    BFInstance[A](hashes, bits | BitSet(hashes(item)), width)

  override def checkAndAdd(other: A): (BF[A], ApproximateBoolean) =
    (this + other, contains(other))

  override def maybeContains(item: A): Boolean = {
    val il = hashes(item)
    var idx = 0
    while (idx < il.length) {
      val i = il(idx)
      if (!bits(i)) return false
      idx += 1
    }
    true
  }

  // use an approximation width of 0.05
  override def size: Approximate[Long] =
    BloomFilter.sizeEstimate(numBits, numHashes, width, 0.05)
}

object BFInstance {
  def apply[A](hashes: BFHash[A], width: Int): BFInstance[A] =
    empty(hashes, width)

  def empty[A](hashes: BFHash[A], width: Int): BFInstance[A] =
    BFInstance(hashes, BitSet.empty, width)
}

case class BFHash[A](numHashes: Int, width: Int)(implicit hash: Hash128[A]) {
  def size: Int = numHashes

  def apply(s: A): Array[Int] =
    nextHash(s, 0, new Array[Int](4), 4, new Array[Int](numHashes))

  private def splitLong(x: Long, buffer: Array[Int], idx: Int): Unit = {
    // unfortunately, this is the function we committed to some time ago, and we have tests
    // locking it down. x.toInt & 0x7fffffff should work, but this gives a few different values
    def toNonNegativeInt(x: Long): Int =
      (math
        .abs(x)
        .toInt) & 0x7fffffff // no change for positive numbers, converts Integer.MIN_VALUE to positive number

    val upper = toNonNegativeInt(x >> 32)
    val lower = toNonNegativeInt((x << 32) >> 32)
    buffer(idx) = upper
    buffer(idx + 1) = lower
  }

  @annotation.tailrec
  private def nextHash(
      valueToHash: A,
      hashIndex: Int,
      buffer: Array[Int],
      bidx: Int,
      target: Array[Int]
  ): Array[Int] =
    if (hashIndex == numHashes) target
    else {
      val thisBidx = if (bidx > 3) {
        val (a, b) =
          hash.hashWithSeed((numHashes - hashIndex).toLong, valueToHash)
        splitLong(a, buffer, 0)
        splitLong(b, buffer, 2)
        0
      } else bidx

      target(hashIndex) = buffer(thisBidx) % width
      nextHash(valueToHash, hashIndex + 1, buffer, thisBidx + 1, target)
    }
}

case class BloomFilterAggregator[A](bfMonoid: BloomFilterMonoid[A])
    extends MonoidAggregator[A, BF[A], BF[A]] {
  override val monoid: BloomFilterMonoid[A] = bfMonoid

  override def prepare(value: A): BF[A] = monoid.create(value)
  override def present(bf: BF[A]): BF[A] = bf
}

object BloomFilterAggregator {
  def apply[A](numHashes: Int, width: Int)(implicit hash: Hash128[A]): BloomFilterAggregator[A] =
    BloomFilterAggregator[A](BloomFilterMonoid[A](numHashes, width))
}
