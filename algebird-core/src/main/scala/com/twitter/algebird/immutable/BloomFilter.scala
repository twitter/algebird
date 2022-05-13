/*
Copyright 2020 Twitter, Inc.

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

package com.twitter.algebird.immutable

import algebra.BoundedSemilattice
import com.twitter.algebird.{Approximate, ApproximateBoolean, Hash128, Monoid, MonoidAggregator}

import scala.collection.compat._
import scala.util._

object BloomFilter {

  final def apply[A](numEntries: Int, fpProb: Double)(implicit hash: Hash128[A]): BloomFilter[A] =
    BloomFilter.optimalWidth(numEntries, fpProb) match {
      case None =>
        throw new java.lang.IllegalArgumentException(
          s"BloomFilter cannot guarantee the specified false positive probability for the number of entries! (numEntries: $numEntries, fpProb: $fpProb)"
        )
      case Some(width) =>
        val numHashes = BloomFilter.optimalNumHashes(numEntries, width)
        new BloomFilter(numHashes, width)
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
   * Cardinality estimates are taken from Theorem 1 on page 15 of "Cardinality estimation and dynamic length
   * adaptation for Bloom filters" by Papapetrou, Siberski, and Nejdl:
   * http://www.softnet.tuc.gr/~papapetrou/publications/Bloomfilters-DAPD.pdf
   *
   * Roughly, by using bounds on the expected number of true bits after n elements have been inserted into the
   * Bloom filter, we can go from the actual number of true bits (which is known) to an estimate of the
   * cardinality.
   *
   * approximationWidth defines an interval around the maximum-likelihood cardinality estimate. Namely, the
   * approximation returned is of the form (min, estimate, max) = ((1 - approxWidth) * estimate, estimate, (1
   * + approxWidth) * estimate)
   */
  def sizeEstimate(
      numBits: Int,
      numHashes: Int,
      width: Int,
      approximationWidth: Double = 0.05
  ): Approximate[Long] = {
    assert(0 <= approximationWidth && approximationWidth < 1, "approximationWidth must lie in [0, 1)")

    /**
     * s(n) is the expected number of bits that have been set to true after n elements have been inserted into
     * the Bloom filter. This is \hat{S}(n) in the cardinality estimation paper used above.
     */
    def s(n: Int): Double =
      width * (1 - scala.math.pow(1 - 1.0 / width, numHashes * n))

    /**
     * sInverse(t) is the maximum likelihood value for the number of elements that have been inserted into the
     * Bloom filter when it has t bits set to true. This is \hat{S}^{-1}(t) in the cardinality estimation
     * paper used above.
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
 * Operations 1) insert: hash the value k times, updating the bitfield at the index equal to each hashed value
 * 2) query: hash the value k times. If there are k collisions, then return true; otherwise false.
 *
 * http://en.wikipedia.org/wiki/Bloom_filter
 */
final case class BloomFilter[A](numHashes: Int, width: Int)(implicit val hash: Hash128[A]) { self =>

  /**
   * Hash the value `numHashes` times and return an array of indices of true bits in the [[BitSet]].
   */
  def hashToArray(s: A): Array[Int] = {
    val target = new Array[Int](numHashes)
    hashToArray(s, target)
    target
  }

  private def hashToArray(s: A, target: Array[Int]): Unit =
    nextHash(s, 0, new Array[Int](4), 4, target)

  private def splitLong(x: Long, buffer: Array[Int], idx: Int): Unit = {
    // unfortunately, this is the function we committed to some time ago, and we have tests
    // locking it down. x.toInt & 0x7fffffff should work, but this gives a few different values
    def toNonNegativeInt(x: Long): Int =
      math
        .abs(x)
        .toInt & 0x7fffffff // no change for positive numbers, converts Integer.MIN_VALUE to positive number

    val upper = toNonNegativeInt(x >> 32)
    val lower = toNonNegativeInt(x << 32 >> 32)
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
  ): Unit =
    if (hashIndex != numHashes) {
      val thisBidx = if (bidx > 3) {
        val (a, b) = hash.hashWithSeed((numHashes - hashIndex).toLong, valueToHash)
        splitLong(a, buffer, 0)
        splitLong(b, buffer, 2)
        0
      } else bidx

      target(hashIndex) = buffer(thisBidx) % width
      nextHash(valueToHash, hashIndex + 1, buffer, thisBidx + 1, target)
    }

  /**
   * Bloom Filter data structure
   */
  sealed abstract class Hash extends Serializable {
    def numHashes: Int = self.numHashes

    def width: Int = self.width

    /**
     * The number of bits set to true in the bloom filter
     */
    def numBits: Int

    /**
     * Proportion of bits that are set to true.
     */
    def density: Double = numBits.toDouble / width

    def ++(other: Hash): Hash

    def +(other: A): Hash

    def |(other: Hash): Hash = this ++ other

    def checkAndAdd(item: A): (Hash, ApproximateBoolean)

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
     * This may be faster if you don't care about evaluating the false positive probability
     */
    def maybeContains(item: A): Boolean

    // Estimates the cardinality of the set of elements that have been
    // inserted into the Bloom Filter.
    def size: Approximate[Long]

    def toBitSet: BitSet

    /**
     * Compute the Hamming distance between the two Bloom filters `a` and `b`. The distance is defined as the
     * number of bits that need to change to in order to transform one filter into the other.
     */
    def hammingDistance(that: Hash): Int =
      (this, that) match {
        // Comparing with empty filter should give number
        // of bits in other set
        case (Empty, Empty)   => 0
        case (Empty, y: Hash) => y.numBits
        case (x: Hash, Empty) => x.numBits

        // Otherwise compare as bit sets
        case _ => (this.toBitSet ^ that.toBitSet).size.toInt
      }

  }

  case object Empty extends Hash {
    override def toBitSet: BitSet = BitSet.empty

    override val numBits: Int = 0

    override def ++(other: Hash): Hash = other

    override def +(other: A): Item = Item(other)

    override def checkAndAdd(other: A): (Hash, ApproximateBoolean) =
      (this + other, ApproximateBoolean.exactFalse)

    override def contains(item: A): ApproximateBoolean = ApproximateBoolean.exactFalse

    override def maybeContains(item: A): Boolean = false

    override def size: Approximate[Long] = Approximate.exact[Long](0)
  }

  case class Item(item: A) extends Hash {
    override val numBits: Int = numHashes

    override def toBitSet: BitSet = BitSet(hashToArray(item))

    override def ++(other: Hash): Hash =
      other match {
        case Empty           => this
        case Item(otherItem) => this + otherItem
        case _               => other + item
      }

    override def +(other: A): Hash = {
      val bs = BitSet.newEmpty(0)
      val hash = new Array[Int](numHashes)

      hashToArray(item, hash)
      bs.mutableAdd(hash)
      hashToArray(other, hash)
      bs.mutableAdd(hash)

      Instance(bs)
    }

    override def checkAndAdd(other: A): (Hash, ApproximateBoolean) =
      if (other == item) {
        (this, ApproximateBoolean.exactTrue)
      } else {
        (this + other, ApproximateBoolean.exactFalse)
      }

    override def contains(x: A): ApproximateBoolean = ApproximateBoolean.exact(item == x)

    override def maybeContains(x: A): Boolean = item == x

    override def size: Approximate[Long] = Approximate.exact[Long](1)
  }

  /*
   * Bloom filter with 1 or more [[BitSet]].
   */
  case class Instance(bits: BitSet) extends Hash {

    /**
     * The number of bits set to true
     */
    override def numBits: Int = bits.size.toInt

    override val toBitSet: BitSet = bits

    override def ++(other: Hash): Hash =
      other match {
        case Empty               => this
        case Item(item)          => this + item
        case Instance(otherBits) => Instance(bits | otherBits)
      }

    override def +(item: A): Hash = Instance(bits | BitSet(hashToArray(item)))

    override def checkAndAdd(other: A): (Hash, ApproximateBoolean) =
      (this + other, contains(other))

    override def maybeContains(item: A): Boolean = {
      val il = hashToArray(item)
      var idx = 0
      var found = true
      while (idx < il.length && found) {
        val i = il(idx)
        if (!bits(i)) {
          found = false
        }
        idx += 1
      }
      found
    }

    // use an approximation width of 0.05
    override def size: Approximate[Long] =
      BloomFilter.sizeEstimate(numBits, numHashes, width, 0.05)
  }

  implicit val monoid: Monoid[Hash] with BoundedSemilattice[Hash] =
    new Monoid[Hash] with BoundedSemilattice[Hash] {
      override val zero: Hash = Empty

      /**
       * Assume the bloom filters are compatible (same width and same hashing functions). This is the union of
       * the 2 bloom filters.
       */
      override def plus(left: Hash, right: Hash): Hash = left ++ right

      override def sum(t: TraversableOnce[Hash]): Hash =
        if (t.iterator.isEmpty) empty
        else {
          val iter = t.iterator
          var bs = BitSet.newEmpty(0)
          val hash = new Array[Int](numHashes)

          while (iter.hasNext) {
            iter.next() match {
              case Empty => ()
              case Item(item) =>
                hashToArray(item, hash)
                bs = bs.mutableAdd(hash)
              case Instance(bitset) =>
                val iter = bitset.iterator
                while (iter.hasNext) {
                  bs = bs.mutableAdd(iter.next())
                }
            }
          }
          if (bs.isEmpty) Empty else Instance(bs)
        }

      override def sumOption(t: TraversableOnce[Hash]): Option[Hash] =
        if (t.iterator.isEmpty) None else Some(sum(t))
    }

  val aggregator: MonoidAggregator[A, Hash, Hash] = new MonoidAggregator[A, Hash, Hash] {
    override val monoid: Monoid[Hash] = self.monoid

    override def prepare(value: A): Hash = Item(value)

    override def present(bf: Hash): Hash = bf
  }

  implicit val equiv: Equiv[Hash] = new Equiv[Hash] {
    override def equiv(a: Hash, b: Hash): Boolean =
      (a eq b) || a.toBitSet.equals(b.toBitSet)
  }

  /**
   * Create a bloom filter with one item.
   */
  def create(item: A): Hash = Item(item)

  /**
   * Create a bloom filter with multiple items.
   */
  def create(data: A*): Hash = create(data.iterator)

  /**
   * Create a bloom filter with multiple items from an iterator
   */
  def create(data: Iterator[A]): Hash = monoid.sum(data.map(Item))

  val empty: Hash = Empty

  /**
   * Attempts to create a new BloomFilter instance from a [[BitSet]]. Failure might occur if the BitSet has a
   * maximum entry behond the BloomFilter expected size.
   *
   * This method will be helpfull on BloomFilter desirialization. Serialization is achieved through the
   * serialization of the underlying [[BitSet]].
   */
  def fromBitSet(bitSet: BitSet): Try[Hash] =
    if (bitSet.isEmpty) {
      Success(empty)
    } else {
      if (bitSet.reverseIterator.next() > width)
        Failure(new IllegalArgumentException("BitSet beyond BloomFilter expected size"))
      else
        Success(Instance(bitSet))
    }

}
