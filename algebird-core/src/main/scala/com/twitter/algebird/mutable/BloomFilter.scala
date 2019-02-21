/*
Copyright 2019 Twitter, Inc.

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

package com.twitter.algebird.mutable

import java.util

import algebra.BoundedSemilattice
import com.twitter.algebird.{
  BFHash => _,
  BloomFilter => ImmutableBloomFilter,
  BF => _,
  BloomFilterAggregator => _,
  BloomFilterMonoid => _,
  _
}

/**
 * Helpers for creating Bloom Filters. Most implementations regarding estimations are same as
 * Immutable Bloom Filters and these functions are aliases for them.
 */
object BloomFilter {

  def apply[A](numEntries: Int, fpProb: Double)(implicit hash: Hash128[A]): BloomFilterMonoid[A] =
    optimalWidth(numEntries, fpProb) match {
      case None =>
        throw new java.lang.IllegalArgumentException(
          s"BloomFilter cannot guarantee the specified false positive probability for the number of entries! (numEntries: $numEntries, fpProb: $fpProb)")
      case Some(width) =>
        val numHashes = optimalNumHashes(numEntries, width)
        BloomFilterMonoid[A](numHashes, width)(hash)
    }

  // Mostly an alias to actual functions defined for Immutable Bloom Filters.
  def optimalNumHashes(numEntries: Int, width: Int): Int =
    ImmutableBloomFilter.optimalNumHashes(numEntries, width)

  def optimalWidth(numEntries: Int, fpProb: Double): Option[Int] =
    ImmutableBloomFilter.optimalWidth(numEntries, fpProb)

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
   * ((1 - approxWidth) * estimate, estimate, (1 + approxWidth) * estimate)
   */
  def sizeEstimate(numBits: Int,
                   numHashes: Int,
                   width: Int,
                   approximationWidth: Double = 0.05): Approximate[Long] =
    ImmutableBloomFilter.sizeEstimate(numBits, numHashes, width, approximationWidth)

}

/**
 * Bloom Filter - a probabilistic data structure to test presence of an element.
 *
 * Operations
 * 1) insert: hash the value k times, updating the bitfield at the index equal to each hashed value
 * 2) query: hash the value k times.  If there are k collisions, then return true; otherwise false.
 *
 * http://en.wikipedia.org/wiki/Bloom_filter
 *
 * This implementation of the BloomFilterMonoid is mutable and adding elements changes the
 * filter. This is particularly useful when a filter needs to be created for a large number (>1M)
 * of elements at once and fast.
 */
case class BloomFilterMonoid[A](numHashes: Int, width: Int)(implicit hash: Hash128[A])
    extends Monoid[MutableBF[A]]
    with BoundedSemilattice[MutableBF[A]] {
  val hashes: MutableBFHash[A] = MutableBFHash[A](numHashes, width)(hash)

  val zero: MutableBF[A] = MutableBFZero[A](hashes, width)

  /**
   * Adds the Bloom Filter on right to the left, mutating and returning Left.
   * Assume that both have the same number of hashes and width.
   */
  override def plus(left: MutableBF[A], right: MutableBF[A]): MutableBF[A] =
    left ++= right

  override def sumOption(as: TraversableOnce[MutableBF[A]]): Option[MutableBF[A]] =
    if (as.isEmpty) {
      None
    } else {
      val outputInstance = MutableBFInstance.empty(hashes, width)
      as.foreach { bf =>
        outputInstance ++= bf
      }
      if (outputInstance.numBits == 0) {
        Some(MutableBFZero(hashes, width))
      } else {
        Some(outputInstance)
      }
    }

  /**
   * Create a bloom filter with one item.
   */
  def create(item: A): MutableBF[A] = MutableBFInstance(hashes, width, item)

  /**
   * Create a bloom filter with multiple items.
   */
  def create(data: A*): MutableBF[A] = create(data.iterator)

  /**
   * Create a bloom filter with multiple items from an iterator
   */
  def create(data: Iterator[A]): MutableBF[A] = {
    val outputInstance = MutableBFInstance.empty(hashes, width)
    data.foreach { itm =>
      outputInstance += itm
    }
    outputInstance
  }

}

object MutableBF {
  implicit def equiv[A]: Equiv[MutableBF[A]] =
    new Equiv[MutableBF[A]] {
      def equiv(a: MutableBF[A], b: MutableBF[A]): Boolean =
        (a eq b) || ((a.numHashes == b.numHashes) &&
          (a.width == b.width) &&
          a.toBitSet.equals(b.toBitSet))
    }
}

/**
 * A Mutable Bloom Filter data structure
 */
sealed abstract class MutableBF[A] extends java.io.Serializable {
  def numHashes: Int

  def width: Int

  /**
   * The number of bits set to true in the bloom filter
   */
  def numBits: Int

  /**
   * Proportion of bits that are set to true.
   */
  def density = numBits.toDouble / width

  def ++=(other: MutableBF[A]): MutableBF[A]

  def +=(other: A): MutableBF[A]

  def checkAndAdd(item: A): (MutableBF[A], ApproximateBoolean)

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
      // TODO: The following logic is same for immutable Bloom Filters and may be referred here.
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

  def toBitSet: util.BitSet

  def copy: MutableBF[A]

  /**
   * Compute the Hamming distance between the two Bloom filters
   * `a` and `b`. The distance is defined as the number of bits that
   * need to change to in order to transform one filter into the other.
   * This is computed using XOR but it doesn't mutate any BloomFilters
   */
  def hammingDistance(that: MutableBF[A]): Int =
    (this, that) match {
      // Comparing with empty filter should give number
      // of bits in other set
      case (x: MutableBFZero[A], y: MutableBFZero[A]) => 0
      case (x: MutableBFZero[A], y: MutableBF[A])     => y.numBits
      case (x: MutableBF[A], y: MutableBFZero[A])     => x.numBits

      // Otherwise compare as bit sets
      case (_, _) =>
        // hammingDistance should not mutate BloomFilter
        val thisCopy = this.toBitSet.clone().asInstanceOf[util.BitSet]
        thisCopy.xor(that.toBitSet)
        thisCopy.cardinality()
    }

}

/**
 * Empty bloom filter.
 */
case class MutableBFZero[A](hashes: MutableBFHash[A], width: Int) extends MutableBF[A] {

  def toBitSet: util.BitSet = new util.BitSet()

  def numHashes: Int = hashes.size

  def numBits = 0

  def ++=(other: MutableBF[A]): MutableBF[A] = other

  def +=(other: A): MutableBF[A] = MutableBFInstance[A](hashes, width, other)

  def checkAndAdd(other: A): (MutableBF[A], ApproximateBoolean) =
    (this += other, ApproximateBoolean.exactFalse)

  override def contains(item: A) = ApproximateBoolean.exactFalse

  def maybeContains(item: A): Boolean = false

  def size = Approximate.exact[Long](0)

  def copy: MutableBF[A] = MutableBFZero(hashes, width)
}

/*
 * Mutable Bloom filter with multiple values
 */
case class MutableBFInstance[A](hashes: MutableBFHash[A], bits: util.BitSet, width: Int)
    extends MutableBF[A] {

  def numHashes: Int = hashes.size

  /**
   * The number of bits set to true
   */
  def numBits: Int = bits.cardinality()

  def toBitSet: util.BitSet = bits

  def ++=(other: MutableBF[A]): MutableBF[A] = {
    require(this.width == other.width)
    require(this.numHashes == other.numHashes)

    other match {
      case MutableBFZero(_, _)                => this
      case MutableBFInstance(_, otherBits, _) =>
        // assume same hashes used
        bits.or(otherBits)
        this
    }
  }

  def +=(item: A): MutableBF[A] = {
    val itemHashes = hashes(item)
    itemHashes.foreach(bits.set)
    this
  }

  def checkAndAdd(other: A): (MutableBF[A], ApproximateBoolean) = {
    val doesContain = contains(other)
    (this += other, doesContain)
  }

  def maybeContains(item: A): Boolean = {
    val il = hashes(item)
    var idx = 0
    while (idx < il.length) {
      val i = il(idx)
      if (!bits.get(i)) return false
      idx += 1
    }
    true
  }

  // use an approximation width of 0.05
  def size: Approximate[Long] =
    BloomFilter.sizeEstimate(numBits, numHashes, width, 0.05)

  def copy: MutableBF[A] = MutableBFInstance(hashes, bits.clone.asInstanceOf[util.BitSet], width)
}

object MutableBFInstance {
  def apply[A](hashes: MutableBFHash[A], width: Int, firstElement: A): MutableBF[A] = {
    val bf = MutableBFInstance.empty(hashes, width)
    bf += firstElement
  }

  def apply[A](hashes: MutableBFHash[A], width: Int): MutableBF[A] =
    empty(hashes, width)

  def empty[A](hashes: MutableBFHash[A], width: Int): MutableBF[A] =
    MutableBFInstance(hashes, new util.BitSet(), width)
}

/**
 * Logic for creating `n` hashes for each item of the BloomFilter.
 *
 * The hash functions derived here are different than the one used for
 * com.twitter.algebird.BloomFilter and hence a com.twitter.algebird.mutable.BloomFilter
 * is incompatible with the immutable one and cannot be converted to one another.
 *
 * This hash function derivation is explained by Adam Kirsch and Michael Mitzenmacher here:
 * https://www.eecs.harvard.edu/~michaelm/postscripts/esa2006a.pdf
 *
 * We have noticed 2 to 4 times higher throughput when using this approach compared the one
 * for the immutable filter.
 */
case class MutableBFHash[A](numHashes: Int, width: Int)(implicit hash128: Hash128[A]) {
  val size: Int = numHashes

  def apply(valueToHash: A): Array[Int] = {
    val (hash1, hash2) = hash128.hashWithSeed(numHashes, valueToHash)

    val hashes = new Array[Int](numHashes)
    var i = 0
    while (i < numHashes) {
      // We just need two 32 bit hashes. So just convert toInt, and ignore the rest.
      hashes(i) = math.abs((hash1.toInt + i * hash2.toInt) % width)
      i += 1
    }
    hashes
  }
}

case class BloomFilterAggregator[A](bfMonoid: BloomFilterMonoid[A])
    extends MonoidAggregator[A, MutableBF[A], MutableBF[A]] {
  val monoid = bfMonoid

  def prepare(value: A) = monoid.create(value)

  def present(bf: MutableBF[A]) = bf
}

object BloomFilterAggregator {
  def apply[A](numHashes: Int, width: Int)(implicit hash: Hash128[A]): BloomFilterAggregator[A] =
    BloomFilterAggregator[A](BloomFilterMonoid[A](numHashes, width))
}
