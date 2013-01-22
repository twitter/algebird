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

import com.googlecode.javaewah.{EWAHCompressedBitmap => CBitSet}

object RichCBitSet {
  def apply(x : Int*) = {
    CBitSet.bitmapOf(x.sorted : _*)
  }
  implicit def cb2rcb(cb : CBitSet) : RichCBitSet = new RichCBitSet(cb)
  implicit def rcb2cb(rcb : RichCBitSet) : CBitSet = rcb.cb
}

class RichCBitSet(val cb : CBitSet) {
  def ++(b : CBitSet) : CBitSet = cb.or(b)

  def &(b : CBitSet) : CBitSet = cb.and(b)
  
  def ==(b : CBitSet) : Boolean = cb.equals(b)

  def toBitSet(width : Int) : BitSet = {
    val a = new Array[Long]((width+63)/64)
    cb.asScala.foreach{ i : java.lang.Integer => a(i.intValue / 64) |= 1L << (i.intValue % 64) }
    BitSet.fromArray(a)
  }
}

object BloomFilter{

  def apply(numEntries: Int, fpProb: Double, seed: Int = 0) = {
    val width = BloomFilter.optimalWidth(numEntries, fpProb)
    val numHashes = BloomFilter.optimalNumHashes(numEntries, width)
    BloomFilterMonoid(numHashes, width, seed)
  }


  // compute optimal number of hashes: k = m/n ln(2)
  def optimalNumHashes(numEntries: Int, width: Int): Int = math.ceil(width / numEntries * math.log(2)).toInt

  // compute optimal width: m = - n ln(p) / (ln(2))^2
  def optimalWidth(numEntries: Int, fpProb: Double): Int = math.ceil(-1 * numEntries * math.log(fpProb) / math.log(2) / math.log(2)).toInt
}

/**
 * Bloom Filter - a probabilistic data structure to test presence of an element.
 *
 * Operations
 *   1) insert: hash the value k times, updating the bitfield at the index equal to the each hashed value
 *   2) query: hash the value k times.  If there are k collisions, then return true; otherwise false.
 *
 * http://en.wikipedia.org/wiki/Bloom_filter
 *
 */
case class BloomFilterMonoid(numHashes: Int, width: Int, seed: Int) extends Monoid[BF]{
  val hashes: BFHash = BFHash(numHashes, width, seed)

  val zero: BF = BFZero(hashes, width)



  /**
   * Assume the bloom filters are compatible (same width and same hashing functions).  This
   * is the union of the 2 bloom filters.
   */
  def plus(left: BF, right: BF): BF = left ++ right

  /**
   * Create a bloom filter with one item.
   */
  def create(item: String): BF = BFItem(item, hashes, width)


  /**
   * Create a bloom filter with multiple items.
   */
  def create(data: String*): BF = {
    data.foldLeft(zero) { case (acc, x) => plus(acc, create(x)) }
  }
}


/**
 * Bloom Filter data structure
 */
sealed abstract class BF extends java.io.Serializable{
  val numHashes: Int

  val width: Int

  def ++ (other: BF): BF

  def + (other: String): BF

  def contains(item: String): ApproximateBoolean

  // Estimates the cardinality of the set of elements that have been
  // inserted into the Bloom Filter.
  def size: Approximate[Long]
}

/**
 * Empty bloom filter.
 */
case class BFZero(hashes: BFHash, width: Int) extends BF {
  lazy val numHashes: Int = hashes.size

  def ++(other: BF) = other

  def + (other: String) = BFItem(other, hashes, width)

  def contains(item: String) = ApproximateBoolean.exactFalse

  def size = Approximate.exact[Long](0)
}

/**
 * Bloom Filter with 1 value.
 */
case class BFItem(item: String, hashes: BFHash, width: Int) extends BF {
  lazy val numHashes: Int = hashes.size

  def ++(other: BF): BF = {
    other match {
      case bf@BFZero(_,_) => this
      case bf@BFItem(otherItem,_,_) => BFSparse(hashes,RichCBitSet(hashes(item).toList ++ hashes(otherItem).toList : _*), width)
      case bf@BFInstance(_, _, _) => bf + item
      case bf@BFSparse(_, _, _) => bf + item
    }
  }

  def + (other: String) = this ++ BFItem(other, hashes, width)

  def contains(x: String) = ApproximateBoolean.exact(item == x)

  def size = Approximate.exact[Long](1)
}

case class BFSparse(hashes : BFHash, bits : RichCBitSet, width : Int) extends BF {
  lazy val numHashes: Int = hashes.size

  lazy val dense : BFInstance = BFInstance(hashes, bits.toBitSet(width), width)

  def ++ (other: BF): BF = {
    require(this.width == other.width)
    require(this.numHashes == other.numHashes)

    other match {
      case bf@BFZero(_,_) => bf ++ this
      case bf@BFItem(_,_,_) => bf ++ this
      case bf@BFInstance(_,otherBits,_) => {
        // assume same hashes used
        BFInstance(hashes,
                   dense.bits ++ otherBits,
                   width)
      }
      case bf@BFSparse(_,otherBits,_) => {
        // assume same hashes used
        BFSparse(hashes,
                 bits ++ otherBits,
                 width)
      }
    }
  }

  def + (item: String): BF = {
    val bitsToActivate = RichCBitSet(hashes(item) : _*)

    BFSparse(hashes,
             bits ++ bitsToActivate,
             width)
  }

  def contains(item: String): ApproximateBoolean = dense.contains(item)

  def size: Approximate[Long] = dense.size
}

/*
 * Bloom filter with multiple values
 */
case class BFInstance(hashes : BFHash, bits: BitSet, width: Int) extends BF {

  lazy val numHashes: Int = hashes.size

  def ++ (other: BF) = {
    require(this.width == other.width)
    require(this.numHashes == other.numHashes)

    other match {
      case bf@BFZero(_,_) => bf ++ this
      case bf@BFItem(_,_,_) => bf ++ this
      case bf@BFSparse(_,_,_) => bf ++ this
      case bf@BFInstance(_,otherBits,_) => {
        // assume same hashes used
        BFInstance(hashes,
                   bits ++ otherBits,
                   width)
      }
    }
  }

  def + (item: String): BFInstance = {
    val bitsToActivate = BitSet(hashes(item) : _*)

    BFInstance(hashes,
               bits ++ bitsToActivate,
               width)
  }

  // faster than == on two bitsets.
  def bitSetContains(bs : BitSet, il : Int*) : Boolean = {
    il.map{i : Int => bs.contains(i)}.reduce{_&&_}
  }

  def contains(item: String) = {
    if (bitSetContains(bits, hashes(item) : _*)) {
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
  def density = bits.size.toDouble / width

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
  lazy val size : Approximate[Long] = size(approximationWidth = 0.05)
  def size(approximationWidth : Double = 0.05) : Approximate[Long] = {
    assert(0 <= approximationWidth && approximationWidth < 1, "approximationWidth must lie in [0, 1)")

    // Variable names correspond to those used in the paper.
    val t = bits.size
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
  private def s(n : Int) : Double = {
    width * (1 - scala.math.pow(1 - 1.0 / width, numHashes * n))
  }

  /**
   * sInverse(t) is the maximum likelihood value for the number of elements
   * that have been inserted into the Bloom filter when it has t bits set to true.
   * This is \hat{S}^{-1}(t) in the cardinality estimation paper used above.
   */
  private def sInverse(t : Int) : Double = {
    scala.math.log(1 - t.toDouble / width) / (numHashes * scala.math.log(1 - 1.0 / width))
  }

}

object BFInstance{
  def apply(hashes: BFHash, width: Int): BFInstance =
    BFInstance(hashes, BitSet.empty, width)
}

case class BFHash(numHashes: Int, width: Int, seed: Long = 0L) extends Function1[String, Iterable[Int]]{
  val size = numHashes

  def apply(s: String) = nextHash(s.getBytes, numHashes)

  private def splitLong(x: Long) = {
    val upper = math.abs(x >> 32).toInt
    val lower = math.abs((x << 32) >> 32).toInt
    (upper, lower)
  }

  private def nextHash(bytes: Array[Byte], k: Int, digested: Seq[Int] = Seq.empty): Stream[Int] = {
    if(k == 0)
      Stream.empty
    else{
      val d = if(digested.isEmpty){
        val (a, b) = MurmurHash128(k)(bytes)
        val (x1, x2) = splitLong(a)
        val (x3, x4) = splitLong(b)
        Seq(x1, x2, x3, x4)
      }else
        digested

      Stream.cons(d(0) % width, nextHash(bytes, k - 1, d.drop(1)))
    }
  }
}

