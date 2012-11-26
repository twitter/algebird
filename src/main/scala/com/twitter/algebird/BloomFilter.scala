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

  def contains(item: String): Boolean
}

/**
 * Empty bloom filter.
 */
case class BFZero(hashes: BFHash, width: Int) extends BF {
  lazy val numHashes: Int = hashes.size

  def ++(other: BF) = other

  def + (other: String) = BFItem(other, hashes, width)

  def contains(item: String) = false
}

/**
 * Bloom Filter with 1 value.
 */
case class BFItem(item: String, hashes: BFHash, width: Int) extends BF {
  lazy val numHashes: Int = hashes.size

  def ++(other: BF): BF = {
    other match {
      case BFZero(_,_) => this
      case BFItem(otherItem,_,_) => {
        val bf = BFInstance(hashes,width)
        bf + item + otherItem
      }
      case bf@BFInstance(_, _, _) => bf + item
    }
  }

  def + (other: String) = this ++ BFItem(other, hashes, width)

  def contains(x: String) = item == x
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

  def contains(item: String) = {
    val otherBits = BitSet(hashes(item) : _*)
    // do this in case hashing function results in < numHashes number of unique bits
    (bits & otherBits) == otherBits
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

