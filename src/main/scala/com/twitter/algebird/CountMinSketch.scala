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

/**
 * A Count-Min sketch is a probabilistic data structure used for summarizing
 * streams of data in sub-linear space.
 *
 * It works as follows:
 *
 * - Take d pairwise independent hash functions h_i, each of which maps
 *   onto the domain [0, w - 1].
 * - Create a 2-dimensional table of counts, with d rows and w columns,
 *   initialized with all zeroes.
 * - When a new element x arrives in the stream, update the table of counts
 *   by setting counts[i, h_i[x]] += 1, for each 1 <= i <= d.
 * - (Note the rough similarity to a Bloom filter.)
 *
 * As an example application, suppose you want to estimate the number of 
 * times an element x has appeared in a data stream so far. 
 * The Count-Min sketch estimate of this frequency is
 *
 *   min_i { counts[i, h_i[x]] }
 * 
 * With probability at least 1 - 0.5^d, this estimate is within 2N / w
 * of the true frequency (i.e., true frequency <= estimate <= true frequency + 2N / w),
 * where N is the total size of the stream so far.
 *
 * See http://en.wikipedia.org/wiki/Count-Min_sketch for more information.
 * Parts of this implementation are taken from
 * https://github.com/clearspring/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/frequency/CountMinSketch.java
 *
 * @author Edwin Chen
 */

/**
 * Monoid for adding Count-Min sketches.
 *
 * @depth Number of hash functions used.
 * @width Number of counters per hash function.
 * @seed  A seed to initialize the random number generator used to create
 *        the pairwise independent hash functions.
 */
class CountMinSketchMonoid(depth : Int, width : Int, seed : Int) extends Monoid[CMS] {
  
  val RAND = new scala.util.Random(seed)

  // Typically, we would use d pairwise independent hash functions of the form
  //
  //   h_i(x) = a_i * x + b_i (mod p)
  //
  // But for this particular application, setting b_i does not matter
  // (since all it does is shift the results of a particular hash),
  // so we omit it and simply use hash functions of the form
  //
  //   h_i(x) = a_i * x (mod p)
  val hashes : Seq[CMSHash] = (0 to (depth - 1)).map { _ => CMSHash(RAND.nextInt, 0, width) }
  
  val zero : CMS = CMSZero(hashes, depth, width)
  
  /**
   * We assume the Count-Min sketches on the left and right use the same hash functions.
   */
  def plus(left : CMS, right : CMS) : CMS = left + right
  
  /**
   * Create a Count-Min sketch out of a single item.
   */
  def create(item : Long) : CMS = CMSItem(item, hashes, depth, width)
}

/**
 * The actual Count-Min sketch data structure.
 */
sealed abstract class CMS extends java.io.Serializable {
  // The total number of elements seen in the data stream so far.
  var totalCount : Long
  
  def depth : Int
  def width : Int
  
  def +(other : CMS) : CMS
  
  /**
   * Returns an estimate of the total number of times this item has been seen
   * in the stream so far.
   * Note that this estimate is an upper bound.
   */  
  def estimateFrequency(item : Long) : Long
  
  /**
   * With probability p >= 1 - 0.5^depth, the Count-Min sketch estimate
   * of the frequency of any element is within 2 * totalCount / width
   * of the true frequency.
   */
  def frequencyConfidence = 1 - 1 / math.pow(2.0, depth)
  def maxErrorOfFrequencyEstimate = (2.0 * totalCount) / width  
}

/**
 * Used for initialization.
 */
case class CMSZero(hashes : Seq[CMSHash], depth : Int, width : Int) extends CMS {
  var totalCount = 0L  
  def +(other : CMS) = other
  def estimateFrequency(item : Long) = 0L
}

/**
 * Used for holding a single element, to avoid repeatedly adding elements from
 * sparse counts tables.
 */
case class CMSItem(item : Long, hashes : Seq[CMSHash], depth : Int, width : Int) extends CMS {
  var totalCount = 1L
  
  def +(other : CMS) : CMS = {
    other match {
      case CMSZero(_, _, _) => this
      case CMSItem(otherItem, _, _, _) => {
        val cms = CMSInstance(hashes, depth, width)
        cms.add(item).add(otherItem)
      }
      case cms@CMSInstance(_, _, _) => cms.add(item)
    }
  }
  
  def estimateFrequency(x : Long) = if (item == x) 1L else 0L
}

/**
 * The general sketch structure, used for holding any number of elements.
 */
case class CMSInstance(hashes : Seq[CMSHash], depth : Int, width : Int) extends CMS {
  val countsTable = CMSCountsTable(depth, width)

  var totalCount = 0L
  
  def +(other : CMS) : CMS = {
    other match {
      case cms@CMSZero(_, _, _) => cms + this
      case cms@CMSItem(_, _, _, _) => cms + this
      case cms@CMSInstance(_, _, _) => {
        countsTable ++= cms.countsTable
        totalCount += cms.totalCount
        this
      }
    }
  }
  
  def estimateFrequency(item : Long) : Long = {
    val estimates = countsTable.counts.zipWithIndex.map { case (row, i) => row(hashes(i).compute(item)) }
    estimates.min
  }
  
  /**
   * Updates the sketch with a new element from the data stream.
   */
  def add(item : Long) : CMSInstance = add(item, 1)
  def add(item : Long, count : Long) : CMSInstance = {
    if (count < 0) {
      throw new Exception("Negative counts not implemented")
    } else {
      (0 to (depth - 1)).foreach { i =>
        val hash = hashes(i).compute(item)
        countsTable += (i, hash, 1)
      }
      totalCount += count
    }
    this
  }
}

/**
 * The Count-Min sketch uses pairwise independent hash functions drawn from
 * a universal hashing family of the form
 *
 *   h(x) = [a * x + b (mod p)] (mod m)
 */
case class CMSHash(a : Int, b : Int, width : Int) {
  
  val PRIME_MODULUS = (1L << 31) - 1
  
  /**
   * Returns a * x + b (mod p) (mod width)
   */
  def compute(x : Long) : Int = {
    val unmodded = a * x + b
    
    // Apparently a super fast way of computing x mod 2^p-1
    // See page 149 of
    // http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
    // after Proposition 7.
    val modded = (unmodded + (unmodded >> 32)) & PRIME_MODULUS
    
    // Modulo-ing integers is apparently twice as fast as
    // modulo-ing Longs.
    modded.toInt % width
  }
}

/**
 * The 2-dimensional table of counters used in the Count-Min sketch.
 * Each row corresponds to a particular hash function.
 */
case class CMSCountsTable(depth : Int, width : Int) {
  assert(depth > 0, "Table must have at least 1 row.")
  assert(width > 0, "Table must have at least 1 column.")
  
  val counts : Array[Array[Long]] = Array.fill(depth, width)(0L)

  def +=(row : Int, col : Int, count : Long) = { 
    counts(row)(col) += count
  }
  
  /**
   * Adds another counts table to this one, through elementwise addition.
   */
  def ++=(other : CMSCountsTable) = {
    assert((depth, width) == (other.depth, other.width), "Tables must have the same dimensions.")
    
    (0 to (depth - 1)).foreach { row =>
      (0 to (width - 1)).foreach { col =>
        this += (row, col, other.counts(row)(col))
      }
    }
  }
}
