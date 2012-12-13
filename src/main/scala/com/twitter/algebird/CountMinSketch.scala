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
 * It works as follows. Let (eps, delta) be two parameters that describe the
 * confidence in our error estimates, and let d = ceil(ln 1/delta)
 * and w = ceil(e / eps). Then:
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
 * With probability at least 1 - delta, this estimate is within eps * N
 * of the true frequency (i.e., true frequency <= estimate <= true frequency + eps * N),
 * where N is the total size of the stream so far.
 *
 * See http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf for technical details,
 * including proofs of the estimates and error bounds used in this implementation.
 *
 * Parts of this implementation are taken from
 * https://github.com/clearspring/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/frequency/CountMinSketch.java
 *
 * @author Edwin Chen
 */

/**
 * Monoid for adding Count-Min sketches.
 *
 * eps and delta are parameters that bound the error of each query estimate. For example, errors in
 * answering queries (e.g., how often has element x appeared in the stream described by the sketch?)
 * are often of the form: "with probability p >= 1 - delta, the estimate is close to the truth by
 * some factor depending on eps."
 *
 * @eps A parameter that bounds the error of each query estimate.
 * @delta A bound on the probability that a query estimate does not lie within some small interval
 *        (an interval that depends on eps) around the truth.
 * @seed  A seed to initialize the random number generator used to create the pairwise independent
 *        hash functions.
 */
class CountMinSketchMonoid(eps : Double, delta : Double, seed : Int) extends Monoid[CMS] {
  assert(0 < eps && eps < 1, "eps must lie in (0, 1).")
  assert(0 < delta && delta < 1, "delta must lie in (0, 1).")

  // Typically, we would use d pairwise independent hash functions of the form
  //
  //   h_i(x) = a_i * x + b_i (mod p)
  //
  // But for this particular application, setting b_i does not matter
  // (since all it does is shift the results of a particular hash),
  // so we omit it and simply use hash functions of the form
  //
  //   h_i(x) = a_i * x (mod p)
  val hashes : Seq[CMSHash] = {
    val r = new scala.util.Random(seed)
    val numHashes = CMS.depth(delta)
    val numCounters = CMS.width(eps)
    (0 to (numHashes - 1)).map { _ => CMSHash(r.nextInt, 0, numCounters) }
  }

  val zero : CMS = CMSZero(hashes, eps, delta)

  /**
   * We assume the Count-Min sketches on the left and right use the same hash functions.
   */
  def plus(left : CMS, right : CMS) : CMS = left ++ right

  /**
   * Create a Count-Min sketch out of a single item.
   */
  def create(item : Long) : CMS = CMSItem(item, hashes, eps, delta)

  /**
   * Creates a Count-Min sketch out of the given data stream.
   */
  def create(data : Seq[Long]) : CMS = {
    data.foldLeft(zero) { case (acc, x) => plus(acc, create(x)) }
  }
}

object CMS {
  def monoid(eps : Double, delta : Double, seed : Int) = new CountMinSketchMonoid(eps, delta, seed)
  def monoid(depth : Int, width : Int, seed : Int) = {
    new CountMinSketchMonoid(CMS.eps(width), CMS.delta(depth), seed)
  }

  /**
   * Functions to translate between (eps, delta) and (depth, width). The translation is:
   * depth = ceil(ln 1/delta)
   * width = ceil(e / eps)
   */
  def eps(width : Int) = scala.math.exp(1.0) / width
  def delta(depth : Int) = 1.0 / scala.math.exp(depth)
  def depth(delta : Double) = scala.math.ceil(scala.math.log(1.0 / delta)).toInt
  def width(eps : Double) = scala.math.ceil(scala.math.exp(1) / eps).toInt
}

/**
 * The actual Count-Min sketch data structure.
 */
sealed abstract class CMS extends java.io.Serializable {

  // Parameters used to bound confidence in error estimates.
  def eps : Double
  def delta : Double

  // Number of hash functions.
  def depth : Int = CMS.depth(delta)
  // Number of counters per hash function.
  def width : Int = CMS.width(eps)

  def ++(other : CMS) : CMS

  /**
   * Returns an estimate of the total number of times this item has been seen
   * in the stream so far.
   * Note that this estimate is an upper bound.
   * It is always true that trueFrequency <= estimatedFrequency.
   * With probability p >= 1 - delta, it also holds that
   * estimatedFrequency <= trueFrequency + eps * totalCount.
   */
  def frequency(item: Long): Approximate[Long]

  /**
   * Returns an estimate of the inner product against another data stream.
   *
   * In other words, let a_i denote the number of times element i has been seen in
   * the data stream summarized by this CMS, and let b_i denote the same for the other CMS.
   * Then this returns an estimate of <a, b> = \sum a_i b_i
   *
   * Note: this can also be viewed as the join size between two relations.
   * It is always true that actualInnerProduct <= estimatedInnerProduct.
   * With probability p >= 1 - delta, it also holds that
   * estimatedInnerProduct <= actualInnerProduct + eps * thisTotalCount * otherTotalCount
   */
  def innerProduct(other: CMS): Approximate[Long]

  // Total number of elements seen in the data stream so far.
  def totalCount : Long
  // The first frequency moment is the total number of elements in the stream.
  def f1 : Long = totalCount
  // The second frequency moment is \sum a_i^2, where a_i is the count of the ith element.
  def f2 : Approximate[Long] = innerProduct(this)
}

/**
 * Used for initialization.
 */
case class CMSZero(hashes : Seq[CMSHash], eps : Double, delta : Double) extends CMS {
  def totalCount = 0L
  def ++(other : CMS) = other
  def frequency(item : Long) = Approximate.exact(0L)
  def innerProduct(other : CMS) = Approximate.exact(0L)
}

/**
 * Used for holding a single element, to avoid repeatedly adding elements from
 * sparse counts tables.
 */
case class CMSItem(item : Long, hashes : Seq[CMSHash], eps : Double, delta : Double) extends CMS {
  def totalCount = 1L

  def ++(other : CMS) : CMS = {
    other match {
      case other : CMSZero => this
      case other : CMSItem => CMSInstance(hashes, eps, delta) + item + other.item
      case other : CMSInstance => other + item
    }
  }

  def frequency(x : Long) = if (item == x) Approximate.exact(1L) else Approximate.exact(0L)

  def innerProduct(other : CMS) : Approximate[Long] = other.frequency(item)
}

/**
 * The general Count-Min sketch structure, used for holding any number of elements.
 */
case class CMSInstance(hashes : Seq[CMSHash], countsTable : CMSCountsTable, totalCount : Long,
                       eps : Double, delta : Double) extends CMS {

  def ++(other : CMS) : CMS = {
    other match {
      case other : CMSZero => other ++ this
      case other : CMSItem => other ++ this
      case other : CMSInstance => {
        CMSInstance(hashes, countsTable ++ other.countsTable, totalCount + other.totalCount, eps, delta)
      }
    }
  }

  private def makeApprox(est: Long): Approximate[Long] = {
    if(est == 0L) {
      Approximate.exact(0L)
    }
    else {
      val lower = math.max(0L, est - (eps * totalCount).toLong)
      Approximate(lower, est, est, 1 - delta)
    }
  }

  def frequency(item : Long) : Approximate[Long] = {
    val estimates = countsTable.counts.zipWithIndex.map { case (row, i) =>
      row(hashes(i)(item))
    }
    makeApprox(estimates.min)
  }

  /**
   * Let X be a CMS, and let count_X[j, k] denote the value in X's 2-dimensional count table at row j and
   * column k.
   * Then the Count-Min sketch estimate of the inner product between A and B is the minimum inner product
   * between their rows:
   * estimatedInnerProduct = min_j (\sum_k count_A[j, k] * count_B[j, k])
   */
  def innerProduct(other : CMS) : Approximate[Long] = {
    other match {
      case other : CMSInstance => {
        assert((other.depth, other.width) == (depth, width), "Tables must have the same dimensions.")

        def innerProductAtDepth(d : Int) = (0 to (width - 1)).map { w =>
                                              countsTable.getCount(d, w) * other.countsTable.getCount(d, w)
                                           }.sum

        val est = (0 to (depth - 1)).map { innerProductAtDepth(_) }.min
        Approximate(est - (eps * totalCount * other.totalCount).toLong, est, est, 1 - delta)
      }
      case _ => other.innerProduct(this)
    }
  }

  /**
   * Updates the sketch with a new element from the data stream.
   */
  def +(item : Long) : CMSInstance = this + (item, 1L)
  def +(item : Long, count : Long) : CMSInstance = {
    if (count < 0) {
      throw new Exception("Negative counts not implemented")
    } else {
      val newCountsTable =
        (0 to (depth - 1)).foldLeft(countsTable) { case (table, row) =>
          val pos = (row, hashes(row)(item))
          table + (pos, count)
        }
      val newCount = totalCount + count

      CMSInstance(hashes, newCountsTable, newCount, eps, delta)
    }
  }
}

object CMSInstance {
  // Initializes a CMSInstance with all zeroes.
  def apply(hashes : Seq[CMSHash], eps : Double, delta : Double) : CMSInstance = {
    CMSInstance(hashes, CMSCountsTable(CMS.depth(delta), CMS.width(eps)), 0, eps, delta)
  }
}

/**
 * The Count-Min sketch uses pairwise independent hash functions drawn from
 * a universal hashing family of the form
 *
 *   h(x) = [a * x + b (mod p)] (mod m)
 */
case class CMSHash(a : Int, b : Int, width : Int) extends Function1[Long, Int] {

  val PRIME_MODULUS = (1L << 31) - 1

  /**
   * Returns a * x + b (mod p) (mod width)
   */
  def apply(x : Long) : Int = {
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
case class CMSCountsTable(counts : Vector[Vector[Long]]) {
  assert(depth > 0, "Table must have at least 1 row.")
  assert(width > 0, "Table must have at least 1 column.")

  def depth : Int = counts.size
  def width : Int = counts(0).size

  def getCount(pos : (Int, Int)) : Long = {
    val (row, col) = pos

    assert(row < depth && col < width, "Position must be within the bounds of this table.")

    counts(row)(col)
  }

  /**
   * Updates the count of a single cell in the table.
   */
  def +(pos : (Int, Int), count : Long) : CMSCountsTable = {
    val (row, col) = pos
    val currCount = getCount(pos)
    val newCounts = counts.updated(row, counts(row).updated(col, currCount + count))

    CMSCountsTable(newCounts)
  }

  /**
   * Adds another counts table to this one, through elementwise addition.
   */
  def ++(other : CMSCountsTable) : CMSCountsTable = {
    assert((depth, width) == (other.depth, other.width), "Tables must have the same dimensions.")

    (0 to (depth - 1)).zip(0 to (width - 1)).foldLeft(this) {
      case (table, pos) => table + (pos, other.getCount(pos))
    }
  }
}

object CMSCountsTable {
   // Creates a new CMSCountsTable with counts initialized to all zeroes.
  def apply(depth : Int, width : Int) : CMSCountsTable = CMSCountsTable(Vector.fill[Long](depth, width)(0L))
}
