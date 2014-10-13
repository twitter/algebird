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

import scala.collection.immutable.SortedSet

/**
 * A Count-Min sketch is a probabilistic data structure used for summarizing
 * streams of data in sub-linear space.
 *
 * It works as follows. Let `(eps, delta)` be two parameters that describe the
 * confidence in our error estimates, and let `d = ceil(ln 1/delta)`
 * and `w = ceil(e / eps)`.
 *
 * Note: Throughout the code `d` and `w` are called `depth` and `width`,
 * respectively.
 *
 * Then:
 *
 * - Take `d` pairwise independent hash functions `h_i`, each of which maps
 *   onto the domain `[0, w - 1]`.
 * - Create a 2-dimensional table of counts, with `d` rows and `w` columns,
 *   initialized with all zeroes.
 * - When a new element x arrives in the stream, update the table of counts
 *   by setting `counts[i, h_i[x]] += 1`, for each `1 <= i <= d`.
 * - (Note the rough similarity to a Bloom filter.)
 *
 * As an example application, suppose you want to estimate the number of
 * times an element `x` has appeared in a data stream so far.
 * The Count-Min sketch estimate of this frequency is
 *
 *   min_i { counts[i, h_i[x]] }
 *
 * With probability at least `1 - delta`, this estimate is within `eps * N`
 * of the true frequency (i.e., `true frequency <= estimate <= true frequency + eps * N`),
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
 * Monoid for adding [[CMS]] sketches.
 *
 * `eps` and `delta` are parameters that bound the error of each query estimate. For example, errors in
 * answering point queries (e.g., how often has element x appeared in the stream described by the sketch?)
 * are often of the form: "with probability p >= 1 - delta, the estimate is close to the truth by
 * some factor depending on eps."
 *
 * Implicit conversions for commonly used types for `K` such as [[Long]] and [[BigInt]]:
 * {{{
 * import com.twitter.algebird.CMSHasherImplicits._
 * }}}
 *
 * @param eps One-sided error bound on the error of each point query, i.e. frequency estimate.
 * @param delta A bound on the probability that a query estimate does not lie within some small interval
 *        (an interval that depends on `eps`) around the truth.
 * @param seed  A seed to initialize the random number generator used to create the pairwise independent
 *        hash functions.
 * @tparam K The type used to identify the elements to be counted.  For example, if you want to count the occurrence of
 *           user names, you could map each username to a unique numeric ID expressed as a `Long`, and then count the
 *           occurrences of those `Long`s with a CMS of type `K=Long`.  Note that this mapping between the elements of
 *           your problem domain and their identifiers used for counting via CMS should be bijective.
 *           We require [[Ordering]] and [[CMSHasher]] context bounds for `K`, see [[CMSHasherImplicits]] for available
 *           implicits that can be imported.
 *           Which type K should you pick in practice?  For domains that have less than `2^64` unique elements, you'd
 *           typically use [[Long]].  For larger domains you can try [[BigInt]], for example.
 */
class CMSMonoid[K: Ordering: CMSHasher](eps: Double, delta: Double, seed: Int) extends Monoid[CMS[K]] {

  val params = {
    val hashes: Seq[CMSHash[K]] = CMSFunctions.generateHashes(eps, delta, seed)
    CMSParams(hashes, eps, delta)
  }

  val zero: CMS[K] = CMSZero[K](params)

  /**
   * We assume the sketches on the left and right use the same hash functions.
   */
  def plus(left: CMS[K], right: CMS[K]): CMS[K] = left ++ right

  /**
   * Create a sketch out of a single item.
   */
  def create(item: K): CMS[K] = CMSItem[K](item, params)

  /**
   * Create a sketch out of multiple items.
   */
  def create(data: Seq[K]): CMS[K] = data.foldLeft(zero) { case (acc, x) => plus(acc, create(x)) }

}

/**
 * Configuration parameters for [[CMS]].
 *
 * @param hashes Pair-wise independent hashes functions.  We need `N=depth` such functions (`depth` can be derived from
 *               `delta`).
 * @param eps One-sided error bound on the error of each point query, i.e. frequency estimate.
 * @param delta A bound on the probability that a query estimate does not lie within some small interval
 *              (an interval that depends on `eps`) around the truth.
 * @tparam K
 */
case class CMSParams[K](hashes: Seq[CMSHash[K]], eps: Double, delta: Double) {

  require(0 < eps && eps < 1, "eps must lie in (0, 1)")
  require(0 < delta && delta < 1, "delta must lie in (0, 1)")
  require(hashes.size >= CMSFunctions.depth(delta), s"we require at least ${CMSFunctions.depth(delta)} hash functions")

}

/**
 * Helper functions to generate or to translate between various CMS parameters (cf. [[CMSParams]]).
 */
object CMSFunctions {

  /**
   * Translates from `width` to `eps`.
   */
  def eps(width: Int): Double = scala.math.exp(1.0) / width

  /**
   * Translates from `depth` to `delta`.
   */
  def delta(depth: Int): Double = 1.0 / scala.math.exp(depth)

  /**
   * Translates from `delta` to `depth`.
   */
  def depth(delta: Double): Int = scala.math.ceil(scala.math.log(1.0 / delta)).toInt

  /**
   * Translates from `eps` to `width`.
   */
  def width(eps: Double): Int = scala.math.ceil(scala.math.exp(1) / eps).toInt

  /**
   * Generates `N=depth` pair-wise independent hash functions.
   *
   * @param eps One-sided error bound on the error of each point query, i.e. frequency estimate.
   * @param delta Error bound on the probability that a query estimate does NOT lie within some small interval around
   *              the truth.
   * @param seed Seed for the random number generator.
   * @tparam K The type used to identify the elements to be counted.
   * @return The generated hash functions.
   */
  def generateHashes[K: CMSHasher](eps: Double, delta: Double, seed: Int): Seq[CMSHash[K]] = {
    // Typically, we would use d -- aka depth -- pair-wise independent hash functions of the form
    //
    //   h_i(x) = a_i * x + b_i (mod p)
    //
    // But for this particular application, setting b_i does not matter (since all it does is shift the results of a
    // particular hash), so we omit it (by setting b_i to 0) and simply use hash functions of the form
    //
    //   h_i(x) = a_i * x (mod p)
    //
    val r = new scala.util.Random(seed)
    val numHashes = depth(delta)
    val numCounters = width(eps)
    (0 to (numHashes - 1)).map { _ => CMSHash[K](r.nextInt(), 0, numCounters) }
  }

}

/**
 * A trait for CMS implementations that can count elements in a data stream and that can answer point queries (i.e.
 * frequency estimates) for these elements.
 *
 * Known implementations: [[CMS]], [[TopPctCMS]].
 *
 * @tparam K The type used to identify the elements to be counted.
 * @tparam C The type of the actual CMS that implements this trait.
 */
trait CMSCounting[K, C[_]] {

  /**
   * Returns the one-sided error bound on the error of each point query, i.e. frequency estimate.
   */
  def eps: Double

  /**
   * Returns the bound on the probability that a query estimate does NOT lie within some small interval (an interval
   * that depends on `eps`) around the truth.
   */
  def delta: Double

  /**
   * Number of hash functions (also: number of rows in the counting table).  This number is derived from `delta`.
   */
  def depth: Int = CMSFunctions.depth(delta)

  /**
   * Number of counters per hash function (also: number of columns in the counting table).  This number is derived from
   * `eps`.
   */
  def width: Int = CMSFunctions.width(eps)

  /**
   * Returns a new sketch that is the combination of this sketch and the other sketch.
   */
  def ++(other: C[K]): C[K]

  /**
   * Counts the item and returns the result as a new sketch.
   */
  def +(item: K): C[K] = this + (item, 1L)

  /**
   * Counts the item `count` times and returns the result as a new sketch.
   */
  def +(item: K, count: Long): C[K]

  /**
   * Returns an estimate of the total number of times this item has been seen
   * in the stream so far. This estimate is an upper bound.
   *
   * It is always true that `estimatedFrequency >= trueFrequency`.
   * With probability `p >= 1 - delta`, it also holds that
   * `estimatedFrequency <= trueFrequency + eps * totalCount`.
   */
  def frequency(item: K): Approximate[Long]

  /**
   * Returns an estimate of the inner product against another data stream.
   *
   * In other words, let a_i denote the number of times element i has been seen in
   * the data stream summarized by this CMS, and let b_i denote the same for the other CMS.
   * Then this returns an estimate of `<a, b> = \sum a_i b_i`.
   *
   * Note: This can also be viewed as the join size between two relations.
   *
   * It is always true that actualInnerProduct <= estimatedInnerProduct.
   * With probability `p >= 1 - delta`, it also holds that
   * `estimatedInnerProduct <= actualInnerProduct + eps * thisTotalCount * otherTotalCount`.
   */
  def innerProduct(other: C[K]): Approximate[Long]

  /**
   * Total number of elements counted (i.e. seen in the data stream) so far.
   */
  def totalCount: Long

  /**
   * The first frequency moment is the total number of elements in the stream.
   */
  def f1: Long = totalCount

  /**
   * The second frequency moment is `\sum a_i^2`, where `a_i` is the count of the i-th element.
   */
  def f2: Approximate[Long]

}

/**
 * A trait for CMS implementations that can track heavy hitters in a data stream.
 *
 * Known implementations: [[TopPctCMS]].
 *
 * @tparam K The type used to identify the elements to be counted.
 */
trait CMSHeavyHitters[K] {

  /**
   * Finds all heavy hitters, i.e., elements in the stream that appear at least
   * `(heavyHittersPct * totalCount)` times.
   *
   * Every item that appears at least `(heavyHittersPct * totalCount)` times is output,
   * and with probability `p >= 1 - delta`, no item whose count is less than
   * `(heavyHittersPct - eps) * totalCount` is output.
   *
   * Note that the set of heavy hitters contains at most `1 / heavyHittersPct`
   * elements, so keeping track of all elements that appear more than (say) 1% of the
   * time requires tracking at most 100 items.
   */
  def heavyHittersPct: Double

  /**
   * Returns the descendingly sorted list of heavy hitters (e.g. the heaviest hitter is the first element).
   */
  def heavyHitters: Set[K]

}

object CMS {

  def monoid[K: Ordering: CMSHasher](eps: Double, delta: Double, seed: Int): CMSMonoid[K] =
    new CMSMonoid[K](eps, delta, seed)

  def monoid[K: Ordering: CMSHasher](depth: Int, width: Int, seed: Int): CMSMonoid[K] =
    monoid(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed)

  def aggregator[K: Ordering: CMSHasher](eps: Double, delta: Double, seed: Int): CMSAggregator[K] =
    new CMSAggregator[K](monoid(eps, delta, seed))

  def aggregator[K: Ordering: CMSHasher](depth: Int, width: Int, seed: Int): CMSAggregator[K] =
    aggregator(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed)

  /**
   * Returns a fresh, zeroed CMS instance.
   */
  def apply[K: Ordering: CMSHasher](eps: Double, delta: Double, seed: Int): CMS[K] = {
    val params = {
      val hashes: Seq[CMSHash[K]] = CMSFunctions.generateHashes(eps, delta, seed)
      CMSParams(hashes, eps, delta)
    }
    CMSInstance[K](params)
  }

}

/**
 * A Count-Min sketch data structure that allows for counting and frequency estimation of elements in a data stream.
 *
 * Tip: If you also need to track heavy hitters ("Top N" problems), take a look at [[TopPctCMS]].
 *
 * =Usage=
 *
 * This example demonstrates how to count [[Long]] elements with [[CMS]], i.e. `K=Long`.
 *
 * Note that the actual counting is always performed with a [[Long]], regardless of your choice of `K`.  That is,
 * the counting table behind the scenes is backed by [[Long]] values (at least in the current implementation), and thus
 * the returned frequency estimates are always instances of `Approximate[Long]`.
 *
 * @example {{{
 * // Implicits that enabling CMS-hashing of `Long` values.
 * import com.twitter.algebird.CMSHasherImplicits._
 *
 * // Create a monoid for a CMS that can count `Long` elements.
 * val cmsMonoid: CMSMonoid[Long] = {
 *   val eps = 0.001
 *   val delta = 1E-10
 *   val seed = 1
 *   CMS.monoid[Long](eps, delta, seed)
 * }
 *
 * // Create a CMS instance that has counted the element `1L`.
 * val cms: CMS[Long] = cmsMonoid.create(1L)
 *
 * // Estimate frequency of `1L`
 * val estimate: Approximate[Long] = cms.frequency(1L)
 * }}}
 *
 * @tparam K The type used to identify the elements to be counted.
 */
sealed abstract class CMS[K: Ordering](params: CMSParams[K]) extends java.io.Serializable with CMSCounting[K, CMS] {

  override val eps: Double = params.eps

  override val delta: Double = params.delta

  override def f2: Approximate[Long] = innerProduct(this)

}

/**
 * Zero element.  Used for initialization.
 */
case class CMSZero[K: Ordering](params: CMSParams[K]) extends CMS[K](params) {

  override val totalCount: Long = 0L

  override def +(item: K, count: Long): CMS[K] = CMSInstance[K](params) + (item, count)

  override def ++(other: CMS[K]): CMS[K] = other

  override def frequency(item: K): Approximate[Long] = Approximate.exact(0L)

  override def innerProduct(other: CMS[K]): Approximate[Long] = Approximate.exact(0L)

}

/**
 * Used for holding a single element, to avoid repeatedly adding elements from sparse counts tables.
 */
case class CMSItem[K: Ordering](item: K, params: CMSParams[K]) extends CMS[K](params) {

  override val totalCount: Long = 1L

  override def +(x: K, count: Long): CMS[K] = CMSInstance[K](params) + item + (x, count)

  override def ++(other: CMS[K]): CMS[K] = {
    other match {
      case other: CMSZero[_] => this
      case other: CMSItem[K] => CMSInstance[K](params) + item + other.item
      case other: CMSInstance[K] => other + item
    }
  }

  override def frequency(x: K): Approximate[Long] = if (item == x) Approximate.exact(1L) else Approximate.exact(0L)

  override def innerProduct(other: CMS[K]): Approximate[Long] = other.frequency(item)

}

/**
 * The general Count-Min sketch structure, used for holding any number of elements.
 */
case class CMSInstance[K: Ordering](countsTable: CMSInstance.CountsTable[K],
  override val totalCount: Long,
  params: CMSParams[K]) extends CMS[K](params) {

  def ++(other: CMS[K]): CMS[K] = {
    other match {
      case other: CMSZero[_] => this
      case other: CMSItem[K] => this + other.item
      case other: CMSInstance[K] =>
        val newTotalCount = totalCount + other.totalCount
        CMSInstance[K](countsTable ++ other.countsTable, newTotalCount, params)
    }
  }

  private def makeApprox(est: Long): Approximate[Long] = {
    if (est == 0L) Approximate.exact(0L)
    else {
      val lower = math.max(0L, est - (eps * totalCount).toLong)
      Approximate(lower, est, est, 1 - delta)
    }
  }

  def frequency(item: K): Approximate[Long] = {
    val estimates = countsTable.counts.zipWithIndex.map { case (row, i) => row(params.hashes(i)(item)) }
    makeApprox(estimates.min)
  }

  /**
   * Let X be a CMS, and let count_X[j, k] denote the value in X's 2-dimensional count table at row j and column k.
   * Then the Count-Min sketch estimate of the inner product between A and B is the minimum inner product between their
   * rows:
   * estimatedInnerProduct = min_j (\sum_k count_A[j, k] * count_B[j, k])
   */
  def innerProduct(other: CMS[K]): Approximate[Long] = {
    other match {
      case other: CMSInstance[_] =>
        require((other.depth, other.width) == (depth, width), "Tables must have the same dimensions.")

        def innerProductAtDepth(d: Int) = (0 to (width - 1)).map { w =>
          countsTable.getCount(d, w) * other.countsTable.getCount(d, w)
        }.sum

        val est = (0 to (depth - 1)).map { innerProductAtDepth }.min
        Approximate(est - (eps * totalCount * other.totalCount).toLong, est, est, 1 - delta)
      case _ => other.innerProduct(this)
    }
  }

  override def +(item: K, count: Long): CMSInstance[K] = {
    require(count >= 0, "count must be >= 0 (negative counts not implemented")
    if (count != 0L) {
      val newCountsTable =
        (0 to (depth - 1)).foldLeft(countsTable) {
          case (table, row) =>
            val pos = (row, params.hashes(row)(item))
            table + (pos, count)
        }
      CMSInstance[K](newCountsTable, totalCount + count, params)
    } else this
  }

}

object CMSInstance {

  /**
   * Initializes a [[CMSInstance]] with all zeroes, i.e. nothing has been counted yet.
   */
  def apply[K: Ordering](params: CMSParams[K]): CMSInstance[K] = {
    val countsTable = CountsTable[K](CMSFunctions.depth(params.delta), CMSFunctions.width(params.eps))
    CMSInstance[K](countsTable, 0, params)
  }

  /**
   * The 2-dimensional table of counters used in the Count-Min sketch.
   * Each row corresponds to a particular hash function.
   */
  // TODO: implement a dense matrix type, and use it here
  case class CountsTable[K](counts: Vector[Vector[Long]]) {
    require(depth > 0, "Table must have at least 1 row.")
    require(width > 0, "Table must have at least 1 column.")

    def depth: Int = counts.size

    def width: Int = counts(0).size

    def getCount(pos: (Int, Int)): Long = {
      val (row, col) = pos
      require(row < depth && col < width, "Position must be within the bounds of this table.")
      counts(row)(col)
    }

    /**
     * Updates the count of a single cell in the table.
     */
    def +(pos: (Int, Int), count: Long): CountsTable[K] = {
      val (row, col) = pos
      val currCount = getCount(pos)
      val newCounts = counts.updated(row, counts(row).updated(col, currCount + count))
      CountsTable[K](newCounts)
    }

    /**
     * Adds another counts table to this one, through element-wise addition.
     */
    def ++(other: CountsTable[K]): CountsTable[K] = {
      require((depth, width) == (other.depth, other.width), "Tables must have the same dimensions.")
      val iil: IndexedSeq[IndexedSeq[Long]] = Monoid.plus[IndexedSeq[IndexedSeq[Long]]](counts, other.counts)
      def toVector[V](is: IndexedSeq[V]): Vector[V] = is match {
        case v: Vector[_] => v
        case _ => Vector(is: _*)
      }
      CountsTable[K](toVector(iil.map { toVector }))
    }
  }

  object CountsTable {
    /**
     * Creates a new [[CountsTable]] with counts initialized to all zeroes.
     */
    def apply[K: Ordering](depth: Int, width: Int): CountsTable[K] =
      CountsTable[K](Vector.fill[Long](depth, width)(0L))
  }

}

/**
 * Monoid for adding [[TopPctCMS]] sketches.
 *
 * Implicit conversions for commonly used types for `K` such as [[Long]] and [[BigInt]]:
 * {{{
 * import com.twitter.algebird.CMSHasherImplicits._
 * }}}
 *
 * @param cms A [[CMS]] instance, which is used for the counting and the frequency estimation performed by this class.
 * @param heavyHittersPct A threshold for finding heavy hitters, i.e., elements that appear at least
 *                  (heavyHittersPct * totalCount) times in the stream.
 * @tparam K The type used to identify the elements to be counted.  For example, if you want to count the occurrence of
 *           user names, you could map each username to a unique numeric ID expressed as a `Long`, and then count the
 *           occurrences of those `Long`s with a CMS of type `K=Long`.  Note that this mapping between the elements of
 *           your problem domain and their identifiers used for counting via CMS should be bijective.
 *           We require [[Ordering]] and [[CMSHasher]] context bounds for `K`, see [[CMSHasherImplicits]] for available
 *           implicits that can be imported.
 *           Which type K should you pick in practice?  For domains that have less than `2^64` unique elements, you'd
 *           typically use [[Long]].  For larger domains you can try [[BigInt]], for example.
 */
class TopPctCMSMonoid[K: Ordering](cms: CMS[K], heavyHittersPct: Double = 0.01) extends Monoid[TopPctCMS[K]] {

  val params: TopPctCMSParams = TopPctCMSParams(heavyHittersPct)

  val zero: TopPctCMS[K] = TopPctCMSZero[K](cms, params)

  /**
   * We assume the sketches on the left and right use the same hash functions.
   */
  def plus(left: TopPctCMS[K], right: TopPctCMS[K]): TopPctCMS[K] = left ++ right

  /**
   * Create a sketch out of a single item.
   */
  def create(item: K): TopPctCMS[K] = TopPctCMSItem[K](item, cms, params)

  /**
   * Create a sketch out of multiple items.
   */
  def create(data: Seq[K]): TopPctCMS[K] = {
    data.foldLeft(zero) { case (acc, x) => plus(acc, create(x)) }
  }

}

case class TopPctCMSParams(heavyHittersPct: Double) {

  require(0 < heavyHittersPct && heavyHittersPct < 1, "heavyHittersPct must lie in (0, 1)")

}

object TopPctCMS {

  def monoid[K: Ordering: CMSHasher](eps: Double,
    delta: Double,
    seed: Int,
    heavyHittersPct: Double): TopPctCMSMonoid[K] =
    new TopPctCMSMonoid[K](CMS(eps, delta, seed), heavyHittersPct)

  def monoid[K: Ordering: CMSHasher](depth: Int,
    width: Int,
    seed: Int,
    heavyHittersPct: Double): TopPctCMSMonoid[K] =
    monoid(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, heavyHittersPct)

  def aggregator[K: Ordering: CMSHasher](eps: Double,
    delta: Double,
    seed: Int,
    heavyHittersPct: Double): TopPctCMSAggregator[K] =
    new TopPctCMSAggregator[K](monoid(eps, delta, seed, heavyHittersPct))

  def aggregator[K: Ordering: CMSHasher](depth: Int,
    width: Int,
    seed: Int,
    heavyHittersPct: Double): TopPctCMSAggregator[K] =
    aggregator(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, heavyHittersPct)

}

/**
 * A Count-Min sketch data structure that allows for (a) counting and frequency estimation of elements in a data stream
 * and (b) tracking the heavy hitters among these elements.
 *
 * Tip: If you do not need to track heavy hitters, take a look at [[CMS]], which is more efficient in this case.
 *
 * =Usage=
 *
 * This example demonstrates how to count `Long` elements with [[TopPctCMS]], i.e. `K=Long`.
 *
 * Note that the actual counting is always performed with a [[Long]], regardless of your choice of `K`.  That is,
 * the counting table behind the scenes is backed by [[Long]] values (at least in the current implementation), and thus
 * the returned frequency estimates are always instances of `Approximate[Long]`.
 *
 * @example {{{
 * // Implicits that enabling CMS-hashing of `Long` values.
 * import com.twitter.algebird.CMSHasherImplicits._
 *
 * // Create a monoid for a CMS that can count `Long` elements.
 * val topPctCMSMonoid: TopPctCMSMonoid[Long] = {
 *   val eps = 0.001
 *   val delta = 1E-10
 *   val seed = 1
 *   val heavyHittersPct = 0.1
 *   TopPctCMS.monoid[Long](eps, delta, seed, heavyHittersPct)
 * }
 *
 * // Create a TopPctCMS instance that has counted the element `1L`.
 * val topPctCMS: TopPctCMS[Long] = topPctCMSMonoid.create(1L)
 *
 * // Estimate frequency of `1L`
 * val estimate: Approximate[Long] = topPctCMS.frequency(1L)
 *
 * // What are the heavy hitters so far?
 * val heavyHitters: Set[Long] = topPctCMS.heavyHitters
 * }}}
 *
 * @tparam K The type used to identify the elements to be counted.
 */
sealed abstract class TopPctCMS[K: Ordering](val cms: CMS[K], params: TopPctCMSParams)
  extends java.io.Serializable with CMSCounting[K, TopPctCMS] with CMSHeavyHitters[K] {

  override val eps: Double = cms.eps

  override val delta: Double = cms.delta

  override val totalCount: Long = cms.totalCount

  override val heavyHittersPct = params.heavyHittersPct

  override def frequency(item: K): Approximate[Long] = cms.frequency(item)

  override def innerProduct(other: TopPctCMS[K]): Approximate[Long] = cms.innerProduct(other.cms)

  def f2: Approximate[Long] = innerProduct(this)

}

/**
 * Zero element.  Used for initialization.
 */
case class TopPctCMSZero[K: Ordering](override val cms: CMS[K],
  params: TopPctCMSParams) extends TopPctCMS[K](cms, params) {

  override val heavyHitters: Set[K] = Set.empty[K]

  override def +(item: K, count: Long): TopPctCMS[K] = TopPctCMSInstance(cms, params) + (item, count)

  override def ++(other: TopPctCMS[K]): TopPctCMS[K] = other

}

/**
 * Used for holding a single element, to avoid repeatedly adding elements from sparse counts tables.
 */
case class TopPctCMSItem[K: Ordering](item: K,
  override val cms: CMS[K],
  params: TopPctCMSParams) extends TopPctCMS[K](cms, params) {

  override val heavyHitters: Set[K] = Set(item)

  override def +(x: K, count: Long): TopPctCMS[K] = TopPctCMSInstance(cms, params) + item + (x, count)

  override def ++(other: TopPctCMS[K]): TopPctCMS[K] = {
    other match {
      case other: TopPctCMSZero[_] => this
      case other: TopPctCMSItem[K] => TopPctCMSInstance[K](cms, params) + item + other.item
      case other: TopPctCMSInstance[K] => other + item
    }
  }

}

object TopPctCMSInstance {

  def apply[K: Ordering](cms: CMS[K], params: TopPctCMSParams): TopPctCMSInstance[K] = {
    implicit val heavyHitterOrdering = HeavyHitter.ordering[K]
    TopPctCMSInstance[K](cms, HeavyHitters[K](SortedSet[HeavyHitter[K]]()), params)
  }

}

case class TopPctCMSInstance[K: Ordering](override val cms: CMS[K],
  hhs: HeavyHitters[K],
  params: TopPctCMSParams)
  extends TopPctCMS[K](cms, params) {

  override def heavyHitters: Set[K] = hhs.items

  override def +(item: K, count: Long): TopPctCMSInstance[K] = {
    require(count >= 0, "count must be >= 0 (negative counts not implemented")
    if (count != 0L) {
      val newHhs = updateHeavyHitters(item, count)
      val newCMS = cms + (item, count)
      TopPctCMSInstance[K](newCMS, newHhs, params)
    } else this
  }

  /**
   * Updates the data structure of heavy hitters when a new item (with associated count) enters the stream.
   */
  private def updateHeavyHitters(item: K, count: Long): HeavyHitters[K] = {
    val oldItemCount = frequency(item).estimate
    val newItemCount = oldItemCount + count
    val newTotalCount = totalCount + count

    // If the new item is a heavy hitter, add it, and remove any previous instances.
    val newHhs =
      if (newItemCount >= params.heavyHittersPct * newTotalCount) {
        hhs - HeavyHitter[K](item, oldItemCount) + HeavyHitter[K](item, newItemCount)
      } else hhs

    // Remove any items below the new heavy hitter threshold.
    newHhs.dropCountsBelow(params.heavyHittersPct * newTotalCount)
  }

  override def ++(other: TopPctCMS[K]): TopPctCMS[K] = {
    other match {
      case other: TopPctCMSZero[_] => this
      case other: TopPctCMSItem[K] => this + other.item
      case other: TopPctCMSInstance[K] =>
        val newTotalCount = totalCount + other.totalCount
        val newHhs = (hhs ++ other.hhs).dropCountsBelow(params.heavyHittersPct * newTotalCount)
        TopPctCMSInstance(cms ++ other.cms, newHhs, params)
    }
  }

}

/**
 * Containers for holding heavy hitter items and their associated counts.
 */
case class HeavyHitters[K: Ordering](hhs: SortedSet[HeavyHitter[K]]) {

  def -(hh: HeavyHitter[K]): HeavyHitters[K] = HeavyHitters[K](hhs - hh)

  def +(hh: HeavyHitter[K]): HeavyHitters[K] = HeavyHitters[K](hhs + hh)

  def ++(other: HeavyHitters[K]): HeavyHitters[K] = HeavyHitters[K](hhs ++ other.hhs)

  def items: Set[K] = hhs.map { _.item }

  def dropCountsBelow(minCount: Double): HeavyHitters[K] = HeavyHitters[K](hhs.dropWhile { _.count < minCount })
}

case class HeavyHitter[K: Ordering](item: K, count: Long)

object HeavyHitter {

  def ordering[K: Ordering]: Ordering[HeavyHitter[K]] = Ordering.by { hh: HeavyHitter[K] => (hh.count, hh.item) }

}

/**
 * An Aggregator for [[CMS]].  Can be created using [[CMS.aggregator]].
 */
case class CMSAggregator[K](cmsMonoid: CMSMonoid[K]) extends MonoidAggregator[K, CMS[K], CMS[K]] {

  val monoid = cmsMonoid

  def prepare(value: K): CMS[K] = monoid.create(value)

  def present(cms: CMS[K]): CMS[K] = cms

}

/**
 * An Aggregator for [[TopPctCMS]].  Can be created using [[TopPctCMS.aggregator]].
 */
case class TopPctCMSAggregator[K](cmsMonoid: TopPctCMSMonoid[K])
  extends MonoidAggregator[K, TopPctCMS[K], TopPctCMS[K]] {

  val monoid = cmsMonoid

  def prepare(value: K): TopPctCMS[K] = monoid.create(value)

  def present(cms: TopPctCMS[K]): TopPctCMS[K] = cms

}

/**
 * The Count-Min sketch uses `d` (aka `depth`) pair-wise independent hash functions drawn from a universal hashing
 * family of the form:
 *
 * `h(x) = [a * x + b (mod p)] (mod m)`
 */
trait CMSHasher[K] {

  val PRIME_MODULUS = (1L << 31) - 1

  /**
   * Returns `a * x + b (mod p) (mod width)`.
   */
  def hash(a: Int, b: Int, width: Int)(x: K): Int

}

case class CMSHash[K: CMSHasher](a: Int, b: Int, width: Int) {

  /**
   * Returns `a * x + b (mod p) (mod width)`.
   */
  def apply(x: K): Int = implicitly[CMSHasher[K]].hash(a, b, width)(x)

}

/**
 * Implicits that enable CMS-hashing for common data types such as [[Long]] and [[BigInt]].
 */
object CMSHasherImplicits {

  implicit object CMSHasherLong extends CMSHasher[Long] {

    def hash(a: Int, b: Int, width: Int)(x: Long) = {
      val unModded: Long = (x * a) + b
      // Apparently a super fast way of computing x mod 2^p-1
      // See page 149 of http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
      // after Proposition 7.
      val modded: Long = (unModded + (unModded >> 32)) & PRIME_MODULUS
      // Modulo-ing integers is apparently twice as fast as modulo-ing Longs.
      modded.toInt % width
    }

  }

  implicit object CMSHasherShort extends CMSHasher[Short] {

    def hash(a: Int, b: Int, width: Int)(x: Short) = CMSHasherInt.hash(a, b, width)(x)

  }

  implicit object CMSHasherInt extends CMSHasher[Int] {

    def hash(a: Int, b: Int, width: Int)(x: Int) = {
      val unModded: Int = (x * a) + b
      val modded: Long = (unModded + (unModded >> 32)) & PRIME_MODULUS
      val h = modded.toInt % width
      assert(h >= 0, "hash must not be negative")
      h
    }

  }

  implicit object CMSHasherBigInt extends CMSHasher[BigInt] {

    def hash(a: Int, b: Int, width: Int)(x: BigInt) = {
      val unModded: BigInt = (x * a) + b
      val modded: BigInt = (unModded + (unModded >> 32)) & PRIME_MODULUS
      val h = modded.toInt % width
      assert(h >= 0, "hash must not be negative")
      h
    }

  }

}