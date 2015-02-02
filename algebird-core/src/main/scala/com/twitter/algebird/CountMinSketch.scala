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
import scala.reflect.ClassTag

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
 * =Usage=
 *
 * `eps` and `delta` are parameters that bound the error of each query estimate. For example, errors in
 * answering point queries (e.g., how often has element x appeared in the stream described by the sketch?)
 * are often of the form: "with probability p >= 1 - delta, the estimate is close to the truth by
 * some factor depending on eps."
 *
 * The type `K` is the type of items you want to count.  You must provide an implicit `CMSHasher[K]` for `K`,  and
 * Algebird ships with several such implicits for commonly used types such as [[Long]] and [[BigInt]]:
 *
 * {{{
 * import com.twitter.algebird.CMSHasherImplicits._
 * }}}
 *
 * If your type `K` is not supported out of the box, you have two options: 1) You provide a "translation" function to
 * convert items of your (unsupported) type `K` to a supported type such as `Array[Byte]`, and then use the `contramap`
 * function of [[CMSHasher]] to create the required `CMSHasher[K]` for your type (see the documentation of [[CMSHasher]]
 * for an example); 2) You implement a `CMSHasher[K]` from scratch, using the existing CMSHasher implementations as a
 * starting point.
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
 *           We require a [[CMSHasher]] context bound for `K`, see [[CMSHasherImplicits]] for available implicits that
 *           can be imported.
 *           Which type K should you pick in practice?  For domains that have less than `2^64` unique elements, you'd
 *           typically use [[Long]].  For larger domains you can try [[BigInt]], for example.  Other possibilities
 *           include Spire's `SafeLong` and `Numerical` data types (https://github.com/non/spire), though Algebird does
 *           not include the required implicits for CMS-hashing (cf. [[CMSHasherImplicits]].
 */
class CMSMonoid[K: CMSHasher](eps: Double, delta: Double, seed: Int) extends Monoid[CMS[K]] {

  val params = {
    val hashes: Seq[CMSHash[K]] = CMSFunctions.generateHashes(eps, delta, seed)
    CMSParams(hashes, eps, delta)
  }

  val zero: CMS[K] = CMSZero[K](params)

  /**
   * Combines the two sketches.
   *
   * The sketches must use the same hash functions.
   */
  def plus(left: CMS[K], right: CMS[K]): CMS[K] = {
    require(left.params.hashes == right.params.hashes, "The sketches must use the same hash functions.")
    left ++ right
  }

  /**
   * Creates a sketch out of a single item.
   */
  def create(item: K): CMS[K] = CMSItem[K](item, params)

  /**
   * Creates a sketch out of multiple items.
   */
  def create(data: Seq[K]): CMS[K] = data.foldLeft(zero) { case (acc, x) => plus(acc, create(x)) }

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
 * Configuration parameters for [[CMS]].
 *
 * @param hashes Pair-wise independent hashes functions.  We need `N=depth` such functions (`depth` can be derived from
 *               `delta`).
 * @param eps One-sided error bound on the error of each point query, i.e. frequency estimate.
 * @param delta A bound on the probability that a query estimate does not lie within some small interval
 *              (an interval that depends on `eps`) around the truth.
 * @tparam K The type used to identify the elements to be counted.
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
  @throws[IllegalArgumentException]("if depth is too large, causing precision errors when computing delta")
  def delta(depth: Int): Double = {
    val i = 1.0 / scala.math.exp(depth)
    require(i != 0.0, s"depth must be smaller as it causes precision errors when computing delta ($depth led to an invalid delta of 0.0)")
    i
  }

  /**
   * Translates from `delta` to `depth`.
   */
  @throws[IllegalArgumentException]("if delta is is not in (0, 1)")
  def depth(delta: Double): Int = {
    require(0 < delta && delta < 1, "delta must lie in (0, 1)")
    scala.math.ceil(scala.math.log(1.0 / delta)).toInt
  }

  /**
   * Translates from `eps` to `width`.
   */
  def width(eps: Double): Int = scala.math.ceil(truncatePrecisionError(scala.math.exp(1) / eps)).toInt

  // Eliminates precision errors such as the following:
  //
  //   scala> val width = 39
  //   scala> scala.math.exp(1) / CMSFunctions.eps(width)
  //   res171: Double = 39.00000000000001   <<< should be 39.0
  //
  // Because of the actual types on which CMSFunctions operates (i.e. Int and Double), the maximum number of decimal
  // places should be 6.
  private def truncatePrecisionError(i: Double, decimalPlaces: Int = 6) =
    BigDecimal(i).setScale(decimalPlaces, BigDecimal.RoundingMode.HALF_UP).toDouble

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
 * Known implementations: [[CMS]], [[TopCMS]].
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
 * It is up to the implementation how the semantics of tracking heavy hitters are defined.  For instance, one
 * implementation could track the "top %" heavy hitters whereas another implementation could track the "top N" heavy
 * hitters.
 *
 * Known implementations: [[TopCMS]].
 *
 * @tparam K The type used to identify the elements to be counted.
 */
trait CMSHeavyHitters[K] {

  /**
   * The pluggable logic of how heavy hitters are being tracked.
   */
  def heavyHittersLogic: HeavyHittersLogic[K]

  /**
   * Returns the set of heavy hitters.
   */
  def heavyHitters: Set[K]

}

object CMS {

  def monoid[K: CMSHasher](eps: Double, delta: Double, seed: Int): CMSMonoid[K] =
    new CMSMonoid[K](eps, delta, seed)

  def monoid[K: CMSHasher](depth: Int, width: Int, seed: Int): CMSMonoid[K] =
    monoid(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed)

  def aggregator[K: CMSHasher](eps: Double, delta: Double, seed: Int): CMSAggregator[K] =
    new CMSAggregator[K](monoid(eps, delta, seed))

  def aggregator[K: CMSHasher](depth: Int, width: Int, seed: Int): CMSAggregator[K] =
    aggregator(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed)

  /**
   * Returns a fresh, zeroed CMS instance.
   */
  def apply[K: CMSHasher](eps: Double, delta: Double, seed: Int): CMS[K] = {
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
 * Tip: If you also need to track heavy hitters ("Top N" problems), take a look at [[TopCMS]].
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
 * // Creates a monoid for a CMS that can count `Long` elements.
 * val cmsMonoid: CMSMonoid[Long] = {
 *   val eps = 0.001
 *   val delta = 1E-10
 *   val seed = 1
 *   CMS.monoid[Long](eps, delta, seed)
 * }
 *
 * // Creates a CMS instance that has counted the element `1L`.
 * val cms: CMS[Long] = cmsMonoid.create(1L)
 *
 * // Estimates the frequency of `1L`
 * val estimate: Approximate[Long] = cms.frequency(1L)
 * }}}
 *
 * @tparam K The type used to identify the elements to be counted.
 */
sealed abstract class CMS[K](val params: CMSParams[K]) extends java.io.Serializable with CMSCounting[K, CMS] {

  override val eps: Double = params.eps

  override val delta: Double = params.delta

  override def f2: Approximate[Long] = innerProduct(this)

}

/**
 * Zero element.  Used for initialization.
 */
case class CMSZero[K](override val params: CMSParams[K]) extends CMS[K](params) {

  override val totalCount: Long = 0L

  override def +(item: K, count: Long): CMS[K] = CMSInstance[K](params) + (item, count)

  override def ++(other: CMS[K]): CMS[K] = other

  override def frequency(item: K): Approximate[Long] = Approximate.exact(0L)

  override def innerProduct(other: CMS[K]): Approximate[Long] = Approximate.exact(0L)

}

/**
 * Used for holding a single element, to avoid repeatedly adding elements from sparse counts tables.
 */
case class CMSItem[K](item: K, override val params: CMSParams[K]) extends CMS[K](params) {

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
case class CMSInstance[K](countsTable: CMSInstance.CountsTable[K],
  override val totalCount: Long,
  override val params: CMSParams[K]) extends CMS[K](params) {

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

  def frequency(item: K): Approximate[Long] =
    makeApprox(countsTable.counts.iterator.zip(params.hashes.iterator).map { case (row, hash) => row(hash(item)) }.min)

  /**
   * Let X be a CMS, and let count_X[j, k] denote the value in X's 2-dimensional count table at row j and column k.
   * Then the Count-Min sketch estimate of the inner product between A and B is the minimum inner product between their
   * rows:
   * estimatedInnerProduct = min_j (\sum_k count_A[j, k] * count_B[j, k]|)
   */
  def innerProduct(other: CMS[K]): Approximate[Long] = {
    other match {
      case other: CMSInstance[_] =>
        require((other.depth, other.width) == (depth, width), "Tables must have the same dimensions.")

        def innerProductAtDepth(d: Int) = (0 to (width - 1)).iterator.map { w =>
          countsTable.getCount(d, w) * other.countsTable.getCount(d, w)
        }.sum

        val est = (0 to (depth - 1)).iterator.map { innerProductAtDepth }.min
        val minimum = math.max(est - (eps * totalCount * other.totalCount).toLong, 0)
        Approximate(minimum, est, est, 1 - delta)
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
  def apply[K](params: CMSParams[K]): CMSInstance[K] = {
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
        case v: Vector[_] => v.asInstanceOf[Vector[V]]
        case _ => Vector(is: _*)
      }
      CountsTable[K](toVector(iil.map { toVector }))
    }
  }

  object CountsTable {

    /**
     * Creates a new [[CountsTable]] with counts initialized to all zeroes.
     */
    def apply[K](depth: Int, width: Int): CountsTable[K] =
      CountsTable[K](Vector.fill[Long](depth, width)(0L))

  }

}

case class TopCMSParams[K](logic: HeavyHittersLogic[K])

/**
 * A Count-Min sketch data structure that allows for (a) counting and frequency estimation of elements in a data stream
 * and (b) tracking the heavy hitters among these elements.
 *
 * The logic of how heavy hitters are computed is pluggable, see [[HeavyHittersLogic]].
 *
 * Tip: If you do not need to track heavy hitters, take a look at [[CMS]], which is more efficient in this case.
 *
 * =Usage=
 *
 * This example demonstrates how to count `Long` elements with [[TopCMS]], i.e. `K=Long`.
 *
 * Note that the actual counting is always performed with a [[Long]], regardless of your choice of `K`.  That is,
 * the counting table behind the scenes is backed by [[Long]] values (at least in the current implementation), and thus
 * the returned frequency estimates are always instances of `Approximate[Long]`.
 *
 * @example {{{
 * // Implicits that enabling CMS-hashing of `Long` values.
 * import com.twitter.algebird.CMSHasherImplicits._
 *
 * // Creates a monoid for a CMS that can count `Long` elements.
 * val topPctCMSMonoid: TopPctCMSMonoid[Long] = {
 *   val eps = 0.001
 *   val delta = 1E-10
 *   val seed = 1
 *   val heavyHittersPct = 0.1
 *   TopPctCMS.monoid[Long](eps, delta, seed, heavyHittersPct)
 * }
 *
 * // Creates a TopCMS instance that has counted the element `1L`.
 * val topCMS: TopCMS[Long] = topPctCMSMonoid.create(1L)
 *
 * // Estimates the frequency of `1L`
 * val estimate: Approximate[Long] = topCMS.frequency(1L)
 *
 * // What are the heavy hitters so far?
 * val heavyHitters: Set[Long] = topCMS.heavyHitters
 * }}}
 *
 * @tparam K The type used to identify the elements to be counted.
 */
sealed abstract class TopCMS[K](val cms: CMS[K], params: TopCMSParams[K])
  extends java.io.Serializable with CMSCounting[K, TopCMS] with CMSHeavyHitters[K] {

  override val eps: Double = cms.eps

  override val delta: Double = cms.delta

  override val totalCount: Long = cms.totalCount

  override def frequency(item: K): Approximate[Long] = cms.frequency(item)

  override def innerProduct(other: TopCMS[K]): Approximate[Long] = cms.innerProduct(other.cms)

  def f2: Approximate[Long] = innerProduct(this)

  /**
   * The pluggable logic with which heavy hitters are being tracked.
   */
  override def heavyHittersLogic: HeavyHittersLogic[K] = params.logic

}

/**
 * Zero element.  Used for initialization.
 */
case class TopCMSZero[K: ClassTag](override val cms: CMS[K], params: TopCMSParams[K])
  extends TopCMS[K](cms, params) {

  override val heavyHitters: Set[K] = Set.empty[K]

  override def +(item: K, count: Long): TopCMS[K] = TopCMSInstance(cms, params) + (item, count)

  override def ++(other: TopCMS[K]): TopCMS[K] = other

}

/**
 * Used for holding a single element, to avoid repeatedly adding elements from sparse counts tables.
 */
case class TopCMSItem[K: ClassTag](item: K, override val cms: CMS[K], params: TopCMSParams[K])
  extends TopCMS[K](cms, params) {

  override val heavyHitters: Set[K] = Set(item)

  override def +(x: K, count: Long): TopCMS[K] = toCMSInstance + (x, count)

  override def ++(other: TopCMS[K]): TopCMS[K] = other match {
    case other: TopCMSZero[_] => this
    case other: TopCMSItem[K] => toCMSInstance + other.item
    case other: TopCMSInstance[K] => other + item
  }

  private def toCMSInstance: TopCMSInstance[K] = {
    val hhs = HeavyHitters.from(HeavyHitter(item, 1L))
    TopCMSInstance(cms, hhs, params)
  }

}

object TopCMSInstance {

  def apply[K: ClassTag](cms: CMS[K], params: TopCMSParams[K]): TopCMSInstance[K] = {
    TopCMSInstance[K](cms, HeavyHitters.empty[K], params)
  }

}

case class TopCMSInstance[K: ClassTag](override val cms: CMS[K], hhs: HeavyHitters[K], params: TopCMSParams[K])
  extends TopCMS[K](cms, params) {

  override def heavyHitters: Set[K] = hhs.items

  override def +(item: K, count: Long): TopCMSInstance[K] = {
    require(count >= 0, "count must be >= 0 (negative counts not implemented")
    if (count != 0L) {
      val newCms = cms + (item, count)
      val newHhs = heavyHittersLogic.updateHeavyHitters(cms, newCms)(hhs, item, count)
      TopCMSInstance[K](newCms, newHhs, params)
    } else this
  }

  override def ++(other: TopCMS[K]): TopCMS[K] = other match {
    case other: TopCMSZero[_] => this
    case other: TopCMSItem[K] => this + other.item
    case other: TopCMSInstance[K] =>
      val newCms = cms ++ other.cms
      val newHhs = heavyHittersLogic.updateHeavyHitters(newCms)(hhs, other.hhs)
      TopCMSInstance(newCms, newHhs, params)
  }

}

/**
 * Controls how a CMS that implements [[CMSHeavyHitters]] tracks heavy hitters.
 */
abstract class HeavyHittersLogic[K: ClassTag] extends java.io.Serializable {

  implicit val orderingHeavyHitterK = HeavyHitter.ordering[K]

  def updateHeavyHitters(oldCms: CMS[K], newCms: CMS[K])(hhs: HeavyHitters[K], item: K, count: Long): HeavyHitters[K] = {
    val oldItemCount = oldCms.frequency(item).estimate
    val oldHh = HeavyHitter[K](item, oldItemCount)
    val newItemCount = oldItemCount + count
    val newHh = HeavyHitter[K](item, newItemCount)
    purgeHeavyHitters(newCms)(hhs - oldHh + newHh)
  }

  def updateHeavyHitters(cms: CMS[K])(left: HeavyHitters[K], right: HeavyHitters[K]) = {
    val candidates = (left.items ++ right.items).map { case i => HeavyHitter[K](i, cms.frequency(i).estimate) }
    val newHhs = HeavyHitters.from(candidates)
    purgeHeavyHitters(cms)(newHhs)
  }

  def purgeHeavyHitters(cms: CMS[K])(hhs: HeavyHitters[K]): HeavyHitters[K]

}

/**
 * Finds all heavy hitters, i.e., elements in the stream that appear at least `(heavyHittersPct * totalCount)` times.
 *
 * Every item that appears at least `(heavyHittersPct * totalCount)` times is output, and with probability
 * `p >= 1 - delta`, no item whose count is less than `(heavyHittersPct - eps) * totalCount` is output.
 *
 * This also means that this parameter is an upper bound on the number of heavy hitters that will be tracked: the set
 * of heavy hitters contains at most `1 / heavyHittersPct` elements.  For example, if `heavyHittersPct=0.01` (or
 * 0.25), then at most `1 / 0.01 = 100` items (or `1 / 0.25 = 4` items) will be tracked/returned as heavy hitters.
 * This parameter can thus control the memory footprint required for tracking heavy hitters.
 */
case class TopPctLogic[K: ClassTag](heavyHittersPct: Double) extends HeavyHittersLogic[K] {

  require(0 < heavyHittersPct && heavyHittersPct < 1, "heavyHittersPct must lie in (0, 1)")

  override def purgeHeavyHitters(cms: CMS[K])(hitters: HeavyHitters[K]): HeavyHitters[K] = {
    val minCount = heavyHittersPct * cms.totalCount
    HeavyHitters[K](hitters.hhs.dropWhile { _.count < minCount })
  }

}

/**
 * Tracks the top N heavy hitters, where `N` is defined by `heavyHittersN`.
 *
 * '''Warning:''' top-N computations are not associative.  The effect is that a top-N CMS has an ordering bias (with
 * regard to heavy hitters) when merging instances.  This means merging heavy hitters across CMS instances may lead to
 * incorrect, biased results:  the outcome is biased by the order in which CMS instances / heavy hitters are being
 * merged, with the rule of thumb being that the earlier a set of heavy hitters is being merged, the more likely is
 * the end result biased towards these heavy hitters.
 *
 * @see Discussion in [[https://github.com/twitter/algebird/issues/353 Algebird issue 353]]
 */
case class TopNLogic[K: ClassTag](heavyHittersN: Int) extends HeavyHittersLogic[K] {

  require(heavyHittersN > 0, "heavyHittersN must be > 0")

  override def purgeHeavyHitters(cms: CMS[K])(hitters: HeavyHitters[K]): HeavyHitters[K] =
    HeavyHitters[K](hitters.hhs.takeRight(heavyHittersN))

}

/**
 * Containers for holding heavy hitter items and their associated counts.
 *
 * =Implementation details=
 *
 * We use a [[SortedSet]] to minimize the time complexity of a CMS `count()` operation.  Heavy hitters need to be
 * updated (and notably purged) on every count operation, so we benefit from a sorted representation of heavy hitters
 * instead of (say) using a [[Set]] and re-sorting the heavy hitters every time from scratch.
 */
case class HeavyHitters[K](hhs: SortedSet[HeavyHitter[K]]) extends java.io.Serializable {

  def -(hh: HeavyHitter[K]): HeavyHitters[K] = HeavyHitters[K](hhs - hh)

  def +(hh: HeavyHitter[K]): HeavyHitters[K] = HeavyHitters[K](hhs + hh)

  def ++(other: HeavyHitters[K]): HeavyHitters[K] = HeavyHitters[K](hhs ++ other.hhs)

  def items: Set[K] = hhs.map { _.item }

}

object HeavyHitters {

  def empty[K: ClassTag]: HeavyHitters[K] = HeavyHitters(emptyHhs)

  private def emptyHhs[K: ClassTag]: SortedSet[HeavyHitter[K]] = SortedSet[HeavyHitter[K]]()(HeavyHitter.ordering[K])

  def from[K: ClassTag](hhs: Set[HeavyHitter[K]]): HeavyHitters[K] = hhs.foldLeft(empty[K])(_ + _)

  def from[K: ClassTag](hh: HeavyHitter[K]): HeavyHitters[K] = HeavyHitters(emptyHhs + hh)

}

case class HeavyHitter[K](item: K, count: Long) extends java.io.Serializable

object HeavyHitter {

  import scala.reflect._

  // Note that we can't define the ordering on `count` only.  We use a set to track heavy hitters, where equality means
  // two elements are identical.  If we were to use `count` only, we would label different items in heavy hitters as
  // identical simply because they have the same count (and such false duplicates may happen easily in practice).  For
  // this reason we need to add another dimension to the ordering to prevent such false duplicates.  For this second
  // dimension we use an item's default Java hash code because we do not want to introduce an `Ordering` constraint.
  def ordering[K: ClassTag]: Ordering[HeavyHitter[K]] = {
    val ct = implicitly[ClassTag[K]]
    ct match {
      // We must special-case Array instances because here, unfortunately, `Array(1).hashCode != Array(1).hashCode`.
      case _ if ct.runtimeClass.isArray =>
        Ordering.by[HeavyHitter[K], (Long, Int)] { hh => (hh.count, hh.item.asInstanceOf[Array[_]].toSeq.hashCode()) }
      case _ =>
        Ordering.by[HeavyHitter[K], (Long, Int)] { hh => (hh.count, hh.item.hashCode()) }
    }
  }

}

/**
 * Monoid for Top-% based [[TopCMS]] sketches.
 *
 * =Usage=
 *
 * The type `K` is the type of items you want to count.  You must provide an implicit `CMSHasher[K]` for `K`,  and
 * Algebird ships with several such implicits for commonly used types such as [[Long]] and [[BigInt]]:
 *
 * {{{
 * import com.twitter.algebird.CMSHasherImplicits._
 * }}}
 *
 * If your type `K` is not supported out of the box, you have two options: 1) You provide a "translation" function to
 * convert items of your (unsupported) type `K` to a supported type such as `Array[Byte]`, and then use the `contramap`
 * function of [[CMSHasher]] to create the required `CMSHasher[K]` for your type (see the documentation of [[CMSHasher]]
 * for an example); 2) You implement a `CMSHasher[K]` from scratch, using the existing CMSHasher implementations as a
 * starting point.
 *
 * @param cms A [[CMS]] instance, which is used for the counting and the frequency estimation performed by this class.
 * @param heavyHittersPct A threshold for finding heavy hitters, i.e., elements that appear at least
 *                  (heavyHittersPct * totalCount) times in the stream.
 * @tparam K The type used to identify the elements to be counted.  For example, if you want to count the occurrence of
 *           user names, you could map each username to a unique numeric ID expressed as a `Long`, and then count the
 *           occurrences of those `Long`s with a CMS of type `K=Long`.  Note that this mapping between the elements of
 *           your problem domain and their identifiers used for counting via CMS should be bijective.
 *           We require a [[CMSHasher]] context bound for `K`, see [[CMSHasherImplicits]] for available implicits that
 *           can be imported.
 *           Which type K should you pick in practice?  For domains that have less than `2^64` unique elements, you'd
 *           typically use [[Long]].  For larger domains you can try [[BigInt]], for example.
 */
class TopPctCMSMonoid[K: ClassTag](cms: CMS[K], heavyHittersPct: Double = 0.01) extends Monoid[TopCMS[K]] {

  val params: TopCMSParams[K] = {
    val logic = new TopPctLogic[K](heavyHittersPct)
    TopCMSParams[K](logic)
  }

  val zero: TopCMS[K] = TopCMSZero[K](cms, params)

  /**
   * Combines the two sketches.
   *
   * The sketches must use the same hash functions.
   */
  def plus(left: TopCMS[K], right: TopCMS[K]): TopCMS[K] = {
    require(left.cms.params.hashes == right.cms.params.hashes, "The sketches must use the same hash functions.")
    left ++ right
  }

  /**
   * Creates a sketch out of a single item.
   */
  def create(item: K): TopCMS[K] = TopCMSItem[K](item, cms + item, params)

  /**
   * Creates a sketch out of multiple items.
   */
  def create(data: Seq[K]): TopCMS[K] = {
    data.foldLeft(zero) { case (acc, x) => plus(acc, create(x)) }
  }

}

object TopPctCMS {

  def monoid[K: ClassTag: CMSHasher](eps: Double,
    delta: Double,
    seed: Int,
    heavyHittersPct: Double): TopPctCMSMonoid[K] =
    new TopPctCMSMonoid[K](CMS(eps, delta, seed), heavyHittersPct)

  def monoid[K: ClassTag: CMSHasher](depth: Int,
    width: Int,
    seed: Int,
    heavyHittersPct: Double): TopPctCMSMonoid[K] =
    monoid(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, heavyHittersPct)

  def aggregator[K: ClassTag: CMSHasher](eps: Double,
    delta: Double,
    seed: Int,
    heavyHittersPct: Double): TopPctCMSAggregator[K] =
    new TopPctCMSAggregator[K](monoid(eps, delta, seed, heavyHittersPct))

  def aggregator[K: ClassTag: CMSHasher](depth: Int,
    width: Int,
    seed: Int,
    heavyHittersPct: Double): TopPctCMSAggregator[K] =
    aggregator(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, heavyHittersPct)

}

/**
 * An Aggregator for [[TopPctCMS]].  Can be created using [[TopPctCMS.aggregator]].
 */
case class TopPctCMSAggregator[K: ClassTag](cmsMonoid: TopPctCMSMonoid[K])
  extends MonoidAggregator[K, TopCMS[K], TopCMS[K]] {

  def monoid = cmsMonoid

  def prepare(value: K): TopCMS[K] = monoid.create(value)

  def present(cms: TopCMS[K]): TopCMS[K] = cms

}

/**
 * Monoid for top-N based [[TopCMS]] sketches.  '''Use with care! (see warning below)'''
 *
 * =Warning: Adding top-N CMS instances (`++`) is an unsafe operation=
 *
 * Top-N computations are not associative.  The effect is that a top-N CMS has an ordering bias (with regard to heavy
 * hitters) when ''merging'' CMS instances (e.g. via `++`).  This means merging heavy hitters across CMS instances may
 * lead to incorrect, biased results:  the outcome is biased by the order in which CMS instances / heavy hitters are
 * being merged, with the rule of thumb being that the earlier a set of heavy hitters is being merged, the more likely
 * is the end result biased towards these heavy hitters.
 *
 * The warning above only applies when ''adding CMS instances'' (think: `cms1 ++ cms2`).  In comparison, heavy hitters
 * are correctly computed when:
 *
 *   - a top-N CMS instance is created from a single data stream, i.e. `Seq[K]`
 *   - items are added/counted individually, i.e. `cms + item` or `cms + (item, count)`.
 *
 * See the discussion in [[https://github.com/twitter/algebird/issues/353 Algebird issue 353]] for further details.
 *
 * =Alternatives=
 *
 * The following, alternative data structures may be better picks than a top-N based CMS given the warning above:
 *
 *   - [[TopPctCMS]]: Has safe merge semantics for its instances including heavy hitters.
 *   - [[SpaceSaver]]: Has the same ordering bias than a top-N CMS, but at least it provides bounds on the bias.
 *
 * =Usage=
 *
 * The type `K` is the type of items you want to count.  You must provide an implicit `CMSHasher[K]` for `K`,  and
 * Algebird ships with several such implicits for commonly used types such as [[Long]] and [[BigInt]]:
 *
 * {{{
 * import com.twitter.algebird.CMSHasherImplicits._
 * }}}
 *
 * If your type `K` is not supported out of the box, you have two options: 1) You provide a "translation" function to
 * convert items of your (unsupported) type `K` to a supported type such as `Array[Byte]`, and then use the `contramap`
 * function of [[CMSHasher]] to create the required `CMSHasher[K]` for your type (see the documentation of [[CMSHasher]]
 * for an example); 2) You implement a `CMSHasher[K]` from scratch, using the existing CMSHasher implementations as a
 * starting point.
 *
 * @param cms A [[CMS]] instance, which is used for the counting and the frequency estimation performed by this class.
 * @param heavyHittersN The maximum number of heavy hitters to track.
 * @tparam K The type used to identify the elements to be counted.  For example, if you want to count the occurrence of
 *           user names, you could map each username to a unique numeric ID expressed as a `Long`, and then count the
 *           occurrences of those `Long`s with a CMS of type `K=Long`.  Note that this mapping between the elements of
 *           your problem domain and their identifiers used for counting via CMS should be bijective.
 *           We require a [[CMSHasher]] context bound for `K`, see [[CMSHasherImplicits]] for available implicits that
 *           can be imported.
 *           Which type K should you pick in practice?  For domains that have less than `2^64` unique elements, you'd
 *           typically use [[Long]].  For larger domains you can try [[BigInt]], for example.
 */
class TopNCMSMonoid[K: ClassTag](cms: CMS[K], heavyHittersN: Int = 100) extends Monoid[TopCMS[K]] {

  val params: TopCMSParams[K] = {
    val logic = new TopNLogic[K](heavyHittersN)
    TopCMSParams[K](logic)
  }

  val zero: TopCMS[K] = TopCMSZero[K](cms, params)

  /**
   * Combines the two sketches.
   *
   * The sketches must use the same hash functions.
   */
  def plus(left: TopCMS[K], right: TopCMS[K]): TopCMS[K] = {
    require(left.cms.params.hashes == right.cms.params.hashes, "The sketches must use the same hash functions.")
    left ++ right
  }

  /**
   * Creates a sketch out of a single item.
   */
  def create(item: K): TopCMS[K] = TopCMSItem[K](item, cms + item, params)

  /**
   * Creates a sketch out of multiple items.
   */
  def create(data: Seq[K]): TopCMS[K] = data.foldLeft(zero) { case (acc, x) => plus(acc, create(x)) }

}

object TopNCMS {

  def monoid[K: ClassTag: CMSHasher](eps: Double,
    delta: Double,
    seed: Int,
    heavyHittersN: Int): TopNCMSMonoid[K] =
    new TopNCMSMonoid[K](CMS(eps, delta, seed), heavyHittersN)

  def monoid[K: ClassTag: CMSHasher](depth: Int,
    width: Int,
    seed: Int,
    heavyHittersN: Int): TopNCMSMonoid[K] =
    monoid(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, heavyHittersN)

  def aggregator[K: ClassTag: CMSHasher](eps: Double,
    delta: Double,
    seed: Int,
    heavyHittersN: Int): TopNCMSAggregator[K] =
    new TopNCMSAggregator[K](monoid(eps, delta, seed, heavyHittersN))

  def aggregator[K: ClassTag: CMSHasher](depth: Int,
    width: Int,
    seed: Int,
    heavyHittersN: Int): TopNCMSAggregator[K] =
    aggregator(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, heavyHittersN)

}

/**
 * An Aggregator for [[TopNCMS]].  Can be created using [[TopNCMS.aggregator]].
 */
case class TopNCMSAggregator[K: ClassTag](cmsMonoid: TopNCMSMonoid[K])
  extends MonoidAggregator[K, TopCMS[K], TopCMS[K]] {

  val monoid = cmsMonoid

  def prepare(value: K): TopCMS[K] = monoid.create(value)

  def present(cms: TopCMS[K]): TopCMS[K] = cms

}

/**
 * The Count-Min sketch uses `d` (aka `depth`) pair-wise independent hash functions drawn from a universal hashing
 * family of the form:
 *
 * `h(x) = [a * x + b (mod p)] (mod m)`
 *
 * As a requirement for using CMS you must provide an implicit `CMSHasher[K]` for the type `K` of the items you want to
 * count.  Algebird ships with several such implicits for commonly used types `K` such as [[Long]] and [[BigInt]]:
 *
 * {{{
 * import com.twitter.algebird.CMSHasherImplicits._
 * }}}
 *
 * If your type `K` is not supported out of the box, you have two options: 1) You provide a "translation" function to
 * convert items of your (unsupported) type `K` to a supported type such as `Array[Byte]`, and then use the `contramap`
 * function of [[CMSHasher]] to create the required `CMSHasher[K]` for your type (see the documentation of `contramap`
 * for an example); 2) You implement a `CMSHasher[K]` from scratch, using the existing CMSHasher implementations as a
 * starting point.
 */
trait CMSHasher[K] extends java.io.Serializable {

  self =>

  /**
   * Returns `a * x + b (mod p) (mod width)`.
   */
  def hash(a: Int, b: Int, width: Int)(x: K): Int

  /**
   * Given `f`, a function from `L` into `K`, creates a `CMSHasher[L]` whose hash function is equivalent to:
   *
   * {{{
   * def hash(a: Int, b: Int, width: Int)(x: L): CMSHasher[L] = CMSHasher[K].hash(a, b, width)(f(x))
   * }}}
   */
  def on[L](f: L => K) = new CMSHasher[L] {
    override def hash(a: Int, b: Int, width: Int)(x: L): Int = self.hash(a, b, width)(f(x))
  }

  /**
   * Given `f`, a function from `L` into `K`, creates a `CMSHasher[L]` whose hash function is equivalent to:
   *
   * {{{
   * def hash(a: Int, b: Int, width: Int)(x: L): CMSHasher[L] = CMSHasher[K].hash(a, b, width)(f(x))
   * }}}
   *
   * Example of creating a CMSHasher for the unsupported type `K=Double`:
   *
   * {{{
   * def f(d: Double): Array[Byte] = {
   *   val l: Long = java.lang.Double.doubleToLongBits(d)
   *   java.nio.ByteBuffer.allocate(8).putLong(l).array()
   * }
   *
   * implicit val cmsHasherDouble: CMSHasher[Double] = CMSHasherArrayByte.contramap((d: Double) => f(d))
   * }}}
   */
  def contramap[L](f: L => K) = on(f)

}

case class CMSHash[K: CMSHasher](a: Int, b: Int, width: Int) extends java.io.Serializable {

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

    override def hash(a: Int, b: Int, width: Int)(x: Long): Int = {
      val unModded: Long = (x * a) + b
      // Apparently a super fast way of computing x mod 2^p-1
      // See page 149 of http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
      // after Proposition 7.
      val modded: Long = (unModded + (unModded >> 32)) & Int.MaxValue
      // Modulo-ing integers is apparently twice as fast as modulo-ing Longs.
      modded.toInt % width
    }

  }

  implicit val cmsHasherShort: CMSHasher[Short] = CMSHasherInt.contramap(x => x.toInt)

  implicit object CMSHasherInt extends CMSHasher[Int] {

    override def hash(a: Int, b: Int, width: Int)(x: Int): Int = {
      val unModded: Int = (x * a) + b
      val modded: Long = (unModded + (unModded >> 32)) & Int.MaxValue
      modded.toInt % width
    }

  }

  implicit object CMSHasherArrayByte extends CMSHasher[Array[Byte]] {

    /**
     * =Implementation details=
     *
     * This hash function is based upon Murmur3.  Note that the original CMS paper requires
     * `d` (depth) pair-wise independent hash functions;  in the specific case of Murmur3 we argue that it is sufficient
     * to pass `d` different seed values to Murmur3 to achieve a similar effect.
     *
     * To seed Murmur3 we use only `a`, which is a randomly drawn `Int` via [[scala.util.Random]] in the CMS code.
     * What is important to note is that we intentionally ignore `b`.  Why?  We need to ensure that we seed Murmur3 with
     * a random value, notably one that is uniformly distributed.  Somewhat surprisingly, combining two random values
     * (such as `a` and `b` in our case) typically worsens the "randomness" of the combination, i.e. the combination is
     * less uniformly distributed as either of its original inputs.  Hence the combination of two random values is
     * discouraged in this context, notably if the two random inputs were generated from the same source anyways, which
     * is the case for us because we use Scala's PRNG only.
     *
     * For further details please refer to the discussion
     * [[http://stackoverflow.com/questions/3956478/understanding-randomness Understanding Randomness]] on
     * StackOverflow.
     *
     * @param a Must be a random value, typically created via [[scala.util.Random]].
     * @param b Ignored by this particular hash function, see the reasoning above for the justification.
     * @param width Width of the CMS counting table, i.e. the width/size of each row in the counting table.
     * @param x Item to be hashed.
     * @return Slot assigned to item `x` in the vector of size `width`, where `x in [0, width)`.
     */
    override def hash(a: Int, b: Int, width: Int)(x: Array[Byte]): Int = {
      val hash: Int = scala.util.hashing.MurmurHash3.arrayHash(x, a)
      // We only want positive integers for the subsequent modulo.  This method mimics Java's Hashtable
      // implementation.  The Java code uses `0x7FFFFFFF` for the bit-wise AND, which is equal to Int.MaxValue.
      val positiveHash = hash & Int.MaxValue
      positiveHash % width
    }

  }

  implicit val cmsHasherBigInt: CMSHasher[BigInt] = CMSHasherArrayByte.contramap((x: BigInt) => x.toByteArray)

  implicit val cmsHasherString: CMSHasher[String] = CMSHasherArrayByte.contramap((x: String) => x.getBytes("UTF-8"))

}