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

import algebra.CommutativeMonoid

import scala.collection.compat._

/**
 * A Count-Min sketch is a probabilistic data structure used for summarizing streams of data in sub-linear
 * space.
 *
 * It works as follows. Let `(eps, delta)` be two parameters that describe the confidence in our error
 * estimates, and let `d = ceil(ln 1/delta)` and `w = ceil(e / eps)`.
 *
 * Note: Throughout the code `d` and `w` are called `depth` and `width`, respectively.
 *
 * Then:
 *
 *   - Take `d` pairwise independent hash functions `h_i`, each of which maps onto the domain `[0, w - 1]`.
 *   - Create a 2-dimensional table of counts, with `d` rows and `w` columns, initialized with all zeroes.
 *   - When a new element x arrives in the stream, update the table of counts by setting `counts[i, h_i[x]] +=
 *     1`, for each `1 <= i <= d`.
 *   - (Note the rough similarity to a Bloom filter.)
 *
 * As an example application, suppose you want to estimate the number of times an element `x` has appeared in
 * a data stream so far. The Count-Min sketch estimate of this frequency is
 *
 * min_i { counts[i, h_i[x]] }
 *
 * With probability at least `1 - delta`, this estimate is within `eps * N` of the true frequency (i.e., `true
 * frequency <= estimate <= true frequency + eps * N`), where N is the total size of the stream so far.
 *
 * See http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf for technical details, including proofs of the
 * estimates and error bounds used in this implementation.
 *
 * Parts of this implementation are taken from
 * https://github.com/clearspring/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/frequency/CountMinSketch.java
 *
 * @author
 *   Edwin Chen
 */
/**
 * Monoid for adding CMS sketches.
 *
 * =Usage=
 *
 * `eps` and `delta` are parameters that bound the error of each query estimate. For example, errors in
 * answering point queries (e.g., how often has element x appeared in the stream described by the sketch?) are
 * often of the form: "with probability p >= 1 - delta, the estimate is close to the truth by some factor
 * depending on eps."
 *
 * The type `K` is the type of items you want to count. You must provide an implicit `CMSHasher[K]` for `K`,
 * and Algebird ships with several such implicits for commonly used types such as `Long` and `BigInt`.
 *
 * If your type `K` is not supported out of the box, you have two options: 1) You provide a "translation"
 * function to convert items of your (unsupported) type `K` to a supported type such as Double, and then use
 * the `contramap` function of [[CMSHasher]] to create the required `CMSHasher[K]` for your type (see the
 * documentation of [[CMSHasher]] for an example); 2) You implement a `CMSHasher[K]` from scratch, using the
 * existing CMSHasher implementations as a starting point.
 *
 * Note: Because Arrays in Scala/Java not have sane `equals` and `hashCode` implementations, you cannot safely
 * use types such as `Array[Byte]`. Extra work is required for Arrays. For example, you may opt to convert
 * `Array[T]` to a `Seq[T]` via `toSeq`, or you can provide appropriate wrapper classes. Algebird provides one
 * such wrapper class, [[Bytes]], to safely wrap an `Array[Byte]` for use with CMS.
 *
 * @param eps
 *   One-sided error bound on the error of each point query, i.e. frequency estimate.
 * @param delta
 *   A bound on the probability that a query estimate does not lie within some small interval (an interval
 *   that depends on `eps`) around the truth.
 * @param seed
 *   A seed to initialize the random number generator used to create the pairwise independent hash functions.
 * @param maxExactCountOpt
 *   An Option parameter about how many exact counts a sparse CMS wants to keep.
 * @tparam K
 *   The type used to identify the elements to be counted. For example, if you want to count the occurrence of
 *   user names, you could map each username to a unique numeric ID expressed as a `Long`, and then count the
 *   occurrences of those `Long`s with a CMS of type `K=Long`. Note that this mapping between the elements of
 *   your problem domain and their identifiers used for counting via CMS should be bijective. We require a
 *   [[CMSHasher]] context bound for `K`, see [[CMSHasherImplicits]] for available implicits that can be
 *   imported. Which type K should you pick in practice? For domains that have less than `2^64` unique
 *   elements, you'd typically use `Long`. For larger domains you can try `BigInt`, for example. Other
 *   possibilities include Spire's `SafeLong` and `Numerical` data types (https://github.com/non/spire),
 *   though Algebird does not include the required implicits for CMS-hashing (cf. [[CMSHasherImplicits]].
 */
class CMSMonoid[K: CMSHasher](eps: Double, delta: Double, seed: Int, maxExactCountOpt: Option[Int] = None)
    extends Monoid[CMS[K]]
    with CommutativeMonoid[CMS[K]] {

  val params: CMSParams[K] = {
    val hashes: Seq[CMSHash[K]] = CMSFunctions.generateHashes(eps, delta, seed)
    CMSParams(hashes, eps, delta, maxExactCountOpt)
  }

  override val zero: CMS[K] = CMSZero[K](params)

  /**
   * Combines the two sketches.
   *
   * The sketches must use the same hash functions.
   */
  override def plus(left: CMS[K], right: CMS[K]): CMS[K] = {
    require(left.params.hashes == right.params.hashes, "The sketches must use the same hash functions.")
    left ++ right
  }

  /**
   * Creates a sketch out of a single item.
   */
  def create(item: K): CMS[K] = CMSItem[K](item, 1L, params)

  /**
   * Creates a sketch out of multiple items.
   */
  def create(data: Seq[K]): CMS[K] = {
    val summation = new CMSSummation(params)
    data.foreach(k => summation.insert(k, 1L))
    summation.result
  }

  override def sumOption(sketches: TraversableOnce[CMS[K]]): Option[CMS[K]] =
    if (sketches.iterator.isEmpty) None else Some(sum(sketches))

  override def sum(sketches: TraversableOnce[CMS[K]]): CMS[K] = {
    val summation = new CMSSummation(params)
    summation.updateAll(sketches)
    summation.result
  }
}

/**
 * This mutable builder can be used when speed is essential and you can be sure the scope of the mutability
 * cannot escape in an unsafe way. The intended use is to allocate and call result in one method without
 * letting a reference to the instance escape into a closure.
 */
class CMSSummation[K](params: CMSParams[K]) {
  private[this] val hashes = params.hashes.toArray
  private[this] val height = CMSFunctions.depth(params.delta)
  private[this] val width = CMSFunctions.width(params.eps)
  private[this] val cells = new Array[Long](height * width)
  private[this] var totalCount = 0L

  final def insert(k: K, count: Long): Unit = {
    var row = 0
    var offset = 0
    val hs = hashes
    while (row < hs.length) {
      cells(offset + hs(row)(k)) += count
      offset += width
      row += 1
    }
    totalCount += count
  }

  def updateAll(sketches: TraversableOnce[CMS[K]]): Unit =
    sketches.iterator.foreach(updateInto)

  def updateInto(cms: CMS[K]): Unit =
    cms match {
      case CMSZero(_) =>
        ()
      case CMSItem(item, count, _) =>
        insert(item, count)
      case SparseCMS(table, _, _) =>
        table.foreach { case (item, c) =>
          insert(item, c)
        }
      case CMSInstance(CMSInstance.CountsTable(matrix), count, _) =>
        var offset = 0
        val rit = matrix.iterator
        while (rit.hasNext) {
          var col = 0
          val cit = rit.next().iterator
          while (cit.hasNext) {
            cells(offset + col) += cit.next()
            col += 1
          }
          offset += width
        }
        totalCount += count
    }

  def result: CMS[K] =
    if (totalCount == 0L) CMSZero(params)
    else {
      def vectorize(row: Int): Vector[Long] = {
        val offset = row * width
        val b = Vector.newBuilder[Long]
        var col = 0
        while (col < width) {
          b += cells(offset + col)
          col += 1
        }
        b.result()
      }

      val b = Vector.newBuilder[Vector[Long]]
      var row = 0
      while (row < height) {
        b += vectorize(row)
        row += 1
      }
      CMSInstance(CMSInstance.CountsTable(b.result()), totalCount, params)
    }
}

/**
 * An Aggregator for [[CMS]]. Can be created using CMS.aggregator.
 */
case class CMSAggregator[K](cmsMonoid: CMSMonoid[K]) extends MonoidAggregator[K, CMS[K], CMS[K]] {
  override val monoid: CMSMonoid[K] = cmsMonoid

  override def prepare(value: K): CMS[K] = monoid.create(value)

  override def present(cms: CMS[K]): CMS[K] = cms

}

/**
 * Configuration parameters for [[CMS]].
 *
 * @param hashes
 *   Pair-wise independent hashes functions. We need `N=depth` such functions (`depth` can be derived from
 *   `delta`).
 * @param eps
 *   One-sided error bound on the error of each point query, i.e. frequency estimate.
 * @param delta
 *   A bound on the probability that a query estimate does not lie within some small interval (an interval
 *   that depends on `eps`) around the truth.
 * @param maxExactCountOpt
 *   An Option parameter about how many exact counts a sparse CMS wants to keep.
 * @tparam K
 *   The type used to identify the elements to be counted.
 */
case class CMSParams[K](
    hashes: Seq[CMSHash[K]],
    eps: Double,
    delta: Double,
    maxExactCountOpt: Option[Int] = None
) {

  require(0 < eps && eps < 1, "eps must lie in (0, 1)")
  require(0 < delta && delta < 1, "delta must lie in (0, 1)")
  require(
    hashes.size >= CMSFunctions.depth(delta),
    s"we require at least ${CMSFunctions.depth(delta)} hash functions"
  )

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
    val i = scala.math.exp(-depth)
    require(
      i > 0.0,
      s"depth must be smaller as it causes precision errors when computing delta ($depth led to an invalid delta of $i)"
    )
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
  def width(eps: Double): Int =
    scala.math.ceil(truncatePrecisionError(scala.math.exp(1) / eps)).toInt

  /**
   * Compute maxExactCount from parameters or `depth` and `width`
   */
  def maxExactCount(maxExactCountOpt: Option[Int], depth: Int, width: Int): Int =
    maxExactCountOpt.getOrElse(math.max(width * depth / 100, 50))

  // Eliminates precision errors such as the following:
  //
  //   scala> val width = 39
  //   scala> scala.math.exp(1) / CMSFunctions.eps(width)
  //   res171: Double = 39.00000000000001   <<< should be 39.0
  //
  // Because of the actual types on which CMSFunctions operates (i.e. Int and Double), the maximum number of decimal
  // places should be 6.
  private def truncatePrecisionError(i: Double, decimalPlaces: Int = 6) =
    BigDecimal(i)
      .setScale(decimalPlaces, BigDecimal.RoundingMode.HALF_UP)
      .toDouble

  /**
   * Generates `N=depth` pair-wise independent hash functions.
   *
   * @param eps
   *   One-sided error bound on the error of each point query, i.e. frequency estimate.
   * @param delta
   *   Error bound on the probability that a query estimate does NOT lie within some small interval around the
   *   truth.
   * @param seed
   *   Seed for the random number generator.
   * @tparam K
   *   The type used to identify the elements to be counted.
   * @return
   *   The generated hash functions.
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
    (0 to (numHashes - 1)).map(_ => CMSHash[K](r.nextInt(), 0, numCounters))
  }

}

/**
 * A trait for CMS implementations that can count elements in a data stream and that can answer point queries
 * (i.e. frequency estimates) for these elements.
 *
 * Known implementations: [[CMS]], [[TopCMS]].
 *
 * @tparam K
 *   The type used to identify the elements to be counted.
 * @tparam C
 *   The type of the actual CMS that implements this trait.
 */
trait CMSCounting[K, C[_]] {

  /**
   * Returns the one-sided error bound on the error of each point query, i.e. frequency estimate.
   */
  def eps: Double

  /**
   * Returns the bound on the probability that a query estimate does NOT lie within some small interval (an
   * interval that depends on `eps`) around the truth.
   */
  def delta: Double

  /**
   * Number of hash functions (also: number of rows in the counting table). This number is derived from
   * `delta`.
   */
  def depth: Int = CMSFunctions.depth(delta)

  /**
   * Number of counters per hash function (also: number of columns in the counting table). This number is
   * derived from `eps`.
   */
  def width: Int = CMSFunctions.width(eps)

  /**
   * An Option parameter about how many exact counts a sparse CMS wants to keep
   */
  def maxExactCountOpt: Option[Int]

  /**
   * Number of exact counts a sparse CMS wants to keep. This number is derived from `maxExactCountOpt`.
   */
  def maxExactCount: Int =
    CMSFunctions.maxExactCount(maxExactCountOpt, depth, width)

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
   * Returns an estimate of the total number of times this item has been seen in the stream so far. This
   * estimate is an upper bound.
   *
   * It is always true that `estimatedFrequency >= trueFrequency`. With probability `p >= 1 - delta`, it also
   * holds that `estimatedFrequency <= trueFrequency + eps * totalCount`.
   */
  def frequency(item: K): Approximate[Long]

  /**
   * Returns an estimate of the inner product against another data stream.
   *
   * In other words, let a_i denote the number of times element i has been seen in the data stream summarized
   * by this CMS, and let b_i denote the same for the other CMS. Then this returns an estimate of `<a, b> =
   * \sum a_i b_i`.
   *
   * Note: This can also be viewed as the join size between two relations.
   *
   * It is always true that actualInnerProduct <= estimatedInnerProduct. With probability `p >= 1 - delta`, it
   * also holds that `estimatedInnerProduct <= actualInnerProduct + eps * thisTotalCount * otherTotalCount`.
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
 * It is up to the implementation how the semantics of tracking heavy hitters are defined. For instance, one
 * implementation could track the "top %" heavy hitters whereas another implementation could track the "top N"
 * heavy hitters.
 *
 * Known implementations: [[TopCMS]].
 *
 * @tparam K
 *   The type used to identify the elements to be counted.
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
    monoid(eps, delta, seed, None)
  def monoid[K: CMSHasher](
      eps: Double,
      delta: Double,
      seed: Int,
      maxExactCountOpt: Option[Int]
  ): CMSMonoid[K] =
    new CMSMonoid[K](eps, delta, seed, maxExactCountOpt)

  def monoid[K: CMSHasher](depth: Int, width: Int, seed: Int): CMSMonoid[K] =
    monoid(depth, width, seed, None)
  def monoid[K: CMSHasher](depth: Int, width: Int, seed: Int, maxExactCountOpt: Option[Int]): CMSMonoid[K] =
    monoid(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, maxExactCountOpt)

  def aggregator[K: CMSHasher](eps: Double, delta: Double, seed: Int): CMSAggregator[K] =
    aggregator(eps, delta, seed, None)
  def aggregator[K: CMSHasher](
      eps: Double,
      delta: Double,
      seed: Int,
      maxExactCountOpt: Option[Int]
  ): CMSAggregator[K] =
    new CMSAggregator[K](monoid(eps, delta, seed, maxExactCountOpt))

  def aggregator[K: CMSHasher](depth: Int, width: Int, seed: Int): CMSAggregator[K] =
    aggregator(depth, width, seed, None)
  def aggregator[K: CMSHasher](
      depth: Int,
      width: Int,
      seed: Int,
      maxExactCountOpt: Option[Int]
  ): CMSAggregator[K] =
    aggregator(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, maxExactCountOpt)

  /**
   * Returns a fresh, zeroed CMS instance.
   */
  def apply[K: CMSHasher](
      eps: Double,
      delta: Double,
      seed: Int,
      maxExactCountOpt: Option[Int] = None
  ): CMS[K] = {
    val params = {
      val hashes: Seq[CMSHash[K]] =
        CMSFunctions.generateHashes(eps, delta, seed)
      CMSParams(hashes, eps, delta, maxExactCountOpt)
    }
    CMSZero[K](params)
  }

}

/**
 * A Count-Min sketch data structure that allows for counting and frequency estimation of elements in a data
 * stream.
 *
 * Tip: If you also need to track heavy hitters ("Top N" problems), take a look at [[TopCMS]].
 *
 * =Usage=
 *
 * This example demonstrates how to count `Long` elements with [[CMS]], i.e. `K=Long`.
 *
 * Note that the actual counting is always performed with a `Long`, regardless of your choice of `K`. That is,
 * the counting table behind the scenes is backed by `Long` values (at least in the current implementation),
 * and thus the returned frequency estimates are always instances of `Approximate[Long]`.
 *
 * @example
 *   {{{
 *
 * // Creates a monoid for a CMS that can count `Long` elements. val cmsMonoid: CMSMonoid[Long] = { val eps =
 * 0.001 val delta = 1E-10 val seed = 1 CMS.monoid[Long](eps, delta, seed) }
 *
 * // Creates a CMS instance that has counted the element `1L`. val cms: CMS[Long] = cmsMonoid.create(1L)
 *
 * // Estimates the frequency of `1L` val estimate: Approximate[Long] = cms.frequency(1L)
 *   }}}
 *
 * @tparam K
 *   The type used to identify the elements to be counted.
 */
sealed abstract class CMS[K](val params: CMSParams[K]) extends java.io.Serializable with CMSCounting[K, CMS] {

  override val eps: Double = params.eps

  override val delta: Double = params.delta

  override val maxExactCountOpt: Option[Int] = params.maxExactCountOpt

  override def f2: Approximate[Long] = innerProduct(this)

}

/**
 * Zero element. Used for initialization.
 */
case class CMSZero[K](override val params: CMSParams[K]) extends CMS[K](params) {

  override val totalCount: Long = 0L

  override def +(item: K, count: Long): CMS[K] = CMSItem[K](item, count, params)

  override def ++(other: CMS[K]): CMS[K] = other

  override def frequency(item: K): Approximate[Long] = Approximate.exact(0L)

  override def innerProduct(other: CMS[K]): Approximate[Long] =
    Approximate.exact(0L)

}

/**
 * Used for holding a single element, to avoid repeatedly adding elements from sparse counts tables.
 */
case class CMSItem[K](item: K, override val totalCount: Long, override val params: CMSParams[K])
    extends CMS[K](params) {

  override def +(x: K, count: Long): CMS[K] =
    SparseCMS[K](params) + (item, totalCount) + (x, count)

  override def ++(other: CMS[K]): CMS[K] =
    other match {
      case _: CMSZero[_] => this
      case other: CMSItem[K] =>
        CMSInstance[K](params) + (item, totalCount) + (other.item, other.totalCount)
      case _ => other + item
    }

  override def frequency(x: K): Approximate[Long] =
    if (item == x) Approximate.exact(totalCount) else Approximate.exact(0L)

  override def innerProduct(other: CMS[K]): Approximate[Long] =
    Approximate.exact(totalCount) * other.frequency(item)

}

/**
 * A sparse Count-Min sketch structure, used for situations where the key is highly skewed.
 */
case class SparseCMS[K](
    exactCountTable: Map[K, Long],
    override val totalCount: Long,
    override val params: CMSParams[K]
) extends CMS[K](params) {
  import SparseCMS._

  override def +(x: K, count: Long): CMS[K] = {
    val currentCount = exactCountTable.getOrElse(x, 0L)
    val newTable = exactCountTable.updated(x, currentCount + count)
    if (newTable.size < maxExactCount) {
      // still sparse
      SparseCMS(newTable, totalCount = totalCount + count, params = params)
    } else {
      toDense(newTable, params)
    }
  }

  override def ++(other: CMS[K]): CMS[K] =
    other match {
      case _: CMSZero[_]       => this
      case other: CMSItem[K]   => this + (other.item, other.totalCount)
      case other: SparseCMS[K] =>
        // This SparseCMS's maxExactCount is used, so ++ is not communitive
        val newTable = Semigroup.plus(exactCountTable, other.exactCountTable)
        if (newTable.size < maxExactCount) {
          // still sparse
          SparseCMS(newTable, totalCount = totalCount + other.totalCount, params = params)
        } else {
          toDense(newTable, params)
        }

      case other: CMSInstance[K] => other ++ this
    }

  override def frequency(x: K): Approximate[Long] =
    Approximate.exact(exactCountTable.getOrElse(x, 0L))

  override def innerProduct(other: CMS[K]): Approximate[Long] =
    exactCountTable.iterator
      .map { case (x, count) => Approximate.exact(count) * other.frequency(x) }
      .reduceOption(_ + _)
      .getOrElse(Approximate.exact(0L))
}

object SparseCMS {

  /**
   * Creates a new [[SparseCMS]] with empty exactCountTable
   */
  def apply[K](params: CMSParams[K]): SparseCMS[K] = {
    val exactCountTable = Map[K, Long]()
    SparseCMS[K](exactCountTable, totalCount = 0, params = params)
  }

  /**
   * Creates a new [[CMSInstance]] from a Map[K, Long]
   */
  def toDense[K](exactCountTable: Map[K, Long], params: CMSParams[K]): CMS[K] =
    // Create new CMSInstace
    exactCountTable.foldLeft(CMSInstance[K](params)) { case (cms, (x, count)) =>
      cms + (x, count)
    }
}

/**
 * The general Count-Min sketch structure, used for holding any number of elements.
 */
case class CMSInstance[K](
    countsTable: CMSInstance.CountsTable[K],
    override val totalCount: Long,
    override val params: CMSParams[K]
) extends CMS[K](params) {

  override def ++(other: CMS[K]): CMS[K] =
    other match {
      case _: CMSZero[_]     => this
      case other: CMSItem[K] => this + other.item
      case other: SparseCMS[K] =>
        other.exactCountTable.foldLeft(this) { case (cms, (x, count)) =>
          cms + (x, count)
        }
      case other: CMSInstance[K] =>
        val newTable = countsTable ++ other.countsTable
        val newTotalCount = totalCount + other.totalCount
        CMSInstance[K](newTable, newTotalCount, params)
    }

  private def makeApprox(est: Long): Approximate[Long] =
    if (est == 0L) Approximate.exact(0L)
    else {
      val lower = math.max(0L, est - (eps * totalCount).toLong)
      Approximate(lower, est, est, 1 - delta)
    }

  override def frequency(item: K): Approximate[Long] = {
    var freq = Long.MaxValue
    val hs = params.hashes
    val it = countsTable.counts.iterator
    var i = 0
    while (it.hasNext) {
      val row = it.next()
      val count = row(hs(i)(item))
      if (count < freq) freq = count
      i += 1
    }
    makeApprox(freq)
  }

  /**
   * Let X be a CMS, and let count_X[j, k] denote the value in X's 2-dimensional count table at row j and
   * column k. Then the Count-Min sketch estimate of the inner product between A and B is the minimum inner
   * product between their rows: estimatedInnerProduct = min_j (\sum_k count_A[j, k] * count_B[j, k]|)
   */
  override def innerProduct(other: CMS[K]): Approximate[Long] =
    other match {
      case other: CMSInstance[_] =>
        require(other.depth == depth && other.width == width, "Tables must have the same dimensions.")

        def innerProductAtDepth(d: Int) =
          (0 to (width - 1)).iterator.map { w =>
            countsTable.getCount((d, w)) * other.countsTable.getCount((d, w))
          }.sum

        val est = (0 to (depth - 1)).iterator.map(innerProductAtDepth).min
        val minimum =
          math.max(est - (eps * totalCount * other.totalCount).toLong, 0)
        Approximate(minimum, est, est, 1 - delta)
      case _ => other.innerProduct(this)
    }

  override def +(item: K, count: Long): CMSInstance[K] = {
    require(count >= 0, "count must be >= 0 (negative counts not implemented")
    if (count != 0L) {
      val newCountsTable =
        (0 to (depth - 1)).foldLeft(countsTable) { case (table, row) =>
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
   * The 2-dimensional table of counters used in the Count-Min sketch. Each row corresponds to a particular
   * hash function.
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
      val newCounts =
        counts.updated(row, counts(row).updated(col, currCount + count))
      CountsTable[K](newCounts)
    }

    /**
     * Adds another counts table to this one, through element-wise addition.
     */
    def ++(other: CountsTable[K]): CountsTable[K] = {
      require(depth == other.depth && width == other.width, "Tables must have the same dimensions.")
      val xss = this.counts.iterator
      val yss = other.counts.iterator
      val rows = Vector.newBuilder[Vector[Long]]
      while (xss.hasNext) {
        val xs = xss.next().iterator
        val ys = yss.next().iterator
        val row = Vector.newBuilder[Long]
        while (xs.hasNext) row += (xs.next() + ys.next())
        rows += row.result()
      }
      CountsTable[K](rows.result())
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
 * A Count-Min sketch data structure that allows for (a) counting and frequency estimation of elements in a
 * data stream and (b) tracking the heavy hitters among these elements.
 *
 * The logic of how heavy hitters are computed is pluggable, see [[HeavyHittersLogic]].
 *
 * Tip: If you do not need to track heavy hitters, take a look at [[CMS]], which is more efficient in this
 * case.
 *
 * =Usage=
 *
 * This example demonstrates how to count `Long` elements with [[TopCMS]], i.e. `K=Long`.
 *
 * Note that the actual counting is always performed with a `Long`, regardless of your choice of `K`. That is,
 * the counting table behind the scenes is backed by `Long` values (at least in the current implementation),
 * and thus the returned frequency estimates are always instances of `Approximate[Long]`.
 *
 * @example
 *   {{{ // Creates a monoid for a CMS that can count `Long` elements. val topPctCMSMonoid:
 *   TopPctCMSMonoid[Long] = { val eps = 0.001 val delta = 1E-10 val seed = 1 val heavyHittersPct = 0.1
 *   TopPctCMS.monoid[Long](eps, delta, seed, heavyHittersPct) }
 *
 * // Creates a TopCMS instance that has counted the element `1L`. val topCMS: TopCMS[Long] =
 * topPctCMSMonoid.create(1L)
 *
 * // Estimates the frequency of `1L` val estimate: Approximate[Long] = topCMS.frequency(1L)
 *
 * // What are the heavy hitters so far? val heavyHitters: Set[Long] = topCMS.heavyHitters }}}
 *
 * @tparam K
 *   The type used to identify the elements to be counted.
 */
sealed abstract class TopCMS[K](val cms: CMS[K], params: TopCMSParams[K])
    extends java.io.Serializable
    with CMSCounting[K, TopCMS]
    with CMSHeavyHitters[K] {

  override val eps: Double = cms.eps

  override val delta: Double = cms.delta

  override val totalCount: Long = cms.totalCount

  override val maxExactCountOpt: Option[Int] = cms.maxExactCountOpt

  override def frequency(item: K): Approximate[Long] = cms.frequency(item)

  override def innerProduct(other: TopCMS[K]): Approximate[Long] =
    cms.innerProduct(other.cms)

  override def f2: Approximate[Long] = innerProduct(this)

  /**
   * The pluggable logic with which heavy hitters are being tracked.
   */
  override def heavyHittersLogic: HeavyHittersLogic[K] = params.logic

}

/**
 * Zero element. Used for initialization.
 */
case class TopCMSZero[K](override val cms: CMS[K], params: TopCMSParams[K]) extends TopCMS[K](cms, params) {

  override val heavyHitters: Set[K] = Set.empty[K]

  override def +(item: K, count: Long): TopCMS[K] =
    TopCMSInstance(cms, params) + (item, count)

  override def ++(other: TopCMS[K]): TopCMS[K] = other

}

/**
 * Used for holding a single element, to avoid repeatedly adding elements from sparse counts tables.
 */
case class TopCMSItem[K](item: K, override val cms: CMS[K], params: TopCMSParams[K])
    extends TopCMS[K](cms, params) {

  override val heavyHitters: Set[K] = Set(item)

  override def +(x: K, count: Long): TopCMS[K] = toCMSInstance + (x, count)

  override def ++(other: TopCMS[K]): TopCMS[K] = other match {
    case _: TopCMSZero[_]         => this
    case other: TopCMSItem[K]     => toCMSInstance + other.item
    case other: TopCMSInstance[K] => other + item
  }

  private def toCMSInstance: TopCMSInstance[K] = {
    val hhs = HeavyHitters.from(HeavyHitter(item, 1L))
    TopCMSInstance(cms, hhs, params)
  }

}

object TopCMSInstance {

  def apply[K](cms: CMS[K], params: TopCMSParams[K]): TopCMSInstance[K] =
    TopCMSInstance[K](cms, HeavyHitters.empty[K], params)

}

case class TopCMSInstance[K](override val cms: CMS[K], hhs: HeavyHitters[K], params: TopCMSParams[K])
    extends TopCMS[K](cms, params) {

  override def heavyHitters: Set[K] = hhs.items

  override def +(item: K, count: Long): TopCMSInstance[K] = {
    require(count >= 0, "count must be >= 0 (negative counts not implemented")
    if (count != 0L) {
      val newCms = cms + (item, count)
      val newHhs =
        heavyHittersLogic.updateHeavyHitters(cms, newCms)(hhs, item, count)
      TopCMSInstance[K](newCms, newHhs, params)
    } else this
  }

  override def ++(other: TopCMS[K]): TopCMS[K] = other match {
    case _: TopCMSZero[_]     => this
    case other: TopCMSItem[K] => this + other.item
    case other: TopCMSInstance[K] =>
      val newCms = cms ++ other.cms
      val newHhs = heavyHittersLogic.updateHeavyHitters(newCms)(hhs, other.hhs)
      TopCMSInstance(newCms, newHhs, params)
  }

}

class TopCMSMonoid[K](emptyCms: CMS[K], logic: HeavyHittersLogic[K]) extends Monoid[TopCMS[K]] {

  val params: TopCMSParams[K] = TopCMSParams(logic)

  override val zero: TopCMS[K] = TopCMSZero[K](emptyCms, params)

  /**
   * Combines the two sketches.
   *
   * The sketches must use the same hash functions.
   */
  override def plus(left: TopCMS[K], right: TopCMS[K]): TopCMS[K] = {
    require(
      left.cms.params.hashes == right.cms.params.hashes,
      "The sketches must use the same hash functions."
    )
    left ++ right
  }

  /**
   * Creates a sketch out of a single item.
   */
  def create(item: K): TopCMS[K] =
    TopCMSItem[K](item, emptyCms + item, params)

  /**
   * Creates a sketch out of multiple items.
   */
  def create(data: Seq[K]): TopCMS[K] =
    data.foldLeft(zero) { case (acc, x) => plus(acc, create(x)) }

  override def sum(sketches: TraversableOnce[TopCMS[K]]): TopCMS[K] = {
    val topCandidates = scala.collection.mutable.Set.empty[K]
    val summation = new CMSSummation(emptyCms.params)
    sketches.iterator.foreach { sketch =>
      summation.updateInto(sketch.cms)
      topCandidates ++= sketch.heavyHitters
    }
    val cms = summation.result
    val ests =
      topCandidates.map(k => HeavyHitter(k, cms.frequency(k).estimate)).toSet
    val hhs = logic.purgeHeavyHitters(cms)(HeavyHitters(ests))
    TopCMSInstance(cms, hhs, params)
  }

  override def sumOption(sketches: TraversableOnce[TopCMS[K]]): Option[TopCMS[K]] =
    if (sketches.iterator.isEmpty) None else Some(sum(sketches))
}

class TopCMSAggregator[K](cmsMonoid: TopCMSMonoid[K]) extends MonoidAggregator[K, TopCMS[K], TopCMS[K]] {

  override def monoid: TopCMSMonoid[K] = cmsMonoid

  override def prepare(value: K): TopCMS[K] = monoid.create(value)

  override def present(cms: TopCMS[K]): TopCMS[K] = cms

}

/**
 * Controls how a CMS that implements [[CMSHeavyHitters]] tracks heavy hitters.
 */
abstract class HeavyHittersLogic[K] extends java.io.Serializable {

  def updateHeavyHitters(
      oldCms: CMS[K],
      newCms: CMS[K]
  )(hhs: HeavyHitters[K], item: K, count: Long): HeavyHitters[K] = {
    val oldItemCount = oldCms.frequency(item).estimate
    val oldHh = HeavyHitter[K](item, oldItemCount)
    val newItemCount = oldItemCount + count
    val newHh = HeavyHitter[K](item, newItemCount)
    purgeHeavyHitters(newCms)(hhs - oldHh + newHh)
  }

  def updateHeavyHitters(cms: CMS[K])(left: HeavyHitters[K], right: HeavyHitters[K]): HeavyHitters[K] = {
    val candidates = (left.items ++ right.items).map { case i =>
      HeavyHitter[K](i, cms.frequency(i).estimate)
    }
    val newHhs = HeavyHitters.from(candidates)
    purgeHeavyHitters(cms)(newHhs)
  }

  def purgeHeavyHitters(cms: CMS[K])(hhs: HeavyHitters[K]): HeavyHitters[K]

}

/**
 * Finds all heavy hitters, i.e., elements in the stream that appear at least `(heavyHittersPct * totalCount)`
 * times.
 *
 * Every item that appears at least `(heavyHittersPct * totalCount)` times is output, and with probability `p
 * >= 1 - delta`, no item whose count is less than `(heavyHittersPct - eps) * totalCount` is output.
 *
 * This also means that this parameter is an upper bound on the number of heavy hitters that will be tracked:
 * the set of heavy hitters contains at most `1 / heavyHittersPct` elements. For example, if
 * `heavyHittersPct=0.01` (or 0.25), then at most `1 / 0.01 = 100` items (or `1 / 0.25 = 4` items) will be
 * tracked/returned as heavy hitters. This parameter can thus control the memory footprint required for
 * tracking heavy hitters.
 */
case class TopPctLogic[K](heavyHittersPct: Double) extends HeavyHittersLogic[K] {

  require(0 < heavyHittersPct && heavyHittersPct < 1, "heavyHittersPct must lie in (0, 1)")

  override def purgeHeavyHitters(cms: CMS[K])(hitters: HeavyHitters[K]): HeavyHitters[K] = {
    val minCount = heavyHittersPct * cms.totalCount
    HeavyHitters[K](hitters.hhs.filter(_.count >= minCount))
  }

}

/**
 * Tracks the top N heavy hitters, where `N` is defined by `heavyHittersN`.
 *
 * '''Warning:''' top-N computations are not associative. The effect is that a top-N CMS has an ordering bias
 * (with regard to heavy hitters) when merging instances. This means merging heavy hitters across CMS
 * instances may lead to incorrect, biased results: the outcome is biased by the order in which CMS instances
 * / heavy hitters are being merged, with the rule of thumb being that the earlier a set of heavy hitters is
 * being merged, the more likely is the end result biased towards these heavy hitters.
 *
 * @see
 *   Discussion in [[https://github.com/twitter/algebird/issues/353 Algebird issue 353]]
 */
case class TopNLogic[K](heavyHittersN: Int) extends HeavyHittersLogic[K] {

  require(heavyHittersN > 0, "heavyHittersN must be > 0")

  override def purgeHeavyHitters(cms: CMS[K])(hitters: HeavyHitters[K]): HeavyHitters[K] = {
    val sorted =
      hitters.hhs.toSeq.sortBy(hh => hh.count).takeRight(heavyHittersN)
    HeavyHitters[K](sorted.toSet)
  }

}

/**
 * Containers for holding heavy hitter items and their associated counts.
 */
case class HeavyHitters[K](hhs: Set[HeavyHitter[K]]) extends java.io.Serializable {

  def -(hh: HeavyHitter[K]): HeavyHitters[K] = HeavyHitters[K](hhs - hh)

  def +(hh: HeavyHitter[K]): HeavyHitters[K] = HeavyHitters[K](hhs + hh)

  def ++(other: HeavyHitters[K]): HeavyHitters[K] =
    HeavyHitters[K](hhs ++ other.hhs)

  def items: Set[K] = hhs.map(_.item)

}

object HeavyHitters {

  def empty[K]: HeavyHitters[K] = HeavyHitters(emptyHhs)

  private def emptyHhs[K]: Set[HeavyHitter[K]] = Set[HeavyHitter[K]]()

  def from[K](hhs: Set[HeavyHitter[K]]): HeavyHitters[K] =
    hhs.foldLeft(empty[K])(_ + _)

  def from[K](hh: HeavyHitter[K]): HeavyHitters[K] = HeavyHitters(emptyHhs + hh)

}

case class HeavyHitter[K](item: K, count: Long) extends java.io.Serializable

/**
 * Monoid for Top-% based [[TopCMS]] sketches.
 *
 * =Usage=
 *
 * The type `K` is the type of items you want to count. You must provide an implicit `CMSHasher[K]` for `K`,
 * and Algebird ships with several such implicits for commonly used types such as `Long` and `BigInt`.
 *
 * If your type `K` is not supported out of the box, you have two options: 1) You provide a "translation"
 * function to convert items of your (unsupported) type `K` to a supported type such as Double, and then use
 * the `contramap` function of [[CMSHasher]] to create the required `CMSHasher[K]` for your type (see the
 * documentation of [[CMSHasher]] for an example); 2) You implement a `CMSHasher[K]` from scratch, using the
 * existing CMSHasher implementations as a starting point.
 *
 * Note: Because Arrays in Scala/Java not have sane `equals` and `hashCode` implementations, you cannot safely
 * use types such as `Array[Byte]`. Extra work is required for Arrays. For example, you may opt to convert
 * `Array[T]` to a `Seq[T]` via `toSeq`, or you can provide appropriate wrapper classes. Algebird provides one
 * such wrapper class, [[Bytes]], to safely wrap an `Array[Byte]` for use with CMS.
 *
 * @param cms
 *   A [[CMS]] instance, which is used for the counting and the frequency estimation performed by this class.
 * @param heavyHittersPct
 *   A threshold for finding heavy hitters, i.e., elements that appear at least (heavyHittersPct * totalCount)
 *   times in the stream.
 * @tparam K
 *   The type used to identify the elements to be counted. For example, if you want to count the occurrence of
 *   user names, you could map each username to a unique numeric ID expressed as a `Long`, and then count the
 *   occurrences of those `Long`s with a CMS of type `K=Long`. Note that this mapping between the elements of
 *   your problem domain and their identifiers used for counting via CMS should be bijective. We require a
 *   [[CMSHasher]] context bound for `K`, see [[CMSHasher]] for available implicits that can be imported.
 *   Which type K should you pick in practice? For domains that have less than `2^64` unique elements, you'd
 *   typically use `Long`. For larger domains you can try `BigInt`, for example.
 */
class TopPctCMSMonoid[K](cms: CMS[K], heavyHittersPct: Double = 0.01)
    extends TopCMSMonoid[K](cms, TopPctLogic[K](heavyHittersPct))

object TopPctCMS {

  def monoid[K: CMSHasher](
      eps: Double,
      delta: Double,
      seed: Int,
      heavyHittersPct: Double
  ): TopPctCMSMonoid[K] =
    new TopPctCMSMonoid[K](CMS(eps, delta, seed), heavyHittersPct)

  def monoid[K: CMSHasher](depth: Int, width: Int, seed: Int, heavyHittersPct: Double): TopPctCMSMonoid[K] =
    monoid(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, heavyHittersPct)

  def aggregator[K: CMSHasher](
      eps: Double,
      delta: Double,
      seed: Int,
      heavyHittersPct: Double
  ): TopPctCMSAggregator[K] =
    new TopPctCMSAggregator[K](monoid(eps, delta, seed, heavyHittersPct))

  def aggregator[K: CMSHasher](
      depth: Int,
      width: Int,
      seed: Int,
      heavyHittersPct: Double
  ): TopPctCMSAggregator[K] =
    aggregator(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, heavyHittersPct)

}

/**
 * An Aggregator for [[TopPctCMS]]. Can be created using [[TopPctCMS.aggregator]].
 */
case class TopPctCMSAggregator[K](cmsMonoid: TopPctCMSMonoid[K]) extends TopCMSAggregator(cmsMonoid)

/**
 * Monoid for top-N based [[TopCMS]] sketches. '''Use with care! (see warning below)'''
 *
 * =Warning: Adding top-N CMS instances (`++`) is an unsafe operation=
 *
 * Top-N computations are not associative. The effect is that a top-N CMS has an ordering bias (with regard to
 * heavy hitters) when ''merging'' CMS instances (e.g. via `++`). This means merging heavy hitters across CMS
 * instances may lead to incorrect, biased results: the outcome is biased by the order in which CMS instances
 * / heavy hitters are being merged, with the rule of thumb being that the earlier a set of heavy hitters is
 * being merged, the more likely is the end result biased towards these heavy hitters.
 *
 * The warning above only applies when ''adding CMS instances'' (think: `cms1 ++ cms2`). In comparison, heavy
 * hitters are correctly computed when:
 *
 *   - a top-N CMS instance is created from a single data stream, i.e. `Seq[K]`
 *   - items are added/counted individually, i.e. `cms + item` or `cms + (item, count)`.
 *
 * See the discussion in [[https://github.com/twitter/algebird/issues/353 Algebird issue 353]] for further
 * details.
 *
 * =Alternatives=
 *
 * The following, alternative data structures may be better picks than a top-N based CMS given the warning
 * above:
 *
 *   - [[TopPctCMS]]: Has safe merge semantics for its instances including heavy hitters.
 *   - [[SpaceSaver]]: Has the same ordering bias than a top-N CMS, but at least it provides bounds on the
 *     bias.
 *
 * =Usage=
 *
 * The type `K` is the type of items you want to count. You must provide an implicit `CMSHasher[K]` for `K`,
 * and Algebird ships with several such implicits for commonly used types such as `Long` and `BigInt`.
 *
 * If your type `K` is not supported out of the box, you have two options: 1) You provide a "translation"
 * function to convert items of your (unsupported) type `K` to a supported type such as [[Double]], and then
 * use the `contramap` function of [[CMSHasher]] to create the required `CMSHasher[K]` for your type (see the
 * documentation of [[CMSHasher]] for an example); 2) You implement a `CMSHasher[K]` from scratch, using the
 * existing CMSHasher implementations as a starting point.
 *
 * Note: Because Arrays in Scala/Java not have sane `equals` and `hashCode` implementations, you cannot safely
 * use types such as `Array[Byte]`. Extra work is required for Arrays. For example, you may opt to convert
 * `Array[T]` to a `Seq[T]` via `toSeq`, or you can provide appropriate wrapper classes. Algebird provides one
 * such wrapper class, [[Bytes]], to safely wrap an `Array[Byte]` for use with CMS.
 *
 * @param cms
 *   A [[CMS]] instance, which is used for the counting and the frequency estimation performed by this class.
 * @param heavyHittersN
 *   The maximum number of heavy hitters to track.
 * @tparam K
 *   The type used to identify the elements to be counted. For example, if you want to count the occurrence of
 *   user names, you could map each username to a unique numeric ID expressed as a `Long`, and then count the
 *   occurrences of those `Long`s with a CMS of type `K=Long`. Note that this mapping between the elements of
 *   your problem domain and their identifiers used for counting via CMS should be bijective. We require a
 *   [[CMSHasher]] context bound for `K`, see [[CMSHasher]] for available implicits that can be imported.
 *   Which type K should you pick in practice? For domains that have less than `2^64` unique elements, you'd
 *   typically use `Long`. For larger domains you can try `BigInt`, for example.
 */
class TopNCMSMonoid[K](cms: CMS[K], heavyHittersN: Int = 100)
    extends TopCMSMonoid[K](cms, TopNLogic[K](heavyHittersN))

object TopNCMS {

  def monoid[K: CMSHasher](eps: Double, delta: Double, seed: Int, heavyHittersN: Int): TopNCMSMonoid[K] =
    new TopNCMSMonoid[K](CMS(eps, delta, seed), heavyHittersN)

  def monoid[K: CMSHasher](depth: Int, width: Int, seed: Int, heavyHittersN: Int): TopNCMSMonoid[K] =
    monoid(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, heavyHittersN)

  def aggregator[K: CMSHasher](
      eps: Double,
      delta: Double,
      seed: Int,
      heavyHittersN: Int
  ): TopNCMSAggregator[K] =
    new TopNCMSAggregator[K](monoid(eps, delta, seed, heavyHittersN))

  def aggregator[K: CMSHasher](depth: Int, width: Int, seed: Int, heavyHittersN: Int): TopNCMSAggregator[K] =
    aggregator(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, heavyHittersN)

}

/**
 * An Aggregator for [[TopNCMS]]. Can be created using [[TopNCMS.aggregator]].
 */
case class TopNCMSAggregator[K](cmsMonoid: TopNCMSMonoid[K]) extends TopCMSAggregator(cmsMonoid)

/**
 * K1 defines a scope for the CMS. For each k1, keep the top heavyHittersN associated k2 values.
 */
case class ScopedTopNLogic[K1, K2](heavyHittersN: Int) extends HeavyHittersLogic[(K1, K2)] {

  require(heavyHittersN > 0, "heavyHittersN must be > 0")

  override def purgeHeavyHitters(
      cms: CMS[(K1, K2)]
  )(hitters: HeavyHitters[(K1, K2)]): HeavyHitters[(K1, K2)] = {
    val grouped = hitters.hhs.groupBy(hh => hh.item._1)
    val (underLimit, overLimit) = grouped.partition {
      _._2.size <= heavyHittersN
    }
    val sorted = overLimit.transform { case (_, hhs) =>
      hhs.toSeq.sortBy(hh => hh.count)
    }
    val purged = sorted.transform { case (_, hhs) =>
      hhs.takeRight(heavyHittersN)
    }
    HeavyHitters[(K1, K2)](purged.values.flatten.toSet ++ underLimit.values.flatten.toSet)
  }

}

/*
 * Monoid for Top-N values per key in an associative [[TopCMS]].
 *
 * Typical use case for this might be (Country, City) pairs.  For a stream of such
 * pairs, we might want to keep track of the most popular cities for each country.
 *
 * This can, of course, be achieved using a Map[Country, TopNCMS[City]], but this
 * requires storing one CMS per distinct Country.
 *
 * Similarly, one could attempt to use a TopNCMS[(Country, City)], but less common
 * countries may not make the cut if N is not "very large".
 *
 * ScopedTopNCMSMonoid[Country, City] will avoid having one Country drown others
 * out, while still only using a single CMS.
 *
 * In general the eviction of K1 is not supported, and all distinct K1 values must
 * be retained.  Therefore it is important to only use this Monoid when the number
 * of distinct K1 values is known to be reasonably bounded.
 */
class ScopedTopNCMSMonoid[K1, K2](cms: CMS[(K1, K2)], heavyHittersN: Int = 100)
    extends TopCMSMonoid[(K1, K2)](cms, ScopedTopNLogic[K1, K2](heavyHittersN))

object ScopedTopNCMS {

  def scopedHasher[K1: CMSHasher, K2: CMSHasher]: CMSHasher[(K1, K2)] = new CMSHasher[(K1, K2)] {
    private val k1Hasher = implicitly[CMSHasher[K1]]
    private val k2Hasher = implicitly[CMSHasher[K2]]

    override def hash(a: Int, b: Int, width: Int)(x: (K1, K2)): Int = {
      val (k1, k2) = x
      val xs = Seq(k1Hasher.hash(a, b, width)(k1), k2Hasher.hash(a, b, width)(k2), a, b)
      (scala.util.hashing.MurmurHash3.seqHash(xs) & Int.MaxValue) % width
    }
  }

  def monoid[K1: CMSHasher, K2: CMSHasher](
      eps: Double,
      delta: Double,
      seed: Int,
      heavyHittersN: Int
  ): ScopedTopNCMSMonoid[K1, K2] =
    new ScopedTopNCMSMonoid[K1, K2](CMS(eps, delta, seed)(scopedHasher[K1, K2]), heavyHittersN)

  def monoid[K1: CMSHasher, K2: CMSHasher](
      depth: Int,
      width: Int,
      seed: Int,
      heavyHittersN: Int
  ): ScopedTopNCMSMonoid[K1, K2] =
    monoid(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, heavyHittersN)

  def aggregator[K1: CMSHasher, K2: CMSHasher](
      eps: Double,
      delta: Double,
      seed: Int,
      heavyHittersN: Int
  ): TopCMSAggregator[(K1, K2)] =
    new TopCMSAggregator(monoid(eps, delta, seed, heavyHittersN))

  def aggregator[K1: CMSHasher, K2: CMSHasher](
      depth: Int,
      width: Int,
      seed: Int,
      heavyHittersN: Int
  ): TopCMSAggregator[(K1, K2)] =
    aggregator(CMSFunctions.eps(width), CMSFunctions.delta(depth), seed, heavyHittersN)

}

case class CMSHash[K: CMSHasher](a: Int, b: Int, width: Int) extends java.io.Serializable {

  /**
   * Returns `a * x + b (mod p) (mod width)`.
   */
  def apply(x: K): Int = implicitly[CMSHasher[K]].hash(a, b, width)(x)

}

/**
 * This formerly held the instances that moved to object CMSHasher
 *
 * These instances are slow, but here for compatibility with old serialized data. For new code, avoid these
 * and instead use the implicits found in the CMSHasher companion object.
 */
object CMSHasherImplicits {

  implicit object CMSHasherBigInt extends CMSHasher[BigInt] {
    override def hash(a: Int, b: Int, width: Int)(x: BigInt): Int =
      CMSHasher.hashBytes(a, b, width)(x.toByteArray)
  }

  implicit object CMSHasherString extends CMSHasher[String] {
    override def hash(a: Int, b: Int, width: Int)(x: String): Int =
      CMSHasher.hashBytes(a, b, width)(x.getBytes("UTF-8"))
  }

  def cmsHasherShort: CMSHasher[Short] = CMSHasher.cmsHasherShort
}
