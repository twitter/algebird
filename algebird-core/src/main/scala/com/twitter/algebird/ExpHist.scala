package com.twitter.algebird

import java.lang.{ Long => JLong }
import scala.annotation.tailrec
import scala.collection.mutable.Builder

/**
 * Exponential Histogram algorithm from
 * http://www-cs-students.stanford.edu/~datar/papers/sicomp_streams.pdf
 *
 * An Exponential Histogram is a sliding window counter that can
 * guarantee a bounded relative error. You configure the data
 * structure with
 *
 * - epsilon, the relative error you're willing to tolerate
 * - windowSize, the number of time ticks that you want to track
 *
 * You interact with the data structure by adding (number, timestamp)
 * pairs into the exponential histogram. querying it for an
 * approximate counts with `guess`.
 *
 * The approximate count is guaranteed to be within conf.epsilon
 * relative error of the true count seen across the supplied
 * `windowSize`.
 *
 * Next steps:
 *
 * - efficient serialization
 * - Query EH with a shorter window than the configured window
 * - Discussion of epsilon vs memory tradeoffs
 *
 *
 * @param conf the config values for this instance.
 * @param buckets Vector of timestamps of each (powers of 2)
 *                ticks. This is the key to the exponential histogram
 *                representation. See [[ExpHist.Canonical]] for more
 *                info.
 * @param total total ticks tracked. `total == buckets.map(_.size).sum`
 * @param time current timestamp of this instance.
 */
case class ExpHist(conf: ExpHist.Config, buckets: Vector[ExpHist.Bucket], total: Long, time: ExpHist.Timestamp) {
  import ExpHist.{ Bucket, Canonical, Timestamp }

  /**
   * Steps this instance forward to the new supplied time. Any
   * buckets with a timestamp <= (newTime - conf.windowSize) will be
   * evicted.
   *
   * @param newTime the new current time.
   * @return ExpHist instance stepped forward to newTime.
   */
  def step(newTime: Timestamp): ExpHist =
    if (newTime <= time) this
    else {
      val (dropped, filtered) = conf.dropExpired(buckets, newTime)
      copy(time = newTime, buckets = filtered, total = total - dropped)
    }

  /**
   * Increment ExpHist by 1 at the supplied timestamp.
   */
  def inc(ts: Timestamp): ExpHist = add(1L, ts)

  /**
   * Increment ExpHist by delta at the supplied timestamp.
   */
  def add(delta: Long, ts: Timestamp): ExpHist = {
    val self = step(ts)
    if (delta == 0) self
    else self.addAllWithoutStep(Vector(Bucket(delta, ts)), delta)
  }

  /**
   * Efficiently add many buckets at once.
   *
   * @param unsorted vector of buckets. All timestamps must be >= this.time.
   * @return ExpHist instance with all buckets added, stepped
   *         forward to the max timestamp in `unsorted`.
   */
  def addAll(unsorted: Vector[Bucket]): ExpHist =
    if (unsorted.isEmpty) this
    else {
      val delta = unsorted.map(_.size).sum

      if (delta == 0) {
        step(unsorted.maxBy(_.timestamp).timestamp)
      } else {
        val sorted = unsorted.sorted(Ordering[Bucket].reverse)
        val timestamp = sorted.head.timestamp
        addAllWithoutStep(sorted, delta).step(timestamp)
      }
    }

  /**
   * Returns a [[Fold]] instance that uses `add` to accumulate deltas
   * into this exponential histogram instance.
   */
  def fold: Fold[Bucket, ExpHist] =
    Fold.foldMutable[Builder[Bucket, Vector[Bucket]], Bucket, ExpHist](
      { case (b, bucket) => b += bucket },
      { _ => Vector.newBuilder[Bucket] },
      { x => addAll(x.result) })

  // This internal method assumes that the instance is stepped forward
  // already, and does NOT try to step internally. It also assumes
  // that `items` is sorted in ASCENDING order, with newer items on
  // the right side, and that `items.map(_.size).sum == delta`.
  private[ExpHist] def addAllWithoutStep(items: Vector[Bucket], delta: Long): ExpHist = {
    val inputs = items ++ buckets
    val desiredBuckets = Canonical.bucketsFromLong(total + delta, conf.l)
    copy(
      buckets = ExpHist.rebucket(inputs, desiredBuckets),
      total = total + delta)
  }

  def oldestBucketSize: Long = if (total == 0) 0L else buckets.last.size

  /**
   * Smallest possible count seen in the last conf.windowSize
   * timestamps.
   */
  def lowerBoundSum: Long = total - oldestBucketSize

  /**
   * Largest possible count seen in the last conf.windowSize
   * timestamps.
   */
  def upperBoundSum: Long = total

  /**
   * Estimate of the count seen across the last conf.windowSize
   * timestamps. Guaranteed to be within conf.epsilon of the true
   * count.
   */
  def guess: Double =
    if (total == 0) 0.0
    else (total - (oldestBucketSize - 1) / 2.0)

  /**
   * Returns an Approximate instance encoding the bounds and the
   * closest long to the estimated sum tracked by this instance.
   */
  def approximateSum: Approximate[Long] =
    Approximate(lowerBoundSum, math.round(guess), upperBoundSum, 1.0)

  /**
   * relative error of guess, guaranteed to be <= conf.epsilon.
   */
  def relativeError: Double =
    if (total == 0) 0.0
    else {
      val minInsideWindow = total + 1 - oldestBucketSize
      val absoluteError = (oldestBucketSize - 1) / 2.0
      absoluteError / minInsideWindow
    }
}

object ExpHist {
  /**
   * Value class wrapper around timestamps (>= 0) used by each bucket.
   */
  case class Timestamp(toLong: Long) extends AnyVal {
    def <=(r: Timestamp): Boolean = toLong <= r.toLong
    def >(r: Timestamp): Boolean = toLong > r.toLong
  }
  object Timestamp {
    implicit val ord: Ordering[Timestamp] = Ordering.by(_.toLong)
  }

  /**
   * @param size number of items tracked by this bucket.
   * @param timestamp timestamp of the most recent item tracked by this bucket.
   */
  case class Bucket(size: Long, timestamp: Timestamp)

  object Bucket {
    implicit val ord: Ordering[Bucket] = Ordering.by { b: Bucket => (b.timestamp, b.size) }
  }

  /**
   * ExpHist guarantees that the returned guess will be within
   * `epsilon` relative error of the true count across a sliding
   * window of size `windowSize`.
   * @param epsilon relative error, from [0, 0.5]
   * @param windowSize number of time ticks to track
   */
  case class Config(epsilon: Double, windowSize: Long) {
    val k: Int = math.ceil(1 / epsilon).toInt
    val l: Int = math.ceil(k / 2.0).toInt

    // Returns the last timestamp before the window. any ts <= [the
    // returned timestamp] is outside the window.
    def expiration(currTime: Timestamp): Timestamp = Timestamp(currTime.toLong - windowSize)

    // Drops all buckets with an expired timestamp, based on the
    // configured window and the supplied current time.
    def dropExpired(buckets: Vector[Bucket], currTime: Timestamp): (Long, Vector[Bucket]) =
      ExpHist.dropExpired(buckets, expiration(currTime))

    /**
     * Returns a [[Fold]] instance that uses `add` to accumulate deltas
     * into an empty exponential histogram instance configured with
     * this Config.
     */
    def fold: Fold[Bucket, ExpHist] = ExpHist.empty(this).fold
  }

  /**
   * Returns an empty instance with the supplied Config.
   */
  def empty(conf: Config): ExpHist = ExpHist(conf, Vector.empty, 0L, Timestamp(0L))

  /**
   *  Returns an instance directly from a number `i`. All buckets in
   *  the returned ExpHist will have the same timestamp, equal to
   *  `ts`.
   */
  def from(i: Long, ts: Timestamp, conf: Config): ExpHist = {
    val buckets = Canonical.bucketsFromLong(i, conf.l).map(Bucket(_, ts))
    ExpHist(conf, buckets, i, ts)
  }

  /**
   * @param buckets [buckets] sorted in DESCENDING order (recent first)
   * @param cutoff buckets with ts <= cutoff are expired
   * @return the sum of evicted bucket sizes and the unexpired buckets
   */
  def dropExpired(buckets: Vector[Bucket], cutoff: Timestamp): (Long, Vector[Bucket]) = {
    val (dropped, remaining) = buckets.reverse.span(_.timestamp <= cutoff)
    (dropped.map(_.size).sum, remaining.reverse)
  }

  /**
   * Converts the supplied buckets into a NEW vector of buckets
   * satisfying this law:
   *
   * {{{
   * rebucket(buckets, desired).map(_.size).sum == desired
   * }}}
   *
   * (rebucket only works if desired.sum == buckets.map(_.size).sum)
   *
   * @param buckets vector of buckets sorted in DESCENDING order (recent first)
   * @param desired bucket sizes to rebucket `buckets` into.
   */
  private[algebird] def rebucket(buckets: Vector[Bucket], desired: Vector[Long]): Vector[Bucket] =
    if (desired.isEmpty) Vector.empty
    else {
      val input = buckets.dropWhile(_.size == 0)
      val bucketSize +: tail = desired
      val remaining = drop(bucketSize, input)
      input.head.copy(size = bucketSize) +: rebucket(remaining, tail)
    }

  /**
   * @param toDrop total count to remove from the left of `input`.
   * @param input buckets
   * @return Vector with buckets, or pieces of buckets, with sizes
   *         totalling `toDrop` items removed from the head. If an
   *         element wasn't fully consumed, the remainder will be
   *         stuck back onto the head.
   */
  @tailrec private[this] def drop(toDrop: Long, input: Vector[Bucket]): Vector[Bucket] = {
    val (b @ Bucket(count, _)) +: tail = input
    (toDrop - count) match {
      case 0 => tail
      case x if x < 0 => b.copy(size = -x) +: tail
      case x if x > 0 => drop(x, tail)
    }
  }

  /**
   * The paper that introduces the exponential histogram proves that,
   * given a positive number `l`, every integer s can be uniquely
   * represented as the sum of
   *
   * (l or (l + 1)) * 2^i + (# from 1 to (l + 1)) 2^j
   *
   * for i = (0 to j - 1), given some j.
   *
   * The paper calls this the "l-canonical" representation of s.
   *
   * It turns out that if you follow the exponential histogram
   * bucket-merging algorithm, you end up with the invariant that the
   * number of buckets with size 2^i exactly matches that power of 2's
   * coefficient in s's l-canonical representation.
   *
   * Put another way - only sequences of buckets with sizes matching
   * the l-canonical representation of some number s are valid
   * exponential histograms.
   *
   * (We use this idea in `ExpHist.rebucket` to take a sequence of
   * buckets of any size and rebucket them into a sequence where the
   * above invariant holds.)
   *
   * This is huge. This means that you can implement `addAll(newBuckets)` by
   *
   * - calculating newS = s + delta contributed by newBuckets
   * - generating the l-canonical sequence of bucket sizes for newS
   * - rebucketing newBuckets ++ oldBuckets into those bucket sizes
   *
   * The resulting sequence of buckets is a valid exponential
   * histogram.
   */
  object Canonical {
    @inline private[this] def floorPowerOfTwo(x: Long): Int =
      JLong.numberOfTrailingZeros(JLong.highestOneBit(x))

    @inline private[this] def modPow2Minus1(i: Int, exp2: Int): Int = i & ((1 << exp2) - 1)
    @inline private[this] def quotientPow2(i: Int, exp2: Int): Int = i >>> exp2
    @inline private[this] def bit(i: Int, idx: Int): Int = (i >>> idx) & 1

    private[this] def binarize(i: Int, bits: Int, offset: Int): Vector[Int] =
      (0 until bits).map { idx => offset + bit(i, idx) }.toVector

    /**
     * @param s the number to convert to l-canonical form
     * @param l the "l" in l-canonical form
     * @return vector of the coefficients of 2^i in the
     *         l-canonical representation of s.
     *
     * For example:
     *
     * {{{
     * scala> Canonical.fromLong(15, 2)
     * res0: Vector[Int] = Vector(3, 2, 2)
     * }}}
     * 15 = (3 * 2^0) + (2 * 2^1) + (2 * 2^2)
     *
     *
     * the "l" in l-canonical means that
     *
     *  - all return vector entries but the last one == `l` or `l + 1`
     *  - 1 <= `returnVector.last` <= l + 1
     *
     * ## L-Canonical Representation Procedure:
     *
     * - Find the largest j s.t. 2^j <= (s + l) / (1 + l)
     * - let s' = 2^j(1 + l) - l
     *
     * - let diff = (s - s') is the position of s within that group.
     * - let b = the little-endian binary rep of diff % (2^j - 1)
     * - let ret = return vector of length j:
     *
     * {{{
     * (0 until j).map { i => ret(i) = b(i) + l }
     * ret(j) = math.floor(diff / 2^j)
     * }}}
     */
    def fromLong(s: Long, l: Int): CanonicalVector =
      if (s <= 0) CanonicalVector(Vector.empty)
      else {
        val num = s + l
        val denom = l + 1
        val j = floorPowerOfTwo(num / denom)
        val offset = (num - (denom << j)).toInt
        CanonicalVector(
          binarize(modPow2Minus1(offset, j), j, l) :+ (quotientPow2(offset, j) + 1))
      }

    /**
     * @param s the number to convert to l-canonical form
     * @param l the "l" in l-canonical form
     * @return vector of numbers that sum to s. Each
     *         entry is a power of 2, and the number of entries of
     *         each power of 2 matches the l-canonical
     *         representation of s.
     *
     * Note that:
     *
     * {{{
     * bucketsFromLong(s, l) == fromLong(s, k).toBuckets
     * }}}
     *
     * bucketsFromLong is more efficient.
     */
    def bucketsFromLong(s: Long, l: Int): Vector[Long] =
      if (s <= 0) Vector.empty
      else {
        val num = s + l
        val denom = l + 1
        val j = floorPowerOfTwo(num / denom)
        val offset = (num - (denom << j)).toInt
        val prefixRep = modPow2Minus1(offset, j)

        (0 until j).toVector.flatMap {
          idx => Vector.fill(l + bit(prefixRep, idx))(1L << idx)
        } ++ List.fill(quotientPow2(offset, j) + 1)(1L << j)
      }
  }

  case class CanonicalVector(rep: Vector[Int]) extends AnyVal {
    def sum: Int = rep.sum

    /**
     * Expands out an l-canonical representation into the original number.
     *
     * @param rep l-canonical representation of some number s for some l
     * @return The original s
     */
    def toLong: Long =
      Monoid.sum(
        rep.iterator.zipWithIndex
          .map { case (i, exp) => i.toLong << exp })

    /**
     * Expands out the l-canonical representation of some number s into
     * a list of bucket sizes in ascending order.
     *
     * @param rep l-canonical representation of some number s for some l
     * @return vector of powers of 2 (where ret.sum == the original s)
     */
    def toBuckets: Vector[Long] =
      rep.iterator
        .zipWithIndex
        .flatMap { case (i, exp) => Iterator.fill(i)(1L << exp) }
        .toVector
  }
}
