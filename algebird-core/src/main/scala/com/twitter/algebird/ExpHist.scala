package com.twitter.algebird

import java.lang.{ Long => JLong }
import scala.annotation.tailrec

/**
 * Exponential Histogram algorithm from
 * http://www-cs-students.stanford.edu/~datar/papers/sicomp_streams.pdf
 *
 * An Exponential Histogram is a sliding window counter that can
 * guarantee a bounded relative error. You configure the data structure with
 *
 * - epsilon, the relative error you're willing to tolerate
 * - windowSize, the number of time ticks that you want to track
 *
 * You interact with the data structure by adding (number, timestamp)
 * pairs into the exponential histogram, and querying it for
 * approximate counts.
 *
 * The approximate count is guaranteed to be within conf.epsilon
 * relative error of the true count seen across the supplied
 * `windowSize`.
 *
 * Next steps:
 *
 * - combine Canonical.fromLong and Canonical.bucketsFromLong
 * - efficient serialization using Canonical
 * - Query EH with a shorter window than the configured window
 * - Discussion of epsilon vs memory tradeoffs
 * - Discussion of how Canonical.fromLong works
 *
 *
 * @param conf the config values for this isntance.
 * @param buckets Vector of timestamps of each (powers of 2)
 *                ticks. This is the core of the exponential
 *                histogram representation. See [[Canonical]] for
 *                more info.
 * @param total total ticks tracked, == buckets.map(_.size).sum
 * @param time current timestamp of this instance.
 *             Used with conf.windowSize to expire buckets.
 */
case class ExpHist(conf: ExpHist.Config, buckets: Vector[ExpHist.Bucket], total: Long, time: Long) {
  import ExpHist.{ Bucket, Canonical }

  /**
   * Steps this instance forward to the new supplied time. Any
   * buckets with a timestamp <= (newTime - conf.windowSize) will be
   * evicted.
   *
   * @param newTime the new current time.
   * @return ExpHist instance stepped forward to newTime.
   */
  def step(newTime: Long): ExpHist =
    if (newTime <= time) this
    else {
      val (dropped, filtered) = conf.dropExpired(buckets, newTime)
      copy(time = newTime, buckets = filtered, total = total - dropped)
    }

  /**
   * Increment ExpHist by 1 at the supplied timestamp.
   */
  def inc(timestamp: Long): ExpHist = add(1L, timestamp)

  /**
   * Increment ExpHist by delta at the supplied timestamp.
   */
  def add(delta: Long, timestamp: Long): ExpHist = {
    val self = step(timestamp)
    if (delta == 0) self
    else self.addAllWithoutStep(Vector(Bucket(delta, timestamp)), delta)
  }

  /**
   * Efficiently add many buckets at once.
   *
   * @param unsorted [bucket]. All timestamps must be >= this.time.
   * @return ExpHist instance with all buckets added, stepped
   *         forward to the most timestamp in `unsorted`.
   */
  def addAll(unsorted: Vector[Bucket]): ExpHist =
    if (unsorted.isEmpty) this
    else {
      val sorted = unsorted.sorted(Ordering[Bucket].reverse)
      val delta = sorted.map(_.size).sum
      val timestamp = sorted.head.timestamp
      if (delta == 0)
        step(timestamp)
      else {
        addAllWithoutStep(sorted, delta).step(timestamp)
      }
    }

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
  case class Bucket(size: Long, timestamp: Long)

  object Bucket {
    implicit val ord: Ordering[Bucket] = Ordering.by { b: Bucket => (b.timestamp, b.size) }
  }

  /**
   * ExpHist guarantees that the returned guess will be within
   * `epsilon` relative error of the true count across a sliding
   * window of size `windowSize`.
   */
  case class Config(epsilon: Double, windowSize: Long) {
    val k: Int = math.ceil(1 / epsilon).toInt
    val l: Int = math.ceil(k / 2.0).toInt

    // Returns the last timestamp before the window. any ts <= [the
    // returned timestamp] is outside the window.
    def expiration(currTime: Long): Long = currTime - windowSize

    // Drops all buckets with an expired timestamp, based on the
    // configured window and the supplied current time.
    def dropExpired(buckets: Vector[Bucket], currTime: Long): (Long, Vector[Bucket]) =
      ExpHist.dropExpired(buckets, expiration(currTime))
  }

  /**
   * Create an empty instance with the supplied Config.
   */
  def empty(conf: Config): ExpHist = ExpHist(conf, Vector.empty, 0L, 0L)

  /**
   *  Generate an instance directly from a number. All buckets in the
   *  returned ExpHist will have the same timestamp, equal to `ts`.
   */
  def from(i: Long, ts: Long, conf: Config): ExpHist = {
    val buckets = Canonical.bucketsFromLong(i, conf.l).map(Bucket(_, ts))
    ExpHist(conf, buckets, i, ts)
  }

  /**
   * @param buckets [buckets] sorted in DESCENDING order (recent first)
   * @param cutoff buckets with ts <= cutoff are expired
   * @return the sum of evicted bucket sizes and the unexpired buckets
   */
  def dropExpired(buckets: Vector[Bucket], cutoff: Long): (Long, Vector[Bucket]) = {
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
   * @param buckets [buckets] sorted in DESCENDING order (recent first)
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
   * given a positive number `l`, every integer can be uniquely
   * represented as the sum of
   *
   * (l or (l + 1)) * 2^i + (# from 1 to (l + 1)) 2^j
   *
   * for i = (0 to j - 1), given some j.
   *
   * The paper calls this the "l-canonical" representation of the
   * number.
   *
   * It turns out that if you follow the exponential histogram
   * bucket-merging algorithm, you end up with the invariant that the
   * number of buckets with size 2^i exactly matches that power of 2's
   * coefficient in its l-canonical representation.
   *
   * Put another way - only sequences of buckets with sizes matching
   * the l-canonical representation are valid exponential histograms.
   *
   * (We use this idea in `ExpHist.rebucket` to take a sequence of
   * buckets of any size and rebucket them into a sequence where the
   * above invariant holds.)
   *
   * This is huge. This means that you can implement `addAll(newBuckets)` by
   *
   * - calculating newTotal = total + delta contributed by newBuckets
   * - generating the l-canonical sequence of bucket sizes for newTotal
   * - rebucketing newBuckets ++ oldBuckets into those bucket sizes
   *
   * The resulting sequence of buckets is a valid exponential
   * histogram.
   */

  object Canonical {
    @inline private[this] def floorPowerOfTwo(x: Long): Int =
      JLong.numberOfTrailingZeros(JLong.highestOneBit(x))

    @inline private[this] def modPow2(i: Int, exp2: Int): Int = i & ((1 << exp2) - 1)
    @inline private[this] def quotient(i: Int, exp2: Int): Int = i >>> exp2
    @inline private[this] def bit(i: Int, idx: Int): Int = (i >>> idx) & 1

    private[this] def binarize(i: Int, bits: Int, offset: Int): Vector[Int] =
      (0 until bits).map { idx => offset + bit(i, idx) }.toVector

    /**
     * @param s the number to convert to l-canonical form
     * @param l the "l" in l-canonical form
     * @return vector of the coefficients of 2^i in the
     *         l-canonical representation of s.
     *
     * the "l" in l-canonical means that
     *
     *  - all return vector entries but the last one == `l` or `l + 1`
     *  - 1 <= `returnVector.last` <= l + 1
     *
     * The return vector's size is the largest k such that
     *
     * 2^k <= (s + l) / (l + 1)
     *
     * For example:
     *
     * {{{
     * scala> Canonical.fromLong(15, 2)
     * res0: Vector[Int] = Vector(3, 2, 2)
     * }}}
     * 15 = (3 * 2^0) + (2 * 2^1) + (2 * 2^2)
     */
    def fromLong(s: Long, l: Int): CanonicalVector =
      if (s <= 0) CanonicalVector(Vector.empty)
      else {
        val num = s + l
        val denom = l + 1
        val j = floorPowerOfTwo(num / denom)
        val offset = (num - (denom << j)).toInt
        CanonicalVector(
          binarize(modPow2(offset, j), j, l) :+ (quotient(offset, j) + 1))
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
        val prefixRep = modPow2(offset, j)

        (0 until j).toVector.flatMap {
          idx => Vector.fill(l + bit(prefixRep, idx))(1L << idx)
        } ++ List.fill(quotient(offset, j) + 1)(1L << j)
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
      if (rep.isEmpty) 0L
      else {
        rep.iterator.zipWithIndex
          .map { case (i, exp) => i.toLong << exp }
          .reduce(_ + _)
      }

    /**
     * Expands out the l-canonical representation of some number s into
     * a list of bucket sizes in ascending order.
     *
     * @param rep l-canonical representation of some number s for some l
     * @return vector of powers of 2 (where ret.sum == the original s)
     */
    def toBuckets: Vector[Long] =
      rep.zipWithIndex.flatMap { case (i, exp) => List.fill(i)(1L << exp) }
  }
}
