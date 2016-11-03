package com.twitter.algebird

import java.lang.{ Long => JLong }
import scala.annotation.tailrec

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
   * @param unsorted vector of buckets. All timestamps must be >= this.time.
   * @return ExpHist instance with all buckets added, stepped
   *         forward to the max timestamp in `unsorted`.
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
  /**
   * @param size number of items tracked by this bucket.
   * @param timestamp timestamp of the most recent item tracked by this bucket.
   */
  case class Bucket(size: Long, timestamp: Long)

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
    def expiration(currTime: Long): Long = currTime - windowSize

    // Drops all buckets with an expired timestamp, based on the
    // configured window and the supplied current time.
    def dropExpired(buckets: Vector[Bucket], currTime: Long): (Long, Vector[Bucket]) =
      ExpHist.dropExpired(buckets, expiration(currTime))
  }

  /**
   * Returns an empty instance with the supplied Config.
   */
  def empty(conf: Config): ExpHist = ExpHist(conf, Vector.empty, 0L, 0L)

  /**
   *  Returns an instance directly from a number `i`. All buckets in
   *  the returned ExpHist will have the same timestamp, equal to
   *  `ts`.
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
     * ## L-Canonical Representation Generation:
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
     *
     *
     * ## Implementation Discussion
     *
     * The exponential histogram algorithm tracks buckets of size
     * 2^i. Every new increment to the histogram adds a bucket of
     * size 1.
     *
     * Because only l or l+1 buckets of size 2^i are allowed for each
     * i, this increment might trigger an incremental merge of
     * smaller buckets into larger buckets.
     *
     * Let's look at 10 steps of the algorithm with l == 2:
     *
     * 1:  1 (1 added)
     * 2:  1 1 (1 added)
     * 3:  1 1 1 (1 added)
     * 4:  1 1 2 (1 added, triggering a 1 + 1 = 2 merge)
     * 5:  1 1 1 2 (1 added)
     * 6:  1 1 2 2 (1 added, triggering a 1 + 1 = 2 merge)
     * 7:  1 1 1 2 2 (1 added)
     * 8:  1 1 2 2 2 (1 added, triggering a 1 + 1 = 2 merge AND a 2 + 2 = 4 merge)
     * 9:  1 1 1 2 2 2 (1 added)
     * 10: 1 1 2 2 4 (1 added, triggering a 1 + 1 = 2 merge AND a 2 + 2 = 4 merge)
     *
     * Notice that the bucket sizes always sum to the algorithm step,
     * ie (10 == 1 + 1 + 1 + 2 + 2 + 4).
     *
     * Now let's write out a list of the number of buckets of each size, ie
     * [bucketsOfSize(1), bucketsOfSize(2), bucketsOfSize(4), ....]
     *
     * Here's the above sequence in the new representation, plus a
     * few more steps:
     *
     * 1:  1     <-- (l + 1)2^0 - l = 3 * 2^0 - 2 = 1
     * 2:  2
     * 3:  3
     * 4:  2 1   <-- (l + 1)2^1 - l = 3 * 2^0 - 2 = 4
     * 5:  3 1
     * 6:  2 2
     * 7:  3 2
     * 8:  2 3
     * 9:  3 3
     * 10: 2 2 1 <-- (l + 1)2^2 - l = 3 * 2^0 - 2 = 10
     * 11: 3 2 1
     * 12: 2 3 1
     * 13: 3 3 1
     * 14: 2 2 2
     * 15: 3 2 2
     * 16: 2 3 2
     * 16: 3 3 2
     * 17: 2 2 3
     *
     * This sequence is called the "l-canonical representation" of s.
     *
     * A pattern emerges! Every bucket size except the largest looks
     * like a binary counter... if you added `l + 1` to the bit, and
     * made the counter little-endian, so the least-significant bits
     * came first. Let's call this the "binary" prefix, or "bin(_)".
     *
     * Here's the above sequence with the prefix decoded from
     * "binary":
     *
     * 1:  1        <-- (l + 1)2^0 - l = 3 * 2^0 - 2 = 1
     * 2:  2
     * 3:  3
     *
     * 4:  bin(0) 1 <-- (l + 1)2^1 - l = 3 * 2^0 - 2 = 4
     * 5:  bin(1) 1
     * 6:  bin(0) 2
     * 7:  bin(1) 2
     * 8:  bin(0) 3
     * 9:  bin(1) 3
     *
     * 10: bin(0) 1 <-- (l + 1)2^2 - l = 3 * 2^0 - 2 = 10
     * 11: bin(1) 1
     * 12: bin(2) 1
     * 13: bin(3) 1
     * 14: bin(0) 2
     * 15: bin(1) 2
     * 16: bin(2) 2
     * 16: bin(3) 2
     * 17: bin(0) 3
     *
     * Some observations about the pattern:
     *
     * The l-canonical representation groups the natural numbers into
     * groups of size (l + 1)2^i for i >= 0.
     *
     * Each group starts at (l + 1)2^i - l (see 1, 4, 10... above)
     *
     * Within each group, the "binary" prefix of the l-canonical rep
     * cycles from 0 to (2^i - 1), l + 1 total times. (This makes
     * sense; each cycle increments the final entry by one until it
     * hits l + 1; after that an increment triggers a merge and a new
     * "group" begins.)
     *
     * The final l-canonical entry ==
     *
     * floor((position within the group) / 2^i), or the "quotient" of
     * that position and 2^i.
     *
     * That's all we need to know to write a procedure to generate
     * the l-canonical representation! Here it is again:
     *
     *
     * ## L-Canonical Representation Procedure:
     *
     * - Find the largest j s.t. 2^j <= (s + l) / (1 + l)
     * - let s' = 2^j(1 + l) - l
     *
     * (s' is the position if the start of a group, ie 1, 4, 10...)
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
