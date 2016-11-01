package com.twitter.algebird

import java.lang.{ Long => JLong }
import scala.annotation.tailrec

/**
 * Exponential Histogram algorithm from
 * http://ilpubs.stanford.edu:8090/504/1/2001-34.pdf.
 */
object ExpHist {
  case class Bucket(size: Long, timestamp: Long)

  object Bucket {
    implicit val ord: Ordering[Bucket] = Ordering.by { b: Bucket => (b.timestamp, b.size) }
  }

  case class Config(epsilon: Double, windowSize: Long) {
    val k: Int = math.ceil(1 / epsilon).toInt
    val l: Int = math.ceil(k / 2.0).toInt

    // Returns the last timestamp before the window. any ts <= [the
    // returned timestamp] is outside the window.
    def expiration(currTime: Long): Long = currTime - windowSize
    def dropExpired(buckets: Vector[Bucket], currTime: Long): (Long, Vector[Bucket]) =
      ExpHist.dropExpired(buckets, expiration(currTime))
  }

  /**
   * Create an empty instance with the supplied Config.
   */
  def empty(conf: Config): ExpHist = ExpHist(conf, Vector.empty, 0L, 0L)

  /**
   *  Create an instance from a number.
   */
  def from(i: Long, ts: Long, conf: Config): ExpHist = {
    val buckets = Canonical.longToBuckets(i, conf.l).map(Bucket(_, ts))
    ExpHist(conf, buckets, i, ts)
  }

  /**
   * Takes a vector of buckets sorted in descending order (newest
   * first) and a cutoff and returns
   *
   * - The sum of the sizes of the dropped buckets
   * - a vector containing only buckets with timestamps AFTER the cutoff.
   */
  def dropExpired(timestamps: Vector[Bucket], cutoff: Long): (Long, Vector[Bucket]) = {
    val (dropped, remaining) = timestamps.reverse.span(_.timestamp <= cutoff)
    (dropped.map(_.size).sum, remaining.reverse)
  }

  /**
   * Rebuckets the vector of inputs into buckets of the supplied
   * sequence of sizes. Returns only the associated `T` information
   * (timestamp, for example).
   */
  def rebucket(buckets: Vector[Bucket], desired: Vector[Long]): Vector[Bucket] =
    if (desired.isEmpty) Vector.empty
    else {
      val bucketSize +: tail = desired
      val remaining = drop(bucketSize, buckets)
      buckets.head.copy(size = bucketSize) +: rebucket(remaining, tail)
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
}

case class ExpHist(conf: ExpHist.Config, buckets: Vector[ExpHist.Bucket], total: Long, time: Long) {
  import ExpHist.Bucket

  def step(newTime: Long): ExpHist =
    if (newTime <= time) this
    else {
      val (dropped, filtered) = conf.dropExpired(buckets, newTime)
      copy(time = newTime, buckets = filtered, total = total - dropped)
    }

  def inc(timestamp: Long): ExpHist = add(1L, timestamp)

  def add(delta: Long, timestamp: Long): ExpHist = {
    val self = step(timestamp)
    if (delta == 0) self
    else self.addAllWithoutStep(Vector(Bucket(delta, timestamp)), delta)
  }

  def addAll(unsorted: Vector[Bucket]): ExpHist =
    if (unsorted.isEmpty) this
    else {
      val sorted = unsorted.sorted(Ordering[Bucket].reverse)
      val delta = sorted.map(_.size).sum
      val timestamp = sorted.head.timestamp
      if (delta == 0)
        step(timestamp)
      else
        addAllWithoutStep(sorted, delta).step(timestamp)
    }

  // Note that this internal method assumes that the instance is
  // stepped forward already, and does NOT try to step internally. It
  // also assumes that `items` is sorted in ASCENDING order, with
  // newer items on the right side.
  // private[ExpHist]
  def addAllWithoutStep(items: Vector[Bucket], delta: Long): ExpHist = {
    val inputs = items ++ buckets
    val desiredBuckets = Canonical.longToBuckets(total + delta, conf.l)
    copy(
      buckets = ExpHist.rebucket(inputs, desiredBuckets),
      total = total + delta)
  }

  def oldestBucketSize: Long = if (buckets.isEmpty) 0L else buckets.last.size

  def lowerBoundSum: Long = total - oldestBucketSize

  def upperBoundSum: Long = total

  def guess: Double = total - (oldestBucketSize - 1) / 2.0

  def relativeError: Double =
    if (total == 0) 0.0
    else {
      val minInsideWindow = total + 1 - oldestBucketSize
      val absoluteError = (oldestBucketSize - 1) / 2.0
      absoluteError / minInsideWindow
    }
}

object Canonical {
  @inline private[this] def floorPowerOfTwo(x: Long): Int =
    JLong.numberOfTrailingZeros(JLong.highestOneBit(x))

  @inline private[this] def modPow2(i: Int, exp2: Int): Int = i & ((1 << exp2) - 1)
  @inline private[this] def quotient(i: Int, exp2: Int): Int = i >>> exp2
  @inline private[this] def bit(i: Int, idx: Int): Int = (i >>> idx) & 1

  private[this] def binarize(i: Int, bits: Int, offset: Int): Vector[Int] =
    (0 until bits).map { idx => offset + bit(i, idx) }.toVector

  /**
   * returns a vector of the total number of buckets of size `s^i`,
   * where `i` is the vector index, used to represent s for a given
   * k.
   *
   * `s` is the sum of the sizes of all of the buckets tracked by
   * the ExpHist instance.
   *
   * `l` or `l + 1` buckets allowed per size.
   */
  def fromLong(s: Long, l: Int): Vector[Int] = {
    if (s <= 0) Vector.empty
    else {
      val num = s + l
      val denom = l + 1
      val j = floorPowerOfTwo(num / denom)
      val offset = (num - (denom << j)).toInt
      binarize(modPow2(offset, j), j, l) :+ (quotient(offset, j) + 1)
    }
  }

  /**
   * Expand out a number's l-canonical form into the original number.
   */
  def toLong(rep: Vector[Int]): Long =
    if (rep.isEmpty) 0L
    else {
      rep.iterator.zipWithIndex
        .map { case (i, exp) => i.toLong << exp }
        .reduce(_ + _)
    }

  /**
   * Expands out the compressed form to a list of the bucket sizes in
   * ascending order.
   */
  def toBuckets(rep: Vector[Int]): Vector[Long] =
    rep.zipWithIndex.flatMap { case (i, exp) => List.fill(i)(1L << exp) }

  /**
   * Returns a sequence of (ascending-order) bucket sizes required to
   * store the supplied `s` in l-canonical form.
   */
  def longToBuckets(s: Long, l: Int): Vector[Long] = toBuckets(fromLong(s, l))
}
