package com.twitter.algebird

import java.lang.{ Long => JLong }
import scala.annotation.tailrec

/**
 * Exponential Histogram algorithm from
 * http://ilpubs.stanford.edu:8090/504/1/2001-34.pdf.
 */
object ExpHist {
  import Canonical.Exp2

  // Same as math.ceil(x / 2.0).toInt
  def div2Ceil(x: Int): Int = (x >> 1) + (if ((x & 1) == 0) 0 else 1)

  case class Config(epsilon: Double, windowSize: Long) {
    val l: Int = div2Ceil(math.ceil(1 / epsilon).toInt)

    // Returns the last timestamp before the window. any ts <= [the
    // returned timestamp] is outside the window.
    def expiration(currTime: Long): Long = currTime - windowSize
  }

  def empty(conf: Config): ExpHist = ExpHist(conf, Vector.empty, Vector.empty, 0L)

  def from(i: Long, ts: Long, conf: Config): ExpHist = {
    val sizes = Canonical.fromLong(i, conf.l)
    ExpHist(conf, Vector.fill(sizes.sum)(ts), sizes, ts)
  }

  /**
   * Takes a vector of timestamps sorted in descending order (newest
   * first) and a cutoff and returns a vector containing only
   * timestamps AFTER the cutoff.
   */
  def dropExpired(timestamps: Vector[Long], cutoff: Long): Vector[Long] =
    timestamps.reverse.dropWhile(_ <= cutoff).reverse

  /**
   * @param x total count to remove from the head of `input`.
   * @param input pairs of (count, T).
   * @return Vector with x total items removed from the head; if
   *         an element wasn't fully consumed, the remainder will be
   *         stuck back onto the head.
   */
  @tailrec private[this] def take[T](x: Long, input: Vector[(Long, T)]): Vector[(Long, T)] = {
    val (count, t) +: tail = input
    (x - count) match {
      case 0 => tail
      case x if x < 0 => (-x, t) +: tail
      case x if x > 0 => take(x, tail)
    }
  }

  @tailrec def take2[T](x: Long, input: Vector[T])(from: T => Long)(to: (Long, T) => T): Vector[T] = {
    val head +: tail = input
    val count = from(head)
    (x - count) match {
      case 0 => tail
      case x if x < 0 => to(-x, head) +: tail
      case x if x > 0 => take2(x, tail)(from)(to)
    }
  }

  /**
   * Rebuckets the vector of inputs into buckets of the supplied
   * sequence of sizes. Returns only the associated `T` information
   * (timestamp, for example).
   */
  def rebucket[T](v: Vector[(Long, T)], buckets: Vector[Exp2]): Vector[T] = {
    if (buckets.isEmpty) Vector.empty
    else {
      val exp2 +: tail = buckets
      val remaining = take(exp2.value, v) // (_._1) { (l, pair) => (l + pair._1, pair._2) }
      v.head._2 +: rebucket(remaining, tail)
    }
  }
}

// TODO: Interesting that relative error only depends on sizes, not on
// timestamps. You could totally get a relative error off of some
// number without ever going through this business, then use that to
// decide when to evict stuff from the bucket.
case class ExpHist(conf: ExpHist.Config, timestamps: Vector[Long], sizes: Vector[Int], time: Long) {
  def step(newTime: Long): ExpHist =
    if (newTime <= time) this
    else {
      val filtered = ExpHist.dropExpired(timestamps, conf.expiration(newTime))
      val bucketsDropped = timestamps.size - filtered.size
      copy(
        time = newTime,
        timestamps = filtered,
        sizes = Canonical.dropBiggest(sizes, bucketsDropped))
    }

  // Efficient implementation of add. To make this solid we'll want to
  // keep a buffer of new items and only add when the buffer expires.
  def add(i: Long, timestamp: Long): ExpHist = {
    val self = step(timestamp)
    if (i == 0) self
    else {
      val newSizes = Canonical.fromLong(self.total + i, conf.l)
      val bucketSizes = Canonical.toBuckets(self.sizes).map(_.value)
      val inputs = (i, timestamp) +: (bucketSizes zip self.timestamps)
      self.copy(
        timestamps = ExpHist.rebucket(inputs, Canonical.toBuckets(newSizes)),
        sizes = newSizes)
    }
  }

  def inc(timestamp: Long): ExpHist = add(1L, timestamp)

  def last: Long = 1 << (sizes.size - 1)

  def total: Long = Canonical.expand(sizes)

  def lowerBoundSum: Long = total - last

  def upperBoundSum: Long = total

  def guess: Double = total - (last - 1) / 2.0

  def relativeError: Double =
    if (total == 0) 0.0
    else {
      val minInsideWindow = total + 1 - last
      val absoluteError = (last - 1) / 2.0
      absoluteError / minInsideWindow
    }

  /**
   * Returns the same ExpHist with a new window. If the new window is
   * smaller than the current window, evicts older items.
   */
  def withWindow(newWindow: Long): ExpHist =
    copy(conf = conf.copy(windowSize = newWindow)).step(time)

  // Returns the vector of bucket sizes from largest to smallest.
  def windows: Vector[Long] =
    for {
      (numBuckets, idx) <- sizes.zipWithIndex.reverse
      bucket <- List.fill(numBuckets)(1L << idx)
    } yield bucket
}

object Canonical {
  case class Exp2(exp: Int) extends AnyVal { l =>
    def double: Exp2 = Exp2(exp + 1)
    def value: Long = 1 << exp
  }

  @inline private[this] def prevPowerOfTwo(x: Long): Int =
    JLong.numberOfTrailingZeros(JLong.highestOneBit(x))

  @inline private[this] def modPow2(i: Int, exp2: Int): Int = i & ((1 << exp2) - 1)
  @inline private[this] def quotient(i: Int, exp2: Int): Int = i >> exp2
  @inline private[this] def bit(i: Int, idx: Int): Int = (i >> idx) & 1

  private[this] def binarize(i: Int, bits: Int, offset: Int): Vector[Int] =
    (0 until bits).map { idx => offset + bit(i, idx) }.toVector

  /**
   * returns a vector of the total number of buckets of size `s^i`,
   * where `i` is the vector index, used to represent s for a given
   * k.
   *
   * `s` is the "sum of the sizes of all of the buckets tracked by
   * the ExpHist instance.
   *
   * `l` is... well, gotta explain that based on the paper.
   */
  def fromLong(s: Long, l: Int): Vector[Int] = {
    val num = s + l
    val denom = l + 1
    val j = prevPowerOfTwo(num / denom)
    val offset = (num - (denom << j)).toInt
    binarize(modPow2(offset, j), j, l) :+ (quotient(offset, j) + 1)
  }

  /**
   * Total number of buckets required to represent this number.
   */
  def bucketsRequired(s: Long, l: Short): Int = fromLong(s, l).sum

  /**
   * Expand out a number's l-canonical form into the original number.
   */
  def expand(rep: Vector[Int]): Long =
    if (rep.isEmpty) 0L
    else {
      rep.iterator.zipWithIndex
        .map { case (i, exp) => i.toLong << exp }
        .reduce(_ + _)
    }

  // Expands out the compressed form to a list of the bucket sizes (sized with exponent only)
  //
  // TODO: law - toBuckets(rep).size == rep.sum
  def toBuckets(rep: Vector[Int]): Vector[Exp2] =
    rep.zipWithIndex.flatMap { case (i, exp) => List.fill(i)(Exp2(exp)) }

  /**
   * Given a canonical representation, takes a number of buckets to
   * drop (starting with the largest buckets!) and removes that
   * number of buckets from the end.
   *
   * (The sum of `canonical` is the total number of buckets in the
   * representation.)
   */
  @tailrec def dropBiggest(canonical: Vector[Int], bucketsToDrop: Int): Vector[Int] =
    (canonical, bucketsToDrop) match {
      case (l, 0) => l
      case (l @ (init :+ last), toDrop) =>
        (toDrop - last) match {
          case 0 => init
          case x if x < 0 => init :+ -x
          case x if x > 0 => dropBiggest(init, x)
        }
      case _ => Vector.empty[Int]
    }
}
