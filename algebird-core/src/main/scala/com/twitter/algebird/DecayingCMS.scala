package com.twitter.algebird

import java.lang.Double.{compare => cmp}
import java.lang.Math
import java.util.Arrays.deepHashCode
import scala.concurrent.duration.Duration
import scala.util.Random

/**
 * DecayingCMS is a module to build count-min sketch instances whose
 * counts decay exponentially.
 *
 * Similar to a Map[K, com.twitter.algebird.DecayedValue], each key is
 * associated with a single count value that decays over time. Unlike
 * a map, the decyaing CMS is an approximate count -- in exchange for
 * the possibility of over-counting, we can bound its size in memory.
 *
 * The intended use case is for metrics or machine learning where
 * exact values aren't needed.
 *
 * You can expect the keys with the biggest values to be fairly
 * accurate but the very small values (rare keys or very old keys) to
 * be lost in the noise. For both metrics and ML this should be fine:
 * you can't learn too much from very rare values.
 *
 * We recommend depth of at least 5, and width of at least 100, but
 * you should do some experiments to determine the smallest parameters
 * that will work for your use case.
 */
final class DecayingCMS[K](
    seed: Long,
    val halfLife: Duration,
    val depth: Int, // number of hashing functions
    val width: Int, // number of table cells per hashing function
    hasher: CMSHasher[K]
) extends Serializable { module =>

  override def toString: String =
    s"DecayingCMS(seed=$seed, halfLife=$halfLife, depth=$depth, width=$width)"

  @inline private def getNextLogScale(
      logScale: Double,
      oldTimeInHL: Double,
      nowInHL: Double
  ): Double =
    if (nowInHL == oldTimeInHL) logScale else logScale + (nowInHL - oldTimeInHL) * log2

  @inline private def getScale(logScale: Double, oldTimeInHL: Double, nowInHL: Double): Double = {
    val logScale1 = getNextLogScale(logScale, oldTimeInHL, nowInHL)
    Math.exp(-logScale1)
  }

  val empty: CMS =
    new CMS(Array.fill(depth)(Vector.fill[Double](width)(0.0)), 0.0, Double.NegativeInfinity)

  /**
   * Represents a decaying scalar value at a particular point in time.
   *
   * The value decays according to halfLife. Another way to think
   * about DoubleAt is that it represents a particular decay curve
   * (and in particular, a point along that curve). Two DoubleAt
   * values may be equivalent if they are two points on the same curve.
   *
   * The `timeToZero` and `timeToUnit` methods can be used to
   * "normalize" DoubleAt values. If two DoubleAt values do not
   * produce the same (approximate) Double values from these methods,
   * they represent different curves.
   */
  class DoubleAt private[algebird] (val value: Double, val timeInHL: Double) extends Serializable {
    lhs =>

    // this is not public because it's not safe in general -- you need
    // to run a function that is time-commutative.
    private[algebird] def map(f: Double => Double): DoubleAt =
      new DoubleAt(f(value), timeInHL)

    // this is not public because it's not safe in general -- you need
    // to run a function that is time-commutative.
    private[algebird] def map2(rhs: DoubleAt)(f: (Double, Double) => Double): DoubleAt =
      if (lhs.timeInHL < rhs.timeInHL) {
        val x = lhs.scaledAt(rhs.timeInHL)
        new DoubleAt(f(x, rhs.value), rhs.timeInHL)
      } else if (lhs.timeInHL == rhs.timeInHL) {
        new DoubleAt(f(lhs.value, rhs.value), rhs.timeInHL)
      } else {
        val y = rhs.scaledAt(lhs.timeInHL)
        new DoubleAt(f(lhs.value, y), lhs.timeInHL)
      }

    def unary_- : DoubleAt = new DoubleAt(-value, timeInHL)
    def abs: DoubleAt = new DoubleAt(Math.abs(value), timeInHL)
    def *(n: Double): DoubleAt = new DoubleAt(value * n, timeInHL)

    def +(rhs: DoubleAt): DoubleAt = map2(rhs)(_ + _)
    def -(rhs: DoubleAt): DoubleAt = map2(rhs)(_ - _)
    def min(rhs: DoubleAt): DoubleAt = map2(rhs)(Math.min)
    def max(rhs: DoubleAt): DoubleAt = map2(rhs)(Math.max)

    def /(rhs: DoubleAt): Double = map2(rhs)(_ / _).value

    /**
     * We consider two DoubleAt values equal not just if their
     * elements are equal, but also if they represent the same value
     * at different points of decay.
     */
    def compare(rhs: DoubleAt): Int = {
      val vc = cmp(lhs.value, rhs.value)
      val tc = cmp(lhs.timeInHL, rhs.timeInHL)
      if (vc == tc) vc
      else if (tc == 0) vc
      else if (vc == 0) tc
      else if (tc < 0) cmp(lhs.scaledAt(rhs.timeInHL), rhs.value)
      else cmp(lhs.value, rhs.scaledAt(lhs.timeInHL))
    }

    /**
     * Time when this value will reach the smallest double value
     * bigger than zero, unless we are already at zero in which
     * case we return the current time
     */
    def timeToZero: Double =
      if (java.lang.Double.isNaN(value)) Double.NaN
      else if (java.lang.Double.isInfinite(value)) Double.PositiveInfinity
      else if (value == 0.0) timeInHL
      else timeToUnit + DoubleAt.TimeFromUnitToZero

    /**
     * This is the scaled time when the current value will reach
     * 1 (or -1 for negative values)
     *
     * This method is a way of collapsing a DoubleAt into a single
     * value (the time in the past or future where its value would be
     * 1, the unit value).
     */
    def timeToUnit: Double =
      if (java.lang.Double.isNaN(value)) Double.NaN
      else if (java.lang.Double.isInfinite(value)) Double.PositiveInfinity
      else if (value == 0.0) Double.NegativeInfinity
      else {
        // solve for result:
        //
        //   1 = value * module.getScale(0.0, timeInHL, result)
        //   1 = value * Math.exp(-getNextLogScale(0.0, timeInHL, result))
        //   1 / value = Math.exp(-getNextLogScale(0.0, timeInHL, result))
        //   log(1 / value) = -getNextLogScale(0.0, timeInHL, result)
        //   -log(1 / value) = getNextLogScale(0.0, timeInHL, result)
        //   log(value) = getNextLogScale(0.0, timeInHL, result)
        //   log(value) = if (result == timeInHL) 0 else 0 + (result - timeInHL) * log2
        //   log(value) = if (result == timeInHL) 0 else (result - timeInHL) * log2
        //
        //   log(value) = (result - timeInHL) * log2
        //   log(value) / log2 = result - timeInHL
        //   log(value) / log2 + timeInHL = result
        Math.log(Math.abs(value)) / log2 + timeInHL
      }

    override def equals(that: Any): Boolean =
      that match {
        case d: DoubleAt => compare(d) == 0
        case _           => false
      }

    override def hashCode: Int =
      timeToUnit.##

    override def toString: String =
      s"DoubleAt($value, $timeInHL)"

    def <(rhs: DoubleAt): Boolean = (lhs.compare(rhs)) < 0
    def <=(rhs: DoubleAt): Boolean = (lhs.compare(rhs)) <= 0
    def >(rhs: DoubleAt): Boolean = (lhs.compare(rhs)) > 0
    def >=(rhs: DoubleAt): Boolean = (lhs.compare(rhs)) >= 0

    def time: Long =
      toTimestamp(timeInHL)

    private def scaledAt(t: Double): Double =
      if (value == 0.0) 0.0
      else value * module.getScale(0.0, timeInHL, t)

    def at(time: Long): Double =
      if (value == 0.0) 0.0
      else value * module.getScale(0.0, timeInHL, fromTimestamp(time))
  }

  object DoubleAt {
    def apply(x: Double, t: Long): DoubleAt =
      new DoubleAt(x, fromTimestamp(t))

    val zero: DoubleAt =
      new DoubleAt(0.0, Double.NegativeInfinity)

    private val TimeFromUnitToZero: Double =
      -Math.log(Double.MinPositiveValue) / log2
  }

  val totalCells: Int = depth * width

  val halfLifeSecs: Double =
    halfLife.toMillis.toDouble / 1000.0

  // TODO: consider a smaller number?
  // we are trading accuracy for possible performence
  private[this] val maxLogScale: Double = 20.0

  /**
   * Allocate an empty array of row.
   *
   * The elements start as null. It's an important optimization _not_
   * to allocate vectors here, since we're often building up cells
   * mutably.
   */
  private def allocCells(): Array[Vector[Double]] =
    new Array[Vector[Double]](depth)

  def toTimestamp(t: Double): Long =
    (t * halfLifeSecs * 1000.0).toLong

  def fromTimestamp(t: Long): Double =
    (t.toDouble / 1000.0) / halfLifeSecs

  val hashFns: Array[K => Int] = {
    val rng = new Random(seed)
    def genPos(): Int =
      rng.nextInt match {
        case 0 => genPos()
        case n => n & 0x7fffffff
      }

    (0 until depth).map { _ =>
      val n = genPos()
      (k: K) => hasher.hash(n, 0, width)(k)
    }.toArray
  }

  private final val log2 = Math.log(2.0)

  /**
   * The idealized formula for the updating current value for a key
   * (y0 -> y1) is given as:
   *
   *   delta = (t1 - t0) / halflife
   *   y1 = y0 * 2^(-delta) + n
   *
   * However, we want to avoid having to rescale every single cell
   * every time we update; i.e. a cell with a zero value should
   * continue to have a zero value when n=0.
   *
   * Therefore, we introduce a change of variable to cell values (z)
   * along with a scale factor (scale), and the following formula:
   *
   *   (1) zN = yN * scaleN
   *
   * Our constraint is expressed as:
   *
   *   (2) If n=0, z1 = z0
   *
   * In that case:
   *
   *   (3) If n=0, (y1 * scale1) = (y0 * scale0)
   *   (4) Substituting for y1, (y0 * 2^(-delta) + 0) * scale1 = y0 * scale0
   *   (5) 2^(-delta) * scale1 = scale0
   *   (6) scale1 = scale0 * 2^(delta)
   *
   * Also, to express z1 in terms of z0, we say:
   *
   *   (7) z1 = y1 * scale1
   *   (8) z1 = (y0 * 2^(-delta) + n) * scale1
   *   (9) z1 = ((z0 / scale0) * 2^(-delta) + n) * scale1
   *  (10) z1 / scale1 = (z0 / (scale1 * 2^(-delta))) * 2^(-delta) + n
   *  (11) z1 / scale1 = z0 / scale1 + n
   *  (12) z1 = z0 + n * scale1
   *
   * So, for cells where n=0, we just update scale0 to scale1, and for
   * cells where n is non-zero, we update z1 in terms of z0 and
   * scale1.
   *
   * If we convert scale to logscale, we have:
   *
   *  (13) logscale1 = logscale0 + delta * log(2)
   *  (14) z1 = z0 + n * exp(logscale1)
   *
   * When logscale1 gets big, we start to distort z1. For example,
   * exp(36) is close to 2^53. We can measure when n * exp(logscale1)
   * gets big, and in those cases we can rescale all our cells (set
   * each z to its corresponding y) and set the logscale to 0.
   *
   *  (15) y1 = z1 / scale1
   *  (16) y1 = z1 / exp(logscale1)
   *  (17) y1 = z1 * exp(-logscale1)
   */
  final class CMS(
      val cells: Array[Vector[Double]],
      val logScale: Double,
      val timeInHL: Double
  ) extends Serializable {

    @inline private def scale: Double =
      Math.exp(-logScale)

    override def toString: String = {
      val s = cells.iterator.map(_.toString).mkString("Array(", ", ", ")")
      s"CMS($s, $logScale, $timeInHL)"
    }

    override def hashCode: Int =
      deepHashCode(cells.asInstanceOf[Array[Object]]) * 59 +
        logScale.## * 17 +
        timeInHL.## * 37 +
        19

    // unfortunately we can't check the path-dependent type of this
    // CMS, which we signal by using a type projection here.
    override def equals(any: Any): Boolean =
      any match {
        case that: DecayingCMS[_]#CMS =>
          this.logScale == that.logScale &&
            this.timeInHL == that.timeInHL &&
            this.cells.length == that.cells.length && {
            var i = 0
            while (i < depth) {
              if (this.cells(i) != that.cells(i)) return false
              i += 1
            }
            true
          }
        case _ =>
          false
      }

    def lastUpdateTime: Long =
      toTimestamp(timeInHL)

    /**
     * Provide lower and upper bounds on values returned for any
     * possible key.
     *
     * The first value is a lower bound: even keys that have never
     * been counted will return this value or greater. This will be
     * zero unless the CMS is saturated.
     *
     * The second value is an upper bound: the key with the largest
     * cardinality will not be reported as being larger than this
     * value (though it might be reported as being smaller).
     *
     * Together these values indicate how saturated and skewed the CMS
     * might be.
     */
    def range: (DoubleAt, DoubleAt) = {
      var minMinimum = Double.PositiveInfinity
      var minMaximum = Double.PositiveInfinity
      var i = 0
      while (i < cells.length) {
        val it = cells(i).iterator
        var localMax = it.next // we know it doesn't start empty
        if (localMax < minMinimum) minMinimum = localMax
        while (it.hasNext) {
          val n = it.next
          if (n > localMax) localMax = n
          else if (n < minMinimum) minMinimum = n
        }
        if (localMax < minMaximum) minMaximum = localMax
        i += 1
      }

      val s = scale
      def sc(x: Double): DoubleAt =
        new DoubleAt(if (x == 0.0) 0.0 else x * s, timeInHL)

      (sc(minMinimum), sc(minMaximum))
    }

    /**
     * Returns the square-root of the inner product of two decaying
     * CMSs.
     *
     * We want the result to decay at the same rate as the CMS for
     * this method to be valid. Taking the square root ensures that
     * this is true. Without it, we would violate the following
     * equality (assuming we had at() on a CMS):
     *
     *   x.innerProduct(y).at(t) = x.at(t).innerProduct(y.at(t))
     *
     * This is why we don't support innerProduct, only
     * innerProductRoot.
     */
    def innerProductRoot(that: CMS): DoubleAt = {
      var i = 0
      var res = Double.PositiveInfinity
      val t = Math.max(this.timeInHL, that.timeInHL)
      val scale = this.getScale(t) * that.getScale(t)
      while (i < depth) {
        var sum = 0.0
        val it0 = this.cells(i).iterator
        val it1 = that.cells(i).iterator
        while (it0.hasNext) {
          val x = it0.next * it1.next
          if (x != 0.0) sum += x
        }
        if (sum < res) res = sum
        i += 1
      }
      val x = if (res != 0.0) Math.sqrt(res * scale) else 0.0
      new DoubleAt(x, t)
    }

    def l2Norm: DoubleAt =
      innerProductRoot(this)

    def scale(x: Double): CMS =
      if (java.lang.Double.isNaN(x)) {
        throw new IllegalArgumentException(s"invalid scale: $x")
      } else if (x < 0.0) {
        throw new IllegalArgumentException(s"negative scale is not allowed: $x")
      } else if (x == 0.0) {
        module.empty
      } else {
        val s = logScale + Math.log(x)
        val c = new CMS(cells, s, timeInHL)
        if (s > maxLogScale) c.rescaleTo(timeInHL) else c
      }

    /**
     * Get the total count of all items in the CMS.
     *
     * The total is the same as the l1Norm, since we don't allow
     * negative values.
     *
     * Total is one of the few non-approximate statistics that
     * DecayingCMS supports. We expect the total to be exact (except
     * for floating-point error).
     */
    def total: DoubleAt = {
      val n = cells(0).sum
      val x = if (n == 0.0) 0.0 else scale * n
      new DoubleAt(x, timeInHL)
    }

    def get(k: K): DoubleAt = {
      var minValue = Double.PositiveInfinity
      var didx = 0
      while (didx < depth) {
        val i = hashFns(didx)(k)
        val inner = cells(didx)
        val value = inner(i)
        if (value < minValue) minValue = value
        didx += 1
      }
      val x = if (minValue == 0.0) 0.0 else scale * minValue
      new DoubleAt(x, timeInHL)
    }

    def getScale(t: Double): Double =
      module.getScale(logScale, timeInHL, t)

    private final def nextLogScale(t: Double): Double =
      module.getNextLogScale(logScale, timeInHL, t)

    def +(other: CMS): CMS = {
      val x = this
      val y = other
      val timeInHL = Math.max(x.timeInHL, y.timeInHL)
      val cms = new CMS(allocCells, 0.0, timeInHL)

      val xscale = x.getScale(timeInHL)
      val yscale = y.getScale(timeInHL)

      // a zero count is zero, no matter, how big the scale is.
      @inline def prod(x: Double, y: Double): Double =
        if (x == 0.0) 0.0 else x * y

      var i = 0
      while (i < depth) {
        val left = x.cells(i)
        val right = y.cells(i)
        var j = 0
        val bldr = rowBuilder()
        while (j < width) {
          bldr += prod(left(j), xscale) + prod(right(j), yscale)
          j += 1
        }
        cms.cells(i) = bldr.result
        i += 1
      }
      cms
    }

    def add(t: Long, k: K, n: Double): CMS =
      scaledAdd(fromTimestamp(t), k, n)

    // TODO: we could allocate a mutable scratch pad, write all the
    // values into it, and then build a CMS out of it. if items is
    // very small, this would be less efficient than what we're doing
    // now. probably the "ideal" solution would be determine how many
    // items there are. if we have fewer than ~width items, this
    // approach is fine. for more, a scratch pad would be better
    // (assuming we wrote that code).
    //
    // alternately, you could map items into (zero + item) and then
    // use the monoid's sum to boil it down.
    //
    // we only use this in testing currently so the current code is
    // fine until we rely on it in production. any change here should
    // probably include benchmarks justifying the design.
    def bulkAdd(items: Iterable[(Long, K, Double)]): CMS =
      items.foldLeft(this) { case (c, (t, k, v)) => c.add(t, k, v) }

    private[algebird] def scaledAdd(ts1: Double, k: K, n: Double): CMS =
      if (n < 0.0) {
        val t = toTimestamp(ts1)
        throw new IllegalArgumentException(
          s"we can only add non-negative numbers to a CMS, got $n for key: $k at time: $t"
        )
      } else if (n == 0.0) {
        this
      } else {
        val logScale1 = nextLogScale(ts1)
        if (logScale1 > maxLogScale) {
          rescaleTo(ts1).scaledAdd(ts1, k, n)
        } else {
          val increment = n * Math.exp(logScale1)
          val cells1 = allocCells()
          var didx = 0
          while (didx < depth) {
            val cell = cells(didx)
            val w = hashFns(didx)(k)
            cells1(didx) = cell.updated(w, cell(w) + increment)
            didx += 1
          }
          new CMS(cells1, logScale1, ts1)
        }
      }

    // Set the scale back to 0.0
    // input time is in half-lives
    private[algebird] def rescaleTo(ts: Double): CMS = {
      val logScale1 = nextLogScale(ts)
      val expL = Math.exp(-logScale1)
      if (expL == 0.0) {
        new CMS(monoid.zero.cells, 0.0, ts)
      } else {
        val cms = new CMS(allocCells, 0.0, ts)
        var i = 0
        while (i < depth) {
          val ci = cells(i)
          cms.cells(i) = ci.map(_ * expL)
          i += 1
        }
        cms
      }
    }
  }

  private def rowBuilder() = {
    val bldr = Vector.newBuilder[Double]
    bldr.sizeHint(width)
    bldr
  }

  object CMS {

    implicit val monoidForCMS: Monoid[CMS] =
      new Monoid[CMS] {

        def zero: CMS = module.empty

        def plus(x: CMS, y: CMS): CMS =
          x + y

        /**
         * Turn a flat array into an array of vectors.
         */
        private def scratchToCells(scratch: Array[Double]): Array[Vector[Double]] = {
          val cells = new Array[Vector[Double]](depth)
          var i = 0
          while (i < depth) {
            var j = i * width
            val limit = j + width
            val bldr = rowBuilder()
            while (j < limit) {
              bldr += scratch(j)
              j += 1
            }
            cells(i) = bldr.result
            i += 1
          }
          cells
        }

        /**
         * This method sums the first `num` items in `arr`.
         */
        private def innerSum(arr: Array[CMS], num: Int): CMS =
          if (num == 0) zero
          else if (num == 1) arr(0)
          else if (num == 2) plus(arr(0), arr(1))
          else {
            // start with zero
            val scratch: Array[Double] = new Array(totalCells)

            val latestTimeInHL: Double =
              arr.iterator.take(num).map(cms => cms.timeInHL).max

            var i = 0
            while (i < num) {
              val cms = arr(i)
              val scale = cms.getScale(latestTimeInHL)
              var j = 0
              while (j < depth) {
                val row = cms.cells(j)
                val stride = j * width
                var k = 0
                while (k < width) {
                  val n = row(k)
                  if (n > 0.0) {
                    scratch(stride + k) += scale * n
                  }
                  k += 1
                }
                j += 1
              }
              i += 1
            }

            val cells = scratchToCells(scratch)

            new CMS(cells, 0.0, latestTimeInHL)
          }

        override def sumOption(xs: TraversableOnce[CMS]): Option[CMS] = {

          val it: Iterator[CMS] = xs.toIterator
          val ChunkSize = 1000

          // the idea here is that we read up to 1000 CMS values into
          // a fixed array, crunch them down to a single CMS, store it
          // in the first array index, read up to 999 more CMS values
          // in, crunch them down, and so on.
          var i = 0
          val arr = new Array[CMS](ChunkSize)
          while (it.hasNext) {
            while (it.hasNext && i < ChunkSize) {
              arr(i) = it.next
              i += 1
            }
            if (i > 1) {
              arr(0) = innerSum(arr, i)
            }
            i = 1
          }
          if (i == 0) None else Some(arr(0))
        }
      }
  }

  val monoid: Monoid[CMS] = CMS.monoidForCMS
}

object DecayingCMS {

  /**
   * Construct a DecayingCMS module.
   *
   * The seed is used to initialize the hash families used by the
   * count-min sketch. Using the same seed will always produce the
   * same hash family.
   *
   * Half-life determines the rate at which values in the CMS decay.
   * If a key was counted once at time t, by time (t + halfLife), the
   * value for that key will be 0.5. After enough half lives the value
   * will decay to zero.
   *
   * The size of the CMS in bytes is O(depth * width).
   *
   * Width controls the relative error due to over-counting
   * (approximately 1/width). For 1% error, use width=100, for 0.1%
   * error, use width=1000, etc.
   *
   * Depth controls the probability the error bounds are broken and
   * that probability scales with exp(-alpha * depth) so, a small depth
   * (e.g. 5-10) is fine. Each update requires O(depth) work so you
   * want to keep this as small as possible.
   */
  def apply[K](seed: Long, halfLife: Duration, depth: Int, width: Int)(implicit
      hasher: CMSHasher[K]
  ): DecayingCMS[K] =
    new DecayingCMS(seed, halfLife, depth, width, hasher)
}
