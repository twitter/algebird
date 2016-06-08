package com.twitter.algebird

import scala.math.{ ceil, exp, log }
import scala.util.Random
import java.lang.Math.{ min, max }

import CMS2.Context

sealed abstract class CMS2[K] {

  def totalCount: Long

  def frequency(value: K)(implicit ctxt: Context[K]): Approximate[Long]

  def innerProduct(that: CMS2[K])(implicit ctxt: Context[K]): Approximate[Long]

  private[algebird] def add(value: K, n: Long = 1)(implicit ctxt: Context[K]): CMS2[K]

  private[algebird] def append(that: CMS2[K])(implicit ctxt: Context[K]): CMS2[K]

  private[algebird] def copy(): CMS2[K]

  def +(value: K)(implicit ctxt: Context[K]): CMS2[K] =
    copy().add(value)

  def ++(that: CMS2[K])(implicit ctxt: Context[K]): CMS2[K] =
    copy().append(that)

  def addAll(values: TraversableOnce[K])(implicit ctxt: Context[K]): CMS2[K] =
    values.foldLeft(copy())((c, k) => c.add(k))

  def addAllCounts(values: TraversableOnce[(K, Long)])(implicit ctxt: Context[K]): CMS2[K] =
    values.foldLeft(copy()) { case (c, (k, n)) => c.add(k, n) }

  def appendAll(those: TraversableOnce[CMS2[K]])(implicit ctxt: Context[K]): CMS2[K] =
    those.foldLeft(copy()) { (acc, c) => acc.append(c) }
}

object CMS2 {

  def empty[K]: CMS2[K] =
    Empty[K]

  def apply[K](k: K): CMS2[K] =
    Single(k, 1L)

  case class Empty[K]() extends CMS2[K] {
    def totalCount: Long = 0L

    def frequency(value: K)(implicit ctxt: Context[K]): Approximate[Long] =
      Approximate.exact(0L)

    def innerProduct(that: CMS2[K])(implicit ctxt: Context[K]): Approximate[Long] =
      Approximate.exact(0L)

    private[algebird] def add(value: K, n: Long = 1)(implicit ctxt: Context[K]): CMS2[K] =
      Single(value, n)

    private[algebird] def append(that: CMS2[K])(implicit ctxt: Context[K]): CMS2[K] =
      that

    private[algebird] def copy(): CMS2[K] =
      this
  }

  case class Single[K](k: K, totalCount: Long) extends CMS2[K] {
    require(totalCount >= 0, s"count should be non-negative (got: $totalCount)")

    def frequency(value: K)(implicit ctxt: Context[K]): Approximate[Long] =
      Approximate.exact(if (value == k) totalCount else 0L)

    def innerProduct(that: CMS2[K])(implicit ctxt: Context[K]): Approximate[Long] =
      that.frequency(k) * Approximate.exact(totalCount)

    private[algebird] def add(value: K, n: Long = 1)(implicit ctxt: Context[K]): CMS2[K] =
      if (value == k) Single(k, totalCount + n)
      else Dense.empty[K].add(k, totalCount).add(value, n)

    private[algebird] def append(that: CMS2[K])(implicit ctxt: Context[K]): CMS2[K] =
      Dense.empty[K].add(k, totalCount).append(that)

    private[algebird] def copy(): CMS2[K] =
      this
  }

  /**
   * Currently `cells` are stored in row-major form.
   *
   *   colums
   * r+-----------+m w
   * o|           |a i
   * w|           |x d
   * s|           |  t
   *  +-----------+  h
   *   max depth
   *
   * index(column, row) = cells(row * width + column)
   */
  case class Dense[K](cells: Array[Long], var totalCount: Long) extends CMS2[K] {

    def frequency(value: K)(implicit ctxt: Context[K]): Approximate[Long] = {
      val h = ctxt.hasher
      val d = ctxt.depth
      val w = ctxt.width
      val col0 = h.hash(0, value)
      var est = cells(col0) // indexing
      var row = 1
      while (row < d) {
        val col = h.hash(row, value)
        est = min(cells(row * w + col), est) // indexing
        row += 1
      }
      ctxt.approximate(est, totalCount)
    }

    def innerProduct(that: CMS2[K])(implicit ctxt: Context[K]): Approximate[Long] =
      that match {
        case Dense(otherCells, otherTotal) =>
          require(cells.length == otherCells.length, "tables must have same dimensions")
          val w = ctxt.width
          val d = ctxt.depth

          def innerProductAtRow(row: Int) = {
            var col = 0
            var sum = 0L
            while (col < w) {
              val i = row * w + col //indexing
              sum += cells(i) * otherCells(i)
              col += 1
            }
            sum
          }

          var est = innerProductAtRow(0)
          var row = 1
          while (row < d) {
            est = min(est, innerProductAtRow(row))
            row += 1
          }

          ctxt.approximate(est, totalCount * otherTotal)
        case otherwise =>
          otherwise.innerProduct(this)
      }

    private[algebird] def add(value: K, n: Long = 1)(implicit ctxt: Context[K]): CMS2[K] = {
      val h = ctxt.hasher
      val d = ctxt.depth
      val w = ctxt.width
      var row = 0
      while (row < d) {
        val col = h.hash(row, value)
        cells(row * w + col) += n // indexing
        row += 1
      }
      totalCount += n
      this
    }

    private[algebird] def append(that: CMS2[K])(implicit ctxt: Context[K]): CMS2[K] =
      that match {
        case Empty() =>
          this
        case Single(k, n) =>
          add(k, n)
        case Dense(otherCells, otherTotal) =>
          require(cells.length == otherCells.length, "tables must have same dimensions")
          var i = 0
          while (i < cells.length && i < otherCells.length) {
            cells(i) += otherCells(i)
            i += 1
          }
          this.totalCount += otherTotal
          this
      }

    private[algebird] def copy(): CMS2[K] =
      Dense(cells.clone, totalCount)
  }

  object Dense {
    def empty[K](implicit ctxt: Context[K]): CMS2[K] =
      Dense(new Array[Long](ctxt.width * ctxt.depth), 0L)
  }

  final case class Context[K](delta: Double, epsilon: Double, seed: Int)(implicit h: CMSHasher[K]) {
    require(0.0 < delta && delta < 1, s"delta should be in (0, 1), got $delta")
    require(0.0 < epsilon && epsilon < 1, s"epsilon should be in (0, 1), got $epsilon")

    final val width: Int = ceil((exp(1.0) / epsilon).toFloat.toDouble).toInt
    final val depth: Int = ceil(log(1.0 / delta)).toInt

    final val hasher: Hasher[K] = Hasher.generate(depth, width, seed)(h)

    def lowerBound(est: Long, total: Long): Long =
      max(0L, est - (epsilon * total).toLong) // zero floor

    def approximate(est: Long, total: Long): Approximate[Long] =
      if (est == 0L) Approximate.exact(0L)
      else Approximate(lowerBound(est, total), est, est, 1.0 - delta)
  }

  object Context {
    def apply[K: CMSHasher](d: Double, e: Double): Context[K] =
      Context(d, e, "fast as heck".hashCode)
  }

  case class Hasher[K](as: Array[Int], width: Int)(implicit h: CMSHasher[K]) {
    def hash(row: Int, value: K): Int = h.hash(as(row), 0, width)(value)
  }

  object Hasher {
    def generate[K](depth: Int, width: Int, seed: Int)(implicit h: CMSHasher[K]): Hasher[K] = {
      val r = new Random(seed)
      Hasher(Array.fill(depth)(r.nextInt()), width)(h)
    }
  }

  implicit def cms2Monoid[K](implicit ctxt: Context[K]): Monoid[CMS2[K]] =
    new Monoid[CMS2[K]] {
      val zero: CMS2[K] = CMS2.Empty[K]

      override def isNonZero(x: CMS2[K]): Boolean =
        x.totalCount > 0

      def plus(x: CMS2[K], y: CMS2[K]): CMS2[K] = x ++ y

      override def sumOption(xs: TraversableOnce[CMS2[K]]): Option[CMS2[K]] =
        if (xs.isEmpty) None
        else Some(zero.appendAll(xs))
    }
}
