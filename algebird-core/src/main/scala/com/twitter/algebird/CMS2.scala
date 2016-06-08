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
    values.foldLeft(copy())((acc, k) => acc.add(k))

  def addAllCounts(values: TraversableOnce[(K, Long)])(implicit ctxt: Context[K]): CMS2[K] =
    values.foldLeft(copy()) { case (acc, (k, n)) => acc.add(k, n) }

  def appendAll(those: TraversableOnce[CMS2[K]])(implicit ctxt: Context[K]): CMS2[K] =
    those.foldLeft(copy()) { (acc, c) => acc.append(c) }

  private[algebird] def rowSumInvariant(implicit ctxt: Context[K]): Boolean
}

object CMS2 {

  def empty[K]: CMS2[K] =
    new Empty[K]

  def apply[K](k: K): CMS2[K] =
    new Single(k, 1L)

  def addAll[K: CMS2.Context](ks: TraversableOnce[K]): CMS2[K] =
    new Empty[K].addAll(ks)

  def addAllCounts[K: CMS2.Context](ks: TraversableOnce[(K, Long)]): CMS2[K] =
    new Empty[K].addAllCounts(ks)

  private case class Empty[K]() extends CMS2[K] {
    def totalCount: Long = 0L

    def frequency(value: K)(implicit ctxt: Context[K]): Approximate[Long] =
      Approximate.exact(0L)

    def innerProduct(that: CMS2[K])(implicit ctxt: Context[K]): Approximate[Long] =
      Approximate.exact(0L)

    private[algebird] def add(value: K, n: Long = 1)(implicit ctxt: Context[K]): CMS2[K] =
      Single(value, n)

    private[algebird] def append(that: CMS2[K])(implicit ctxt: Context[K]): CMS2[K] =
      that.copy()

    private[algebird] def copy(): CMS2[K] =
      this

    private[algebird] def rowSumInvariant(implicit ctxt: Context[K]): Boolean = true
  }

  private case class Single[K](k: K, totalCount: Long) extends CMS2[K] {
    require(totalCount >= 0, s"count should be non-negative (got: $totalCount)")

    def frequency(value: K)(implicit ctxt: Context[K]): Approximate[Long] =
      Approximate.exact(if (value == k) totalCount else 0L)

    def innerProduct(that: CMS2[K])(implicit ctxt: Context[K]): Approximate[Long] =
      that.frequency(k) * Approximate.exact(totalCount)

    private[algebird] def add(value: K, n: Long = 1)(implicit ctxt: Context[K]): CMS2[K] =
      if (value == k) Single(k, totalCount + n)
      else Dense.empty[K].add(k, totalCount).add(value, n)

    private[algebird] def append(that: CMS2[K])(implicit ctxt: Context[K]): CMS2[K] =
      that.copy().add(k, totalCount)

    private[algebird] def copy(): CMS2[K] =
      this

    def equalsSingle(s: Single[K])(implicit ctxt: Context[K]): Boolean = (totalCount == s.totalCount) && {
      val h = ctxt.hasher
      val d = ctxt.depth
      var row = 0
      while ((row < d) && (h.hash(row, k) == h.hash(row, s.k))) { row += 1 }
      row == d
    }

    private[algebird] def rowSumInvariant(implicit ctxt: Context[K]): Boolean = true
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
   * also, by our update strategy, each row sums to totalCount (which
   * implies, if all rows have a single column with totalCount, this
   * is a singleton dense value).
   */
  private case class Dense[K](cells: Array[Long], var totalCount: Long) extends CMS2[K] {

    override def hashCode: Int =
      totalCount.toInt ^ java.util.Arrays.hashCode(cells)

    override def equals(that: Any): Boolean =
      (this eq (that.asInstanceOf[AnyRef])) || {
        that match {
          case Dense(thoseCells, thoseCounts) =>
            totalCount == thoseCounts && java.util.Arrays.equals(cells, thoseCells)
          case _ => false
        }
      }

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

    def equalsSingle(s: Single[K])(implicit ctxt: Context[K]): Boolean = (totalCount == s.totalCount) && {
      /**
       * The invariant on the row is that the sum == totalCount (because we put
       * into each row one time), so if we equal the count of s in each of the
       * hash locations, all other locations must be 0
       */
      val h = ctxt.hasher
      val d = ctxt.depth
      var row = 0
      while ((row < d) && (totalCount == cells(h.hash(row, s.k)))) { row += 1 }
      row == d
    }

    private[algebird] def rowSumInvariant(implicit ctxt: Context[K]): Boolean = {
      val d = ctxt.depth
      val w = ctxt.width
      var row = 0
      var sum = totalCount
      while ((row < d) && (sum == totalCount)) {
        var col = 0
        sum = 0L
        while (col < w) {
          sum += cells(row * w + col)
          col += 1
        }
        row += 1
      }
      (row == d)
    }
  }

  private object Dense {
    def empty[K](implicit ctxt: Context[K]): CMS2[K] =
      Dense(new Array[Long](ctxt.width * ctxt.depth), 0L)
  }

  final case class Context[K](epsilon: Double, delta: Double, seed: Int)(implicit h: CMSHasher[K]) {
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
    def apply[K: CMSHasher](e: Double, d: Double): Context[K] =
      Context(e, d, "fast as heck".hashCode)
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

  implicit def cms2Equiv[K](implicit ctxt: Context[K]): Equiv[CMS2[K]] = new Equiv[CMS2[K]] {
    def equiv(a: CMS2[K], b: CMS2[K]): Boolean = (a, b) match {
      case (Empty(), that) => that.totalCount == 0L
      case (that, Empty()) => that.totalCount == 0L
      case (s1 @ Single(_, _), s2 @ Single(_, _)) => s1.equalsSingle(s2)
      case (s @ Single(_, _), d @ Dense(_, _)) => d.equalsSingle(s)
      case (d @ Dense(_, _), s @ Single(_, _)) => d.equalsSingle(s)
      case (d1 @ Dense(_, _), d2 @ Dense(_, _)) => d1 == d2
    }
  }

  implicit def cms2Monoid[K](implicit ctxt: Context[K]): Monoid[CMS2[K]] =
    new Monoid[CMS2[K]] {
      val zero: CMS2[K] = new CMS2.Empty[K]

      override def isNonZero(x: CMS2[K]): Boolean =
        x.totalCount > 0

      def plus(x: CMS2[K], y: CMS2[K]): CMS2[K] = x ++ y

      override def sumOption(xs: TraversableOnce[CMS2[K]]): Option[CMS2[K]] =
        if (xs.isEmpty) None
        else Some(zero.appendAll(xs))
    }
}
