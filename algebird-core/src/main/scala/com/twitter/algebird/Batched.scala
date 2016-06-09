package com.twitter.algebird

import scala.annotation.tailrec

sealed abstract class Batched[T] {

  def sum(implicit sg: Semigroup[T]): T

  private[algebird] def size: Int

  def combine(that: Batched[T]): Batched[T] =
    Batched.Items(this, that)

  def append(that: Iterable[T]): Batched[T] =
    that.iterator.map(Batched(_)).foldLeft(this)(_ combine _)
}

object Batched {

  def apply[T](t: T): Batched[T] =
    Item(t)

  def aggregator[A, B, C](batchSize: Int, agg: Aggregator[A, B, C]): Aggregator[A, Batched[B], C] = new Aggregator[A, Batched[B], C] {
    def prepare(a: A): Batched[B] = Item(agg.prepare(a))
    def semigroup: Semigroup[Batched[B]] = new BatchedSemigroup(batchSize, agg.semigroup)
    def present(b: Batched[B]): C = agg.present(b.sum(agg.semigroup))
  }

  def monoidAggregator[A, B, C](batchSize: Int, agg: MonoidAggregator[A, B, C]): MonoidAggregator[A, Batched[B], C] =
    new MonoidAggregator[A, Batched[B], C] {
      def prepare(a: A): Batched[B] = Item(agg.prepare(a))
      def monoid: Monoid[Batched[B]] = new BatchedMonoid(batchSize, agg.monoid)
      def present(b: Batched[B]): C = agg.present(b.sum(agg.semigroup))
    }

  def foldOption[T: Semigroup](batchSize: Int): Fold[T, Option[T]] =
    Fold.foldLeft[T, Option[Batched[T]]](None: Option[Batched[T]]) {
      case (Some(b), t) => Some(b.combine(Item(t)))
      case (None, t) => Some(Item(t))
    }
      .map(_.map(_.sum))

  def fold[T: Monoid](batchSize: Int): Fold[T, T] =
    Fold.foldLeft[T, Batched[T]](Batched(Monoid.zero[T])) { (b, t) => b.combine(Item(t)) }
      .map(_.sum)

  /**
   * This represents a single (unbatched) value.
   */
  private[algebird] case class Item[T](t: T) extends Batched[T] {
    def size: Int = 1
    def sum(implicit sg: Semigroup[T]): T = t
  }

  /**
   * This represents two (or more) batched values being added.
   *
   * The actual addition is deferred until the `.sum` method is called.
   */
  private[algebird] case class Items[T](left: Batched[T], right: Batched[T]) extends Batched[T] {
    // Items#size will always be >= 2.
    val size: Int = left.size + right.size

    def sum(implicit sg: Semigroup[T]): T =
      sg.sumOption(new ItemsIterator(this)).get
  }

  private[algebird] class ItemsIterator[A](root: Batched[A]) extends Iterator[A] {
    var stack: List[Batched[A]] = Nil
    var running: Boolean = true
    var ready: A = descend(root)

    def ascend(): Unit =
      stack match {
        case Nil =>
          running = false
        case h :: t =>
          stack = t
          ready = descend(h)
      }

    @tailrec private def descend(v: Batched[A]): A =
      v match {
        case Items(lhs, rhs) =>
          stack = rhs :: stack
          descend(lhs)
        case Item(value) =>
          value
      }

    def hasNext: Boolean =
      running

    def next(): A =
      if (running) {
        val result = ready
        ascend()
        result
      } else {
        throw new NoSuchElementException("next on empty iterator")
      }
  }

}

class BatchedSemigroup[T](batchSize: Int, sg: Semigroup[T]) extends Semigroup[Batched[T]] {

  require(batchSize > 0, s"Batch size must be > 0, found: $batchSize")

  def plus(a: Batched[T], b: Batched[T]): Batched[T] = {
    val next = Batched.Items(a, b)
    if (next.size < batchSize) next
    else Batched.Item(next.sum(sg))
  }
}

class BatchedMonoid[T](batchSize: Int, monoid: Monoid[T]) extends BatchedSemigroup(batchSize, monoid) with Monoid[Batched[T]] {
  def zero: Batched[T] = Batched(monoid.zero)
  override def isNonZero(b: Batched[T]): Boolean = isNonZeroRec(List(b))

  @annotation.tailrec private def isNonZeroRec(stack: List[Batched[T]]): Boolean = stack match {
    case Nil => true
    case Batched.Item(i) :: tail => monoid.isNonZero(i) && isNonZeroRec(tail)
    case Batched.Items(a, b) :: tail => isNonZeroRec(a :: b :: tail)
  }
}
