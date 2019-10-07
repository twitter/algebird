package com.twitter.algebird

import scala.annotation.tailrec

/**
 * Batched: the free semigroup.
 *
 * For any type `T`, `Batched[T]` represents a way to lazily combine T
 * values as a semigroup would (i.e. associatively). A `Semigroup[T]`
 * instance can be used to recover a `T` value from a `Batched[T]`.
 *
 * Like other free structures, Batched trades space for time. A sum of
 * batched values defers the underlying semigroup action, instead
 * storing all values in memory (in a tree structure). If an
 * underlying semigroup is available, `Batched.semigroup` and
 * `Batch.monoid` can be configured to periodically sum the tree to
 * keep the overall size below `batchSize`.
 *
 * `Batched[T]` values are guaranteed not to be empty -- that is, they
 * will contain at least one `T` value.
 */
sealed abstract class Batched[T] extends Serializable {

  /**
   * Sum all the `T` values in this batch using the given semigroup.
   */
  def sum(implicit sg: Semigroup[T]): T

  /**
   * Combine two batched values.
   *
   * As mentioned above, this just creates a new tree structure
   * containing `this` and `that`.
   */
  def combine(that: Batched[T]): Batched[T] =
    Batched.Items(this, that)

  /**
   * Compact this batch if it exceeds `batchSize`.
   *
   * Compacting a branch means summing it, and then storing the summed
   * value in a new single-item batch.
   */
  def compact(batchSize: Int)(implicit s: Semigroup[T]): Batched[T] =
    if (size < batchSize) this else Batched.Item(sum(s))

  /**
   * Add more values to a batched value.
   *
   * This method will grow the tree to the left.
   */
  def append(that: TraversableOnce[T]): Batched[T] =
    that.foldLeft(this)((b, t) => b.combine(Batched(t)))

  /**
   * Provide an iterator over the underlying tree structure.
   *
   * This is the order used by `.sum`.
   *
   * This iterator traverses the tree from left-to-right. If the
   * original expression was (w + x + y + z), this iterator returns w,
   * x, y, and then z.
   */
  def iterator: Iterator[T] =
    this match {
      case Batched.Item(t) => Iterator.single(t)
      case b               => new Batched.ForwardItemsIterator(b)
    }

  /**
   * Convert the batch to a `List[T]`.
   */
  def toList: List[T] =
    reverseIterator.foldLeft(List.empty[T])((ts, t) => t :: ts)

  /**
   * Provide a reversed iterator over the underlying tree structure.
   *
   * This iterator traverses the tree from right-to-left. If the
   * original expression was (w + x + y + z), this iterator returns z,
   * y, x, and then w.
   */
  def reverseIterator: Iterator[T] =
    this match {
      case Batched.Item(t) => Iterator.single(t)
      case b               => new Batched.ReverseItemsIterator(b)
    }

  /**
   * Report the size of the underlying tree structure.
   *
   * This is an O(1) operation -- each subtree knows how big it is.
   */
  def size: Int
}

object Batched {

  /**
   * Constructed a batch from a single value.
   */
  def apply[T](t: T): Batched[T] =
    Item(t)

  /**
   * Constructed an optional batch from a collection of values.
   *
   * Since batches cannot be empty, this method returns `None` if `ts`
   * is empty, and `Some(batch)` otherwise.
   */
  def items[T](ts: TraversableOnce[T]): Option[Batched[T]] =
    if (ts.isEmpty) None
    else {
      val it = ts.toIterator
      val t0 = it.next
      Some(Item(t0).append(it))
    }

  /**
   * Equivalence for batches.
   *
   * Batches are equivalent if they sum to the same value. Since the
   * free semigroup is associative, it's not correct to take tree
   * structure into account when determining equality.
   *
   * One thing to note here is that two equivalent batches might
   * produce different lists (for instance, if one of the batches has
   * more zeros in it than another one).
   */
  implicit def equiv[A](implicit e: Equiv[A], s: Semigroup[A]): Equiv[Batched[A]] =
    new Equiv[Batched[A]] {
      override def equiv(x: Batched[A], y: Batched[A]): Boolean =
        e.equiv(x.sum(s), y.sum(s))
    }

  /**
   * The free semigroup for batched values.
   *
   * This semigroup just accumulates batches and doesn't ever evaluate
   * them to flatten the tree.
   */
  implicit def semigroup[A]: Semigroup[Batched[A]] =
    new Semigroup[Batched[A]] {
      override def plus(x: Batched[A], y: Batched[A]): Batched[A] = x.combine(y)
    }

  /**
   * Compacting semigroup for batched values.
   *
   * This semigroup ensures that the batch's tree structure has fewer
   * than `batchSize` values in it. When more values are added, the
   * tree is compacted using `s`.
   */
  def compactingSemigroup[A: Semigroup](batchSize: Int): Semigroup[Batched[A]] =
    new BatchedSemigroup[A](batchSize)

  /**
   * Compacting monoid for batched values.
   *
   * This monoid ensures that the batch's tree structure has fewer
   * than `batchSize` values in it. When more values are added, the
   * tree is compacted using `m`.
   *
   * It's worth noting that `x + 0` here will produce the same sum as
   * `x`, but `.toList` will produce different lists (one will have an
   * extra zero).
   */
  def compactingMonoid[A: Monoid](batchSize: Int): Monoid[Batched[A]] =
    new BatchedMonoid[A](batchSize)

  /**
   * This aggregator batches up `agg` so that all the addition can be
   * performed at once.
   *
   * It is useful when `sumOption` is much faster than using `plus`
   * (e.g. when there is temporary mutable state used to make
   * summation fast).
   */
  def aggregator[A, B, C](batchSize: Int, agg: Aggregator[A, B, C]): Aggregator[A, Batched[B], C] =
    new Aggregator[A, Batched[B], C] {
      override def prepare(a: A): Batched[B] = Item(agg.prepare(a))
      override def semigroup: Semigroup[Batched[B]] =
        new BatchedSemigroup(batchSize)(agg.semigroup)
      override def present(b: Batched[B]): C = agg.present(b.sum(agg.semigroup))
    }

  /**
   * This monoid aggregator batches up `agg` so that all the addition
   * can be performed at once.
   *
   * It is useful when `sumOption` is much faster than using `plus`
   * (e.g. when there is temporary mutable state used to make
   * summation fast).
   */
  def monoidAggregator[A, B, C](
      batchSize: Int,
      agg: MonoidAggregator[A, B, C]
  ): MonoidAggregator[A, Batched[B], C] =
    new MonoidAggregator[A, Batched[B], C] {
      override def prepare(a: A): Batched[B] = Item(agg.prepare(a))
      override def monoid: Monoid[Batched[B]] = new BatchedMonoid(batchSize)(agg.monoid)
      override def present(b: Batched[B]): C = agg.present(b.sum(agg.semigroup))
    }

  def foldOption[T: Semigroup](batchSize: Int): Fold[T, Option[T]] =
    Fold
      .foldLeft[T, Option[Batched[T]]](Option.empty[Batched[T]]) {
        case (Some(b), t) => Some(b.combine(Item(t)).compact(batchSize))
        case (None, t)    => Some(Item(t))
      }
      .map(_.map(_.sum))

  def fold[T](batchSize: Int)(implicit m: Monoid[T]): Fold[T, T] =
    Fold
      .foldLeft[T, Batched[T]](Batched(m.zero)) { (b, t) =>
        b.combine(Item(t)).compact(batchSize)
      }
      .map(_.sum)

  /**
   * This represents a single (unbatched) value.
   */
  private[algebird] case class Item[T](t: T) extends Batched[T] {
    override def size: Int = 1
    override def sum(implicit sg: Semigroup[T]): T = t
  }

  /**
   * This represents two (or more) batched values being added.
   *
   * The actual addition is deferred until the `.sum` method is called.
   */
  private[algebird] case class Items[T](left: Batched[T], right: Batched[T]) extends Batched[T] {
    // Items#size will always be >= 2.
    override val size: Int = left.size + right.size

    override def sum(implicit sg: Semigroup[T]): T =
      sg.sumOption(new ForwardItemsIterator(this)).get
  }

  /**
   * Abstract iterator through a batch's tree.
   *
   * This class is agnostic about whether the traversal is
   * left-to-right or right-to-left. The abstract method `descend`
   * controls which direction the iterator moves.
   */
  private[algebird] abstract class ItemsIterator[A](root: Batched[A]) extends Iterator[A] {
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

    def descend(v: Batched[A]): A

    override def hasNext: Boolean =
      running

    override def next(): A =
      if (running) {
        val result = ready
        ascend()
        result
      } else {
        throw new NoSuchElementException("next on empty iterator")
      }
  }

  /**
   * Left-to-right iterator through a batch's tree.
   */
  private[algebird] class ForwardItemsIterator[A](root: Batched[A]) extends ItemsIterator[A](root) {
    override def descend(v: Batched[A]): A = {
      @inline @tailrec def descend0(v: Batched[A]): A =
        v match {
          case Items(lhs, rhs) =>
            stack = rhs :: stack
            descend0(lhs)
          case Item(value) =>
            value
        }
      descend0(v)
    }
  }

  /**
   * Right-to-left iterator through a batch's tree.
   */
  private[algebird] class ReverseItemsIterator[A](root: Batched[A]) extends ItemsIterator[A](root) {
    override def descend(v: Batched[A]): A = {
      @inline @tailrec def descend0(v: Batched[A]): A =
        v match {
          case Items(lhs, rhs) =>
            stack = lhs :: stack
            descend0(rhs)
          case Item(value) =>
            value
        }
      descend0(v)
    }
  }
}

/**
 * Compacting semigroup for batched values.
 *
 * This semigroup ensures that the batch's tree structure has fewer
 * than `batchSize` values in it. When more values are added, the
 * tree is compacted using `s`.
 */
class BatchedSemigroup[T: Semigroup](batchSize: Int) extends Semigroup[Batched[T]] {

  require(batchSize > 0, s"Batch size must be > 0, found: $batchSize")

  override def plus(a: Batched[T], b: Batched[T]): Batched[T] =
    a.combine(b).compact(batchSize)
}

/**
 * Compacting monoid for batched values.
 *
 * This monoid ensures that the batch's tree structure has fewer
 * than `batchSize` values in it. When more values are added, the
 * tree is compacted using `m`.
 */
class BatchedMonoid[T: Monoid](batchSize: Int)
    extends BatchedSemigroup[T](batchSize)
    with Monoid[Batched[T]] {
  override val zero: Batched[T] = Batched(Monoid.zero)

  // if we knew that (a+b=0) only for (a=0, b=0), we could instead do:
  //   new Batched.ItemsIterator(b).exists(monoid.isNonZero)
  override def isNonZero(b: Batched[T]): Boolean =
    Monoid.isNonZero(b.sum)
}
