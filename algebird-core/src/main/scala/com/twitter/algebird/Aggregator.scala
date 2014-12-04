package com.twitter.algebird

import java.util.PriorityQueue
/**
 * Aggregators compose well.
 *
 * To create a parallel aggregator that operates on a single
 * input in parallel, use:
 * GeneratedTupleAggregator.from2((agg1, agg2))
 */
object Aggregator extends java.io.Serializable {
  /**
   * This will be removed in algebird 0.9. Here to add methods without breaking binary compat.
   */
  implicit def enrich[A, B, C](agg: Aggregator[A, B, C]): AggregatorEnrichment[A, B, C] =
    new AggregatorEnrichment(agg)

  /**
   * This is a trivial aggregator that always returns a single value
   */
  def const[T](t: T): Aggregator[Any, Unit, T] =
    prepareMonoid { _: Any => () }
      .andThenPresent(_ => t)
  /**
   * Using Aggregator.prepare,present you can add to this aggregator
   */
  def fromReduce[T](red: (T, T) => T): Aggregator[T, T, T] = fromSemigroup(Semigroup.from(red))
  def fromSemigroup[T](implicit sg: Semigroup[T]): Aggregator[T, T, T] = new Aggregator[T, T, T] {
    def prepare(input: T) = input
    def reduce(l: T, r: T) = sg.plus(l, r)
    def present(reduction: T) = reduction
  }
  def fromMonoid[T](implicit mon: Monoid[T]): MonoidAggregator[T, T, T] = prepareMonoid(identity[T])
  // Uses the product from the ring
  def fromRing[T](implicit rng: Ring[T]): RingAggregator[T, T, T] = fromRing[T, T](rng, identity[T])

  def fromMonoid[F, T](implicit mon: Monoid[T], prep: F => T): MonoidAggregator[F, T, T] =
    prepareMonoid(prep)(mon)

  def prepareMonoid[F, T](prep: F => T)(implicit m: Monoid[T]): MonoidAggregator[F, T, T] = new MonoidAggregator[F, T, T] {
    def prepare(input: F) = prep(input)
    def monoid = m
    def present(reduction: T) = reduction
  }
  // Uses the product from the ring
  def fromRing[F, T](implicit rng: Ring[T], prep: F => T): RingAggregator[F, T, T] = new RingAggregator[F, T, T] {
    def prepare(input: F) = prep(input)
    def ring = rng
    def present(reduction: T) = reduction
  }

  /**
   * How many items satisfy a predicate
   */
  def count[T](pred: T => Boolean): MonoidAggregator[T, Long, Long] =
    prepareMonoid { t: T => if (pred(t)) 1L else 0L }

  /**
   * Do any items satisfy some predicate
   */
  def exists[T](pred: T => Boolean): MonoidAggregator[T, Boolean, Boolean] =
    prepareMonoid(pred)(OrVal.unboxedMonoid)
  /**
   * Do all items satisfy a predicate
   */
  def forall[T](pred: T => Boolean): MonoidAggregator[T, Boolean, Boolean] =
    prepareMonoid(pred)(AndVal.unboxedMonoid)
  /**
   * Take the first (left most in reduce order) item found
   */
  def head[T]: Aggregator[T, T, T] = fromReduce[T] { (l, r) => l }
  /**
   * Take the last (right most in reduce order) item found
   */
  def last[T]: Aggregator[T, T, T] = fromReduce[T] { (l, r) => r }
  /**
   * Get the maximum item
   */
  def max[T: Ordering]: Aggregator[T, T, T] = new MaxAggregator[T]
  def maxBy[U, T: Ordering](fn: U => T): Aggregator[U, U, U] = {
    implicit val ordU = Ordering.by(fn)
    max[U]
  }
  /**
   * Get the minimum item
   */
  def min[T: Ordering]: Aggregator[T, T, T] = new MinAggregator[T]
  def minBy[U, T: Ordering](fn: U => T): Aggregator[U, U, U] = {
    implicit val ordU = Ordering.by(fn)
    min[U]
  }
  /**
   * This returns the number of items we find
   */
  def size: MonoidAggregator[Any, Long, Long] =
    prepareMonoid { (_: Any) => 1L }
  /**
   * Take the smallest `count` items using a heap
   */
  def sortedTake[T: Ordering](count: Int): MonoidAggregator[T, PriorityQueue[T], Seq[T]] =
    new mutable.PriorityQueueToListAggregator[T](count)
  /**
   * Take the largest `count` items using a heap
   */
  def sortedReverseTake[T: Ordering](count: Int): MonoidAggregator[T, PriorityQueue[T], Seq[T]] =
    new mutable.PriorityQueueToListAggregator[T](count)(implicitly[Ordering[T]].reverse)
  /**
   * Put everything in a List. Note, this could fill the memory if the List is very large.
   */
  def toList[T]: MonoidAggregator[T, List[T], List[T]] =
    prepareMonoid { t: T => List(t) }
  /**
   * Put everything in a Set. Note, this could fill the memory if the Set is very large.
   */
  def toSet[T]: MonoidAggregator[T, Set[T], Set[T]] =
    prepareMonoid { t: T => Set(t) }

  /**
   * This builds an in-memory Set, and then finally gets the size of that set.
   * This may not be scalable if the Uniques are very large. You might check the
   * HyperLogLog Aggregator to get an approximate version of this that is scalable.
   */
  def uniqueCount[T]: Aggregator[T, Set[T], Int] =
    toSet[T].andThenPresent(_.size)
}

/**
 * This is a type that models map/reduce(map). First each item is mapped,
 * then we reduce with a semigroup, then finally we present the results.
 *
 * Unlike Fold, Aggregator keeps it's middle aggregation type externally visible.
 * This is because Aggregators are useful in parallel map/reduce systems where
 * there may be some additional types needed to cross the map/reduce boundary
 * (such a serialization and intermediate storage). If you don't care about the
 * middle type, an _ may be used and the main utility of the instance is still
 * preserved (e.g. def operate[T](ag: Aggregator[T, _, Int]): Int)
 *
 * Note, join is very useful to combine multiple aggregations with one pass.
 * Also GeneratedTupleAggregator.fromN((agg1, agg2, ... aggN)) can glue these
 * together well.
 *
 * This type is the the Fold.M from Haskell's fold package:
 * https://hackage.haskell.org/package/folds-0.6.2/docs/Data-Fold-M.html
 */
trait Aggregator[-A, B, +C] extends java.io.Serializable { self =>
  def prepare(input: A): B
  def reduce(l: B, r: B): B
  def present(reduction: B): C

  def reduce(items: TraversableOnce[B]): B = items.reduce{ reduce(_, _) }
  def apply(inputs: TraversableOnce[A]): C = present(reduce(inputs.map{ prepare(_) }))

  def append(l: B, r: A): B = reduce(l, prepare(r))

  def appendAll(old: B, items: TraversableOnce[A]): B =
    if (items.isEmpty) old else reduce(old, reduce(items.map(prepare)))

  /** Like calling andThen on the present function */
  def andThenPresent[D](present2: C => D): Aggregator[A, B, D] =
    new Aggregator[A, B, D] {
      def prepare(input: A) = self.prepare(input)
      def reduce(l: B, r: B) = self.reduce(l, r)
      def present(reduction: B) = present2(self.present(reduction))
    }
  /** Like calling compose on the prepare function */
  def composePrepare[A1](prepare2: A1 => A): Aggregator[A1, B, C] =
    new Aggregator[A1, B, C] {
      def prepare(input: A1) = self.prepare(prepare2(input))
      def reduce(l: B, r: B) = self.reduce(l, r)
      def present(reduction: B) = self.present(reduction)
    }
}

/**
 * This will be removed in algebird 0.9
 * it is here to backport methods added in 0.9.
 */
class AggregatorEnrichment[A, B, C](agg: Aggregator[A, B, C]) {
  /**
   * This allows you to run two aggregators on the same data with a single pass
   */
  def join[A2 <: A, B2, C2](that: Aggregator[A2, B2, C2]): Aggregator[A2, (B, B2), (C, C2)] =
    GeneratedTupleAggregator.from2((agg, that))

  /**
   * An Aggregator can be converted to a Fold, but not vice-versa
   * Note, a Fold is more constrained so only do this if you require
   * joining a Fold with an Aggregator to produce a Fold
   */
  def toFold: Fold[A, Option[C]] = Fold.fold[Option[B], A, Option[C]](
    {
      case (None, a) => Some(agg.prepare(a))
      case (Some(b), a) => Some(agg.append(b, a))
    },
    None,
    { _.map(agg.present(_)) })
}

trait MonoidAggregator[-A, B, +C] extends Aggregator[A, B, C] {
  def monoid: Monoid[B]
  final def reduce(l: B, r: B): B = monoid.plus(l, r)
  final override def reduce(items: TraversableOnce[B]): B =
    monoid.sum(items)

  def appendAll(items: TraversableOnce[A]): B = appendAll(monoid.zero, items)
}

trait RingAggregator[-A, B, +C] extends Aggregator[A, B, C] {
  def ring: Ring[B]
  final def reduce(l: B, r: B): B = ring.times(l, r)
  final override def reduce(items: TraversableOnce[B]): B =
    if (items.isEmpty) ring.one // There are several pseudo-rings, so avoid one if you can
    else items.reduceLeft(reduce _)

  def appendAll(items: TraversableOnce[A]): B = appendAll(ring.one, items)
}
