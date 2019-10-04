package com.twitter.algebird
import java.util.PriorityQueue

/**
 * Preparer is a way to build up an Aggregator through composition using a
 * more natural API: it allows you to start with the input type and describe a series
 * of transformations and aggregations from there, rather than starting from the aggregation
 * and composing "outwards" in both directions.
 *
 * Uses of Preparer will always start with a call to Preparer[A], and end with a call to
 * monoidAggregate or a related method, to produce an Aggregator instance.
 */
sealed trait Preparer[A, T] extends java.io.Serializable {

  /**
   * Produce a new MonoidAggregator which includes the Preparer's transformation chain in its prepare stage.
   */
  def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C]

  /**
   * Produce a new Preparer that chains this one-to-many transformation.
   * Because "many" could include "none", this limits future aggregations
   * to those done using monoids.
   */
  def flatMap[U](fn: T => TraversableOnce[U]): FlatMapPreparer[A, U]

  /**
   * Like flatMap using identity.
   */
  def flatten[U](implicit ev: <:<[T, TraversableOnce[U]]): FlatMapPreparer[A, U] = flatMap(ev)

  /**
   * Filter out values that do not meet the predicate.
   * Like flatMap, this limits future aggregations to MonoidAggregator.
   */
  def filter(fn: T => Boolean): FlatMapPreparer[A, T] = flatMap { t =>
    if (fn(t)) Some(t) else None
  }

  def collect[U](p: PartialFunction[T, U]): FlatMapPreparer[A, U] =
    flatMap { t =>
      if (p.isDefinedAt(t)) Some(p(t)) else None
    }

  /**
   * count and following methods all just call monoidAggregate with one of the standard Aggregators.
   * see the Aggregator object for more docs.
   */
  def count(pred: T => Boolean): MonoidAggregator[A, Long, Long] = monoidAggregate(Aggregator.count(pred))
  def exists(pred: T => Boolean): MonoidAggregator[A, Boolean, Boolean] =
    monoidAggregate(Aggregator.exists(pred))
  def forall(pred: T => Boolean): MonoidAggregator[A, Boolean, Boolean] =
    monoidAggregate(Aggregator.forall(pred))
  def size: MonoidAggregator[A, Long, Long] = monoidAggregate(Aggregator.size)

  def sortedTake(count: Int)(implicit ord: Ordering[T]): MonoidAggregator[A, PriorityQueue[T], Seq[T]] =
    monoidAggregate(Aggregator.sortedTake(count))

  def sortedReverseTake(
      count: Int
  )(implicit ord: Ordering[T]): MonoidAggregator[A, PriorityQueue[T], Seq[T]] =
    monoidAggregate(Aggregator.sortedReverseTake(count))

  def toList: MonoidAggregator[A, Option[Batched[T]], List[T]] = monoidAggregate(Aggregator.toList)
  def toSet: MonoidAggregator[A, Set[T], Set[T]] = monoidAggregate(Aggregator.toSet)
  def uniqueCount: MonoidAggregator[A, Set[T], Int] = monoidAggregate(Aggregator.uniqueCount)

  /**
   * transform a given Aggregator into a MonoidAggregator by lifting the reduce and present stages
   * into Option space
   */
  def lift[B, C](aggregator: Aggregator[T, B, C]): MonoidAggregator[A, Option[B], Option[C]] =
    monoidAggregate(aggregator.lift)

  /**
   * headOption and following methods are all just calling lift with standard Aggregators
   * see the Aggregator object for more docs
   */
  def headOption: MonoidAggregator[A, Option[T], Option[T]] = lift(Aggregator.head)
  def lastOption: MonoidAggregator[A, Option[T], Option[T]] = lift(Aggregator.last)
  def maxOption(implicit ord: Ordering[T]): MonoidAggregator[A, Option[T], Option[T]] = lift(Aggregator.max)
  def maxOptionBy[U: Ordering](fn: T => U): MonoidAggregator[A, Option[T], Option[T]] = {
    implicit val ordT: Ordering[T] = Ordering.by(fn)
    lift(Aggregator.max[T])
  }

  def minOption(implicit ord: Ordering[T]): MonoidAggregator[A, Option[T], Option[T]] = lift(Aggregator.min)
  def minOptionBy[U: Ordering](fn: T => U): MonoidAggregator[A, Option[T], Option[T]] = {
    implicit val ordT: Ordering[T] = Ordering.by(fn)
    lift(Aggregator.min[T])
  }

  def sumOption(implicit sg: Semigroup[T]): MonoidAggregator[A, Option[T], Option[T]] =
    lift(Aggregator.fromSemigroup(sg))
  def reduceOption(fn: (T, T) => T): MonoidAggregator[A, Option[T], Option[T]] =
    lift(Aggregator.fromReduce(fn))
}

object Preparer {

  /**
   * This is the expected entry point for creating a new Preparer.
   */
  def apply[A]: MapPreparer[A, A] = MapPreparer.identity[A]
}

/**
 * A Preparer that has had zero or more map transformations applied, but no flatMaps.
 * This can produce any type of Aggregator.
 */
trait MapPreparer[A, T] extends Preparer[A, T] {

  def prepareFn: A => T

  def map[U](fn: T => U): MapPreparer[A, U] =
    MapPreparer[A, U](fn.compose(prepareFn))

  override def flatMap[U](fn: T => TraversableOnce[U]): FlatMapPreparer[A, U] =
    FlatMapPreparer[A, U](fn.compose(prepareFn))

  override def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C] =
    aggregator.composePrepare(prepareFn)

  /**
   * Produce a new Aggregator which includes the Preparer's transformation chain in its prepare stage.
   */
  def aggregate[B, C](aggregator: Aggregator[T, B, C]): Aggregator[A, B, C] =
    aggregator.composePrepare(prepareFn)

  /**
   * Split the processing into two parallel aggregations.
   * You provide a function which produces two different aggregators from this preparer,
   * and it will return a single aggregator which does both aggregations in parallel.
   * (See also Aggregator's join method.)
   *
   * We really need to generate N versions of this for 3-way, 4-way etc splits.
   */
  def split[B1, B2, C1, C2](
      fn: MapPreparer[T, T] => (Aggregator[T, B1, C1], Aggregator[T, B2, C2])
  ): Aggregator[A, (B1, B2), (C1, C2)] = {
    val (a1, a2) = fn(MapPreparer.identity[T])
    aggregate(a1.join(a2))
  }

  /**
   * head and following methods all just call aggregate with one of the standard Aggregators.
   * see the Aggregator object for more docs.
   */
  def head: Aggregator[A, T, T] = aggregate(Aggregator.head)
  def last: Aggregator[A, T, T] = aggregate(Aggregator.last)
  def max(implicit ord: Ordering[T]): Aggregator[A, T, T] = aggregate(Aggregator.max)
  def maxBy[U: Ordering](fn: T => U): Aggregator[A, T, T] = {
    implicit val ordT: Ordering[T] = Ordering.by(fn)
    aggregate(Aggregator.max[T])
  }

  def min(implicit ord: Ordering[T]): Aggregator[A, T, T] = aggregate(Aggregator.min)
  def minBy[U: Ordering](fn: T => U): Aggregator[A, T, T] = {
    implicit val ordT: Ordering[T] = Ordering.by(fn)
    aggregate(Aggregator.min[T])
  }

  def sum(implicit sg: Semigroup[T]): Aggregator[A, T, T] = aggregate(Aggregator.fromSemigroup(sg))
  def reduce(fn: (T, T) => T): Aggregator[A, T, T] = aggregate(Aggregator.fromReduce(fn))
}

object MapPreparer {

  /**
   * Create a concrete MapPreparer.
   */
  def apply[A, T](fn: A => T): MapPreparer[A, T] = new MapPreparer[A, T] {
    override val prepareFn: A => T = fn
  }

  /**
   * This is purely an optimization for the case of mapping by identity.
   * It overrides the key methods to not actually use the identity function.
   */
  def identity[A]: MapPreparer[A, A] = new MapPreparer[A, A] {
    override val prepareFn: A => A = (a: A) => a
    override def map[U](fn: A => U): MapPreparer[A, U] = MapPreparer(fn)
    override def flatMap[U](fn: A => TraversableOnce[U]): FlatMapPreparer[A, U] = FlatMapPreparer(fn)
    override def monoidAggregate[B, C](aggregator: MonoidAggregator[A, B, C]): MonoidAggregator[A, B, C] =
      aggregator
    override def aggregate[B, C](aggregator: Aggregator[A, B, C]): Aggregator[A, B, C] = aggregator
  }
}

/**
 * A Preparer that has had one or more flatMap operations applied.
 * It can only accept MonoidAggregators.
 */
trait FlatMapPreparer[A, T] extends Preparer[A, T] {

  def prepareFn: A => TraversableOnce[T]

  def map[U](fn: T => U): FlatMapPreparer[A, U] =
    FlatMapPreparer { a: A =>
      prepareFn(a).map(fn)
    }

  override def flatMap[U](fn: T => TraversableOnce[U]): FlatMapPreparer[A, U] =
    FlatMapPreparer { a: A =>
      prepareFn(a).flatMap(fn)
    }

  override def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C] =
    aggregator.sumBefore.composePrepare(prepareFn)

  /**
   * alias of monoidAggregate for convenience
   * unlike MapPreparer's aggregate, can only take MonoidAggregator
   */
  def aggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C] =
    monoidAggregate(aggregator)

  /**
   * Like monoidAggregate, but using an implicit Monoid to construct the Aggregator
   */
  def sum(implicit monoid: Monoid[T]): MonoidAggregator[A, T, T] =
    monoidAggregate(Aggregator.fromMonoid(monoid))

  /**
   * Split the processing into two parallel aggregations.
   * You provide a function which produces two different aggregators from this preparer,
   * and it will return a single aggregator which does both aggregations in parallel.
   * (See also Aggregator's join method.)
   *
   * We really need to generate N versions of this for 3-way, 4-way etc splits.
   */
  def split[B1, B2, C1, C2](
      fn: FlatMapPreparer[TraversableOnce[T], T] => (
          MonoidAggregator[TraversableOnce[T], B1, C1],
          MonoidAggregator[TraversableOnce[T], B2, C2]
      )
  ): Aggregator[A, (B1, B2), (C1, C2)] = {
    val (a1, a2) = fn(FlatMapPreparer.identity[T])
    a1.join(a2).composePrepare(prepareFn)
  }
}

object FlatMapPreparer {

  /**
   * Create a concrete FlatMapPreparer.
   */
  def apply[A, T](fn: A => TraversableOnce[T]): FlatMapPreparer[A, T] = new FlatMapPreparer[A, T] {
    override val prepareFn: A => TraversableOnce[T] = fn
  }

  /**
   * This is purely an optimization for the case of flatMapping by identity.
   * It overrides the key methods to not actually use the identity function.
   */
  def identity[A]: FlatMapPreparer[TraversableOnce[A], A] = new FlatMapPreparer[TraversableOnce[A], A] {
    override val prepareFn: TraversableOnce[A] => TraversableOnce[A] = (a: TraversableOnce[A]) => a

    override def map[U](fn: A => U): FlatMapPreparer[TraversableOnce[A], U] =
      FlatMapPreparer { a: TraversableOnce[A] =>
        a.map(fn)
      }

    override def flatMap[U](fn: A => TraversableOnce[U]): FlatMapPreparer[TraversableOnce[A], U] =
      FlatMapPreparer { a: TraversableOnce[A] =>
        a.flatMap(fn)
      }

    override def monoidAggregate[B, C](
        aggregator: MonoidAggregator[A, B, C]
    ): MonoidAggregator[TraversableOnce[A], B, C] =
      aggregator.sumBefore
  }
}
