package com.twitter.algebird

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
  def flatten[U](implicit ev: <:<[T, TraversableOnce[U]]) = flatMap(ev)

  /**
   * Filter out values that do not meet the predicate.
   * Like flatMap, this limits future aggregations to MonoidAggregator.
   */
  def filter(fn: T => Boolean) = flatMap { t => if (fn(t)) Some(t) else None }

  def collect[U](p: PartialFunction[T, U]): FlatMapPreparer[A, U] =
    flatMap { t => if (p.isDefinedAt(t)) Some(p(t)) else None }

  /**
   * count and following methods all just call monoidAggregate with one of the standard Aggregators.
   * see the Aggregator object for more docs.
   */

  def count(pred: T => Boolean) = monoidAggregate(Aggregator.count(pred))
  def exists(pred: T => Boolean) = monoidAggregate(Aggregator.exists(pred))
  def forall(pred: T => Boolean) = monoidAggregate(Aggregator.forall(pred))
  def size = monoidAggregate(Aggregator.size)

  def sortedTake(count: Int)(implicit ord: Ordering[T]) =
    monoidAggregate(Aggregator.sortedTake(count))

  def sortedReverseTake(count: Int)(implicit ord: Ordering[T]) =
    monoidAggregate(Aggregator.sortedReverseTake(count))

  def toList = monoidAggregate(Aggregator.toList)
  def toSet = monoidAggregate(Aggregator.toSet)
  def uniqueCount = monoidAggregate(Aggregator.uniqueCount)

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
  def headOption = lift(Aggregator.head)
  def lastOption = lift(Aggregator.last)
  def maxOption(implicit ord: Ordering[T]) = lift(Aggregator.max)
  def maxOptionBy[U: Ordering](fn: T => U) = {
    implicit val ordT = Ordering.by(fn)
    lift(Aggregator.max[T])
  }

  def minOption(implicit ord: Ordering[T]) = lift(Aggregator.min)
  def minOptionBy[U: Ordering](fn: T => U) = {
    implicit val ordT = Ordering.by(fn)
    lift(Aggregator.min[T])
  }

  def sumOption(implicit sg: Semigroup[T]) = lift(Aggregator.fromSemigroup(sg))
  def reduceOption(fn: (T, T) => T) = lift(Aggregator.fromReduce(fn))
}

object Preparer {
  /**
   * This is the expected entry point for creating a new Preparer.
   */
  def apply[A] = MapPreparer.identity[A]
}

/**
 * A Preparer that has had zero or more map transformations applied, but no flatMaps.
 * This can produce any type of Aggregator.
 */
trait MapPreparer[A, T] extends Preparer[A, T] {

  def prepareFn: A => T

  def map[U](fn: T => U): MapPreparer[A, U] =
    MapPreparer[A, U](fn.compose(prepareFn))

  def flatMap[U](fn: T => TraversableOnce[U]) =
    FlatMapPreparer[A, U](fn.compose(prepareFn))

  def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C] =
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

  def split[B1, B2, C1, C2](fn: MapPreparer[T, T] => (Aggregator[T, B1, C1], Aggregator[T, B2, C2])): Aggregator[A, (B1, B2), (C1, C2)] = {
    val (a1, a2) = fn(MapPreparer.identity[T])
    aggregate(a1.join(a2))
  }

  /**
   * head and following methods all just call aggregate with one of the standard Aggregators.
   * see the Aggregator object for more docs.
   */

  def head = aggregate(Aggregator.head)
  def last = aggregate(Aggregator.last)
  def max(implicit ord: Ordering[T]) = aggregate(Aggregator.max)
  def maxBy[U: Ordering](fn: T => U) = {
    implicit val ordT = Ordering.by(fn)
    aggregate(Aggregator.max[T])
  }

  def min(implicit ord: Ordering[T]) = aggregate(Aggregator.min)
  def minBy[U: Ordering](fn: T => U) = {
    implicit val ordT = Ordering.by(fn)
    aggregate(Aggregator.min[T])
  }

  def sum(implicit sg: Semigroup[T]) = aggregate(Aggregator.fromSemigroup(sg))
  def reduce(fn: (T, T) => T) = aggregate(Aggregator.fromReduce(fn))
}

object MapPreparer {
  /**
   * Create a concrete MapPreparer.
   */
  def apply[A, T](fn: A => T) = new MapPreparer[A, T] { val prepareFn = fn }

  /**
   * This is purely an optimization for the case of mapping by identity.
   * It overrides the key methods to not actually use the identity function.
   */
  def identity[A] = new MapPreparer[A, A] {
    val prepareFn = (a: A) => a
    override def map[U](fn: A => U) = MapPreparer(fn)
    override def flatMap[U](fn: A => TraversableOnce[U]) = FlatMapPreparer(fn)
    override def monoidAggregate[B, C](aggregator: MonoidAggregator[A, B, C]) = aggregator
    override def aggregate[B, C](aggregator: Aggregator[A, B, C]) = aggregator
  }
}

/**
 * A Preparer that has had one or more flatMap operations applied.
 * It can only accept MonoidAggregators.
 */
trait FlatMapPreparer[A, T] extends Preparer[A, T] {

  def prepareFn: A => TraversableOnce[T]

  def map[U](fn: T => U): FlatMapPreparer[A, U] =
    FlatMapPreparer { a: A => prepareFn(a).map(fn) }

  def flatMap[U](fn: T => TraversableOnce[U]) =
    FlatMapPreparer { a: A => prepareFn(a).flatMap(fn) }

  def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C] =
    aggregator.sumBefore.composePrepare(prepareFn)

  /**
   * alias of monoidAggregate for convenience
   * unlike MapPreparer's aggregate, can only take MonoidAggregator
   */
  def aggregate[B, C](aggregator: MonoidAggregator[T, B, C]) = monoidAggregate(aggregator)

  /**
   * Like monoidAggregate, but using an implicit Monoid to construct the Aggregator
   */
  def sum(implicit monoid: Monoid[T]) = monoidAggregate(Aggregator.fromMonoid(monoid))

  /**
   * Split the processing into two parallel aggregations.
   * You provide a function which produces two different aggregators from this preparer,
   * and it will return a single aggregator which does both aggregations in parallel.
   * (See also Aggregator's join method.)
   *
   * We really need to generate N versions of this for 3-way, 4-way etc splits.
   */

  def split[B1, B2, C1, C2](fn: FlatMapPreparer[TraversableOnce[T], T] => (MonoidAggregator[TraversableOnce[T], B1, C1], MonoidAggregator[TraversableOnce[T], B2, C2])): Aggregator[A, (B1, B2), (C1, C2)] = {
    val (a1, a2) = fn(FlatMapPreparer.identity[T])
    a1.join(a2).composePrepare(prepareFn)
  }
}

object FlatMapPreparer {
  /**
   * Create a concrete FlatMapPreparer.
   */
  def apply[A, T](fn: A => TraversableOnce[T]) = new FlatMapPreparer[A, T] { val prepareFn = fn }

  /**
   * This is purely an optimization for the case of flatMapping by identity.
   * It overrides the key methods to not actually use the identity function.
   */
  def identity[A] = new FlatMapPreparer[TraversableOnce[A], A] {
    val prepareFn = (a: TraversableOnce[A]) => a

    override def map[U](fn: A => U) =
      FlatMapPreparer{ a: TraversableOnce[A] => a.map(fn) }

    override def flatMap[U](fn: A => TraversableOnce[U]) =
      FlatMapPreparer{ a: TraversableOnce[A] => a.flatMap(fn) }

    override def monoidAggregate[B, C](aggregator: MonoidAggregator[A, B, C]) = aggregator.sumBefore
  }
}
