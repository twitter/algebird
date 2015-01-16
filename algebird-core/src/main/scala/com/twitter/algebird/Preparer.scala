package com.twitter.algebird

trait Preparer[A, T, +This[A, T] <: Preparer[A, T, This]] extends HasMonoidAggregate[A, T] {
  def map[U](fn: T => U): This[A, U]

  def flatMap[U](fn: T => TraversableOnce[U]): FlatMapPreparer[A, U]

  def flatten[U](implicit ev: <:<[T, TraversableOnce[U]]) = flatMap(ev)

  //really ought to generate N versions of this...
  def split[B1, B2, C1, C2](fn: This[A, T] => (Aggregator[A, B1, C1], Aggregator[A, B2, C2])): Aggregator[A, (B1, B2), (C1, C2)] = {
    val (a1, a2) = fn(this.asInstanceOf[This[A, T]])
    a1.join(a2)
  }
}

object Preparer {
  def apply[A] = MapPreparer[A, A](identity)
}

case class MapPreparer[A, T](prepareFn: A => T)
  extends Preparer[A, T, MapPreparer]
  with HasAggregate[A, T] {

  def map[U](fn: T => U) =
    MapPreparer[A, U](fn.compose(prepareFn))

  def flatMap[U](fn: T => TraversableOnce[U]) =
    FlatMapPreparer[A, U](fn.compose(prepareFn))

  def aggregate[B, C](aggregator: Aggregator[T, B, C]): Aggregator[A, B, C] =
    aggregator.composePrepare(prepareFn)

  def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C] =
    aggregator.composePrepare(prepareFn)
}

case class FlatMapPreparer[A, T](prepareFn: A => TraversableOnce[T])
  extends Preparer[A, T, FlatMapPreparer]
  with HasLift[A, T] {
  def map[U](fn: T => U) =
    FlatMapPreparer { a: A => prepareFn(a).map(fn) }

  def flatMap[U](fn: T => TraversableOnce[U]) =
    FlatMapPreparer { a: A => prepareFn(a).flatMap(fn) }

  def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C] =
    aggregator.sumBefore.composePrepare(prepareFn)

  //alias for convenience
  def aggregate[B, C](aggregator: MonoidAggregator[T, B, C]) = monoidAggregate(aggregator)

  def sum(implicit monoid: Monoid[T]) = monoidAggregate(Aggregator.fromMonoid(monoid))
}

trait HasMonoidAggregate[A, T] {
  def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C]

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
}

trait HasAggregate[A, T] {
  def aggregate[B, C](aggregator: Aggregator[T, B, C]): Aggregator[A, B, C]

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

trait HasLift[A, T] extends HasMonoidAggregate[A, T] {
  def lift[B, C](aggregator: Aggregator[T, B, C]): MonoidAggregator[A, Option[B], Option[C]] =
    monoidAggregate(aggregator.lift)

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

//implementing unzip as an enrichment because it seems to help with type inference
//ideally we'd generate N versions of this, too
//and figure out how to make it work with FlatMapPreparer
case class Unzipper[A, U, V](preparer: MapPreparer[A, (U, V)]) {
  def unzip[B1, B2, C1, C2](fn: (MapPreparer[U, U], MapPreparer[V, V]) => (Aggregator[U, B1, C1], Aggregator[V, B2, C2])) = {
    val (a1, a2) = fn(Preparer[U], Preparer[V])
    preparer.aggregate(a1.zip(a2))
  }
}

//for some reason this implicit doesn't get picked up automatically?
object Unzipper {
  implicit def unzipper[A, U, V](preparer: MapPreparer[A, (U, V)]): Unzipper[A, U, V] = Unzipper(preparer)
}

