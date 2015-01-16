package com.twitter.algebird

trait Preparer[A, T, +This[A, T] <: Preparer[A, T, This]] extends HasMonoidAggregate[A, T] {
  def map[U](fn: T => U): This[A, U]

  def flatMap[U](fn: T => TraversableOnce[U]): FlatPreparer[A, U]

  //really ought to generate N versions of this...
  def split[B1, B2, C1, C2](fn: This[A, T] => (Aggregator[A, B1, C1], Aggregator[A, B2, C2])): Aggregator[A, (B1, B2), (C1, C2)] = {
    val (a1, a2) = fn(this.asInstanceOf[This[A, T]])
    a1.join(a2)
  }
}

object Preparer {
  def apply[A]: SimplePreparer[A, A] = SimplePreparer[A, A](identity)
}

case class SimplePreparer[A, T](prepareFn: A => T)
  extends Preparer[A, T, SimplePreparer]
  with HasAggregate[A, T] {

  def map[U](fn: T => U) =
    SimplePreparer[A, U](fn.compose(prepareFn))

  def flatMap[U](fn: T => TraversableOnce[U]) =
    FlatPreparer[A, U](fn.compose(prepareFn))

  def aggregate[B, C](aggregator: Aggregator[T, B, C]): Aggregator[A, B, C] =
    aggregator.composePrepare(prepareFn)

  def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C] =
    aggregator.composePrepare(prepareFn)
}

case class FlatPreparer[A, T](prepareFn: A => TraversableOnce[T])
  extends Preparer[A, T, FlatPreparer]
  with HasLift[A, T] {
  def map[U](fn: T => U) =
    FlatPreparer { a: A => prepareFn(a).map(fn) }

  def flatMap[U](fn: T => TraversableOnce[U]) =
    FlatPreparer { a: A => prepareFn(a).flatMap(fn) }

  def monoidAggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C] =
    aggregator.sumBefore.composePrepare(prepareFn)

  //alias for convenience
  def aggregate[B, C](aggregator: MonoidAggregator[T, B, C]) = monoidAggregate(aggregator)
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
}

trait HasLift[A, T] extends HasMonoidAggregate[A, T] {
  def lift[B, C](aggregator: Aggregator[T, B, C]): MonoidAggregator[A, Option[B], Option[C]] =
    monoidAggregate(aggregator.lift)

  def head = lift(Aggregator.head)
  def last = lift(Aggregator.last)
  def max(implicit ord: Ordering[T]) = lift(Aggregator.max)
  def maxBy[U: Ordering](fn: T => U) = {
    implicit val ordT = Ordering.by(fn)
    lift(Aggregator.max[T])
  }

  def min(implicit ord: Ordering[T]) = lift(Aggregator.min)
  def minBy[U: Ordering](fn: T => U) = {
    implicit val ordT = Ordering.by(fn)
    lift(Aggregator.min[T])
  }
}
