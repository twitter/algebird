package com.twitter.algebird

case class Preparer[A, T](prepareFn: A => T) {
  def map[U](fn: T => U) =
    Preparer[A, U](fn.compose(prepareFn))

  def flatMap[U](fn: T => TraversableOnce[U]) =
    FlatPreparer[A, U](fn.compose(prepareFn))

  def aggregate[B, C](aggregator: Aggregator[T, B, C]): Aggregator[A, B, C] =
    aggregator.composePrepare(prepareFn)

  def split[B1, B2, C1, C2](fn: Preparer[A, T] => (Aggregator[A, B1, C1], Aggregator[A, B2, C2])): Aggregator[A, (B1, B2), (C1, C2)] = {
    val (a1, a2) = fn(this)
    a1.join(a2)
  }
}

object Preparer {
  def apply[A]: Preparer[A, A] = Preparer[A, A](identity)
}

case class FlatPreparer[A, T](prepareFn: A => TraversableOnce[T]) {
  def map[U](fn: T => U): FlatPreparer[A, U] =
    FlatPreparer { a: A => prepareFn(a).map(fn) }

  def flatMap[U](fn: T => TraversableOnce[U]): FlatPreparer[A, U] =
    FlatPreparer { a: A => prepareFn(a).flatMap(fn) }

  def aggregate[B, C](aggregator: MonoidAggregator[T, B, C]): MonoidAggregator[A, B, C] =
    aggregator.sumBefore.composePrepare(prepareFn)

  def split[B1, B2, C1, C2](fn: FlatPreparer[A, T] => (Aggregator[A, B1, C1], Aggregator[A, B2, C2])): Aggregator[A, (B1, B2), (C1, C2)] = {
    val (a1, a2) = fn(this)
    a1.join(a2)
  }
}
