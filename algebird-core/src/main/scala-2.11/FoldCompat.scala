package com.twitter.algebird

trait FoldApplicativeCompat {
  implicit def applicative[I]: Applicative[Fold[I, *]] =
    new FoldApplicative[I]
}

/**
 * Folds are Applicatives!
 */
class FoldApplicative[I] extends Applicative[Fold[I, *]] {
  override def map[T, U](mt: Fold[I, T])(fn: T => U): Fold[I, U] =
    mt.map(fn)
  override def apply[T](v: T): Fold[I, T] =
    Fold.const(v)
  override def join[T, U](mt: Fold[I, T], mu: Fold[I, U]): Fold[I, (T, U)] =
    mt.join(mu)
  override def sequence[T](ms: Seq[Fold[I, T]]): Fold[I, Seq[T]] =
    Fold.sequence(ms)
  override def joinWith[T, U, V](mt: Fold[I, T], mu: Fold[I, U])(fn: (T, U) => V): Fold[I, V] =
    mt.joinWith(mu)(fn)
}
