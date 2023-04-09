package com.twitter.algebird

class ScanApplicative[I] extends Applicative[Scan[I, _]] {
  override def map[T, U](mt: Scan[I, T])(fn: T => U): Scan[I, U] =
    mt.andThenPresent(fn)

  override def apply[T](v: T): Scan[I, T] =
    Scan.const(v)

  override def join[T, U](mt: Scan[I, T], mu: Scan[I, U]): Scan[I, (T, U)] =
    mt.join(mu)
}

private[algebird] trait ScanApplicativeCompat {
  implicit def applicative[I]: Applicative[Scan[I, _]] = new ScanApplicative[I]
}
