package com.twitter.algebird

private[algebird] trait AggregatorCompat {
  implicit def applicative[I]: Applicative[({ type L[O] = Aggregator[I, ?, O] })#L] =
    new AggregatorApplicative[I]
}

/**
 * Aggregators are Applicatives, but this hides the middle type. If you need a join that does not hide the
 * middle type use join on the trait, or GeneratedTupleAggregator.fromN
 */
class AggregatorApplicative[I] extends Applicative[({ type L[O] = Aggregator[I, ?, O] })#L] {
  override def map[T, U](mt: Aggregator[I, ?, T])(fn: T => U): Aggregator[I, ?, U] =
    mt.andThenPresent(fn)
  override def apply[T](v: T): Aggregator[I, ?, T] =
    Aggregator.const(v)
  override def join[T, U](mt: Aggregator[I, ?, T], mu: Aggregator[I, ?, U]): Aggregator[I, ?, (T, U)] =
    mt.join(mu)
  override def join[T1, T2, T3](
      m1: Aggregator[I, ?, T1],
      m2: Aggregator[I, ?, T2],
      m3: Aggregator[I, ?, T3]
  ): Aggregator[I, ?, (T1, T2, T3)] =
    GeneratedTupleAggregator.from3((m1, m2, m3))

  override def join[T1, T2, T3, T4](
      m1: Aggregator[I, ?, T1],
      m2: Aggregator[I, ?, T2],
      m3: Aggregator[I, ?, T3],
      m4: Aggregator[I, ?, T4]
  ): Aggregator[I, ?, (T1, T2, T3, T4)] =
    GeneratedTupleAggregator.from4((m1, m2, m3, m4))

  override def join[T1, T2, T3, T4, T5](
      m1: Aggregator[I, ?, T1],
      m2: Aggregator[I, ?, T2],
      m3: Aggregator[I, ?, T3],
      m4: Aggregator[I, ?, T4],
      m5: Aggregator[I, ?, T5]
  ): Aggregator[I, ?, (T1, T2, T3, T4, T5)] =
    GeneratedTupleAggregator.from5((m1, m2, m3, m4, m5))
}
