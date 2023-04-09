package com.twitter.algebird

trait AggregatorCompat {
   implicit def applicative[I]: Applicative[({ type L[O] = Aggregator[I, _, O] })#L] =
    new AggregatorApplicative[I]
}

/**
 * Aggregators are Applicatives, but this hides the middle type. If you need a join that does not hide the
 * middle type use join on the trait, or GeneratedTupleAggregator.fromN
 */
class AggregatorApplicative[I] extends Applicative[({ type L[O] = Aggregator[I, _, O] })#L] {
  override def map[T, U](mt: Aggregator[I, _, T])(fn: T => U): Aggregator[I, _, U] =
    mt.andThenPresent(fn)
  override def apply[T](v: T): Aggregator[I, _, T] =
    Aggregator.const(v)
  override def join[T, U](mt: Aggregator[I, _, T], mu: Aggregator[I, _, U]): Aggregator[I, _, (T, U)] =
    mt.join(mu)
  override def join[T1, T2, T3](
      m1: Aggregator[I, _, T1],
      m2: Aggregator[I, _, T2],
      m3: Aggregator[I, _, T3]
  ): Aggregator[I, _, (T1, T2, T3)] =
    GeneratedTupleAggregator.from3((m1, m2, m3))

  override def join[T1, T2, T3, T4](
      m1: Aggregator[I, _, T1],
      m2: Aggregator[I, _, T2],
      m3: Aggregator[I, _, T3],
      m4: Aggregator[I, _, T4]
  ): Aggregator[I, _, (T1, T2, T3, T4)] =
    GeneratedTupleAggregator.from4((m1, m2, m3, m4))

  override def join[T1, T2, T3, T4, T5](
      m1: Aggregator[I, _, T1],
      m2: Aggregator[I, _, T2],
      m3: Aggregator[I, _, T3],
      m4: Aggregator[I, _, T4],
      m5: Aggregator[I, _, T5]
  ): Aggregator[I, _, (T1, T2, T3, T4, T5)] =
    GeneratedTupleAggregator.from5((m1, m2, m3, m4, m5))
}
