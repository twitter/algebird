package com.twitter.algebird

/**
 * Aggregators compose well.
 *
 * To create a parallel aggregator that operates on a single
 * input in parallel, use:
 * GeneratedTupleAggregator.from2((agg1, agg2))
 */
object Aggregator extends java.io.Serializable {
  implicit def applicative[I]: Applicative[({ type L[O] = Aggregator[I, O] })#L] = new AggregatorApplicative[I]

  /**
   * This is a trivial aggregator that always returns a single value
   */
  def const[T](t: T): Aggregator[Any, T] = new MonoidAggregator[Any, T] {
    type B = Unit
    def prepare(in: Any) = ()
    val monoid = implicitly[Monoid[Unit]]
    def present(red: Unit) = t
  }
  /**
   * Using Aggregator.prepare,present you can add to this aggregator
   */
  def fromReduce[T](red: (T, T) => T): Aggregator[T, T] = fromSemigroup(Semigroup.from(red))
  def fromSemigroup[T](implicit sg: Semigroup[T]): Aggregator[T, T] = new Aggregator[T, T] {
    type B = T
    def prepare(input: T) = input
    def semigroup = sg
    def present(reduction: T) = reduction
  }
  def fromMonoid[T](implicit mon: Monoid[T]): MonoidAggregator[T, T] = fromMonoid[T, T](mon, identity[T])
  // Uses the product from the ring
  def fromRing[T](implicit rng: Ring[T]): RingAggregator[T, T] = fromRing[T, T](rng, identity[T])

  def fromMonoid[F, T](implicit mon: Monoid[T], prep: F => T): MonoidAggregator[F, T] = new MonoidAggregator[F, T] {
    type B = T
    def prepare(input: F) = prep(input)
    def monoid = mon
    def present(reduction: T) = reduction
  }
  // Uses the product from the ring
  def fromRing[F, T](implicit rng: Ring[T], prep: F => T): RingAggregator[F, T] = new RingAggregator[F, T] {
    type B = T
    def prepare(input: F) = prep(input)
    def ring = rng
    def present(reduction: T) = reduction
  }

  /**
   * How many items satisfy a predicate
   */
  def count[T](pred: T => Boolean): MonoidAggregator[T, Long] = new MonoidAggregator[T, Long] {
    type B = Long
    def prepare(t: T) = if (pred(t)) 1L else 0L
    val monoid = implicitly[Monoid[Long]]
    def present(l: B) = l
  }
  /**
   * Do any items satisfy some predicate
   */
  def exists[T](pred: T => Boolean): MonoidAggregator[T, Boolean] = new MonoidAggregator[T, Boolean] {
    type B = Boolean
    def prepare(t: T) = pred(t)
    val monoid = OrVal.unboxedMonoid
    def present(r: B) = r
  }
  /**
   * Do all items satisfy a predicate
   */
  def forall[T](pred: T => Boolean): MonoidAggregator[T, Boolean] = new MonoidAggregator[T, Boolean] {
    type B = Boolean
    def prepare(t: T) = pred(t)
    val monoid = AndVal.unboxedMonoid
    def present(r: B) = r
  }
  /**
   * Take the first (left most in reduce order) item found
   */
  def head[T]: Aggregator[T, T] = fromReduce[T] { (l, r) => l }
  /**
   * Take the last (right most in reduce order) item found
   */
  def last[T]: Aggregator[T, T] = fromReduce[T] { (l, r) => r }
  /**
   * Get the maximum item
   */
  def max[T: Ordering]: Aggregator[T, T] = new MaxAggregator[T]
  def maxBy[U, T: Ordering](fn: U => T): Aggregator[U, U] = {
    implicit val ordU = Ordering.by(fn)
    max[U]
  }
  /**
   * Get the minimum item
   */
  def min[T: Ordering]: Aggregator[T, T] = new MinAggregator[T]
  def minBy[U, T: Ordering](fn: U => T): Aggregator[U, U] = {
    implicit val ordU = Ordering.by(fn)
    min[U]
  }
  /**
   * This returns the number of items we find
   */
  def size: MonoidAggregator[Any, Long] = new MonoidAggregator[Any, Long] {
    type B = Long
    def prepare(t: Any) = 1L
    val monoid = implicitly[Monoid[Long]]
    override def present(l: B) = l
  }
  /**
   * Take the smallest `count` items using a heap
   */
  def sortedTake[T: Ordering](count: Int): MonoidAggregator[T, Seq[T]] =
    new mutable.PriorityQueueToListAggregator[T](count)
  /**
   * Take the largest `count` items using a heap
   */
  def sortedReverseTake[T: Ordering](count: Int): MonoidAggregator[T, Seq[T]] =
    new mutable.PriorityQueueToListAggregator[T](count)(implicitly[Ordering[T]].reverse)
  /**
   * Put everything in a List. Note, this could fill the memory if the List is very large.
   */
  def toList[T]: MonoidAggregator[T, List[T]] = new MonoidAggregator[T, List[T]] {
    type B = List[T]
    override def prepare(t: T) = List(t)
    val monoid = implicitly[Monoid[List[T]]]
    override def present(l: List[T]) = l
  }
  /**
   * Put everything in a Set. Note, this could fill the memory if the Set is very large.
   */
  def toSet[T]: MonoidAggregator[T, Set[T]] = new MonoidAggregator[T, Set[T]] {
    type B = Set[T]
    def prepare(t: T) = Set(t)
    val monoid = implicitly[Monoid[Set[T]]]
    override def present(l: Set[T]) = l
  }

  /**
   * This builds an in-memory Set, and then finally gets the size of that set.
   * This may not be scalable if the Uniques are very large. You might check the
   * HyperLogLog Aggregator to get an approximate version of this that is scalable.
   */
  def uniqueCount: MonoidAggregator[Any, Int] = new MonoidAggregator[Any, Int] {
    type B = Set[Any]
    def prepare(t: Any) = Set(t)
    val monoid = implicitly[Monoid[Set[Any]]]
    override def present(s: Set[Any]) = s.size
  }
}

/**
 * This is a type that models map/reduce(map). First each item is mapped,
 * then we reduce with a semigroup, then finally we present the results.
 *
 * Note, join is very useful to combine multiple aggregations with one pass.
 * Also GeneratedTupleAggregator.fromN((agg1, agg2, ... aggN)) can glue these together
 * well.
 *
 * This type is the the Fold.M from Haskell's fold package:
 * https://hackage.haskell.org/package/folds-0.6.2/docs/Data-Fold-M.html
 */
trait Aggregator[-A, +C] extends java.io.Serializable { self =>
  type B
  def prepare(input: A): B
  def semigroup: Semigroup[B]
  def present(reduction: B): C

  /* *****
   * All the following are in terms of the above
   */

  /**
   * combine two inner values
   */
  def reduce(l: B, r: B): B = semigroup.plus(l, r)
  /**
   * This may error if items is empty. To be safe you might use reduceOption
   * if you don't know that items is non-empty
   */
  def reduce(items: TraversableOnce[B]): B = semigroup.sumOption(items).get
  def reduceOption(items: TraversableOnce[B]): Option[B] = semigroup.sumOption(items)

  def apply(inputs: TraversableOnce[A]): C = present(reduce(inputs.map(prepare)))

  def append(l: B, r: A): B = reduce(l, prepare(r))

  def appendAll(old: B, items: TraversableOnce[A]): B =
    if (items.isEmpty) old else reduce(old, reduce(items.map(prepare)))

  /** Like calling andThen on the present function */
  def andThenPresent[D](present2: C => D): Aggregator[A, D] =
    new Aggregator[A, D] {
      type B = self.B
      def prepare(input: A) = self.prepare(input)
      def semigroup = self.semigroup
      def present(reduction: B) = present2(self.present(reduction))
    }
  /** Like calling compose on the prepare function */
  def composePrepare[A1](prepare2: A1 => A): Aggregator[A1, C] =
    new Aggregator[A1, C] {
      type B = self.B
      def prepare(input: A1) = self.prepare(prepare2(input))
      def semigroup = self.semigroup
      def present(reduction: B) = self.present(reduction)
    }

  /**
   * This allows you to run two aggregators on the same data with a single pass
   */
  def join[A2 <: A, C2](that: Aggregator[A2, C2]): Aggregator[A2, (C, C2)] =
    GeneratedTupleAggregator.from2((this, that))

  /**
   * An Aggregator can be converted to a Fold, but not vice-versa
   * Note, a Fold is more constrained so only do this if you require
   * joining a Fold with an Aggregator to produce a Fold
   */
  def toFold: Fold[A, Option[C]] = Fold.fold[Option[B], A, Option[C]](
    {
      case (None, a) => Some(self.prepare(a))
      case (Some(b), a) => Some(self.append(b, a))
    },
    None,
    { _.map(self.present(_)) })
}

/**
 * Aggregators are Applicatives!
 */
class AggregatorApplicative[I] extends Applicative[({ type L[O] = Aggregator[I, O] })#L] {
  override def map[T, U](mt: Aggregator[I, T])(fn: T => U): Aggregator[I, U] =
    mt.andThenPresent(fn)
  override def apply[T](v: T): Aggregator[I, T] =
    Aggregator.const(v)
  override def join[T, U](mt: Aggregator[I, T], mu: Aggregator[I, U]): Aggregator[I, (T, U)] =
    mt.join(mu)
  override def join[T1, T2, T3](m1: Aggregator[I, T1], m2: Aggregator[I, T2], m3: Aggregator[I, T3]): Aggregator[I, (T1, T2, T3)] =
    GeneratedTupleAggregator.from3(m1, m2, m3)
  override def join[T1, T2, T3, T4](m1: Aggregator[I, T1], m2: Aggregator[I, T2], m3: Aggregator[I, T3], m4: Aggregator[I, T4]): Aggregator[I, (T1, T2, T3, T4)] =
    GeneratedTupleAggregator.from4(m1, m2, m3, m4)
  override def join[T1, T2, T3, T4, T5](m1: Aggregator[I, T1], m2: Aggregator[I, T2], m3: Aggregator[I, T3], m4: Aggregator[I, T4], m5: Aggregator[I, T5]): Aggregator[I, (T1, T2, T3, T4, T5)] =
    GeneratedTupleAggregator.from5(m1, m2, m3, m4, m5)
}

trait MonoidAggregator[-A, +C] extends Aggregator[A, C] {
  def monoid: Monoid[B]
  def semigroup = monoid
  final override def reduce(items: TraversableOnce[B]): B =
    monoid.sum(items)

  def appendAll(items: TraversableOnce[A]): B = reduce(items.map(prepare))
}

trait RingAggregator[-A, +C] extends MonoidAggregator[A, C] {
  def ring: Ring[B]
  def monoid = Ring.asTimesMonoid(ring)
}
