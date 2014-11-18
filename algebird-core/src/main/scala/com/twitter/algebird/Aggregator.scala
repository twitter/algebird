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
  def const[T](t: T): Aggregator[Any, T] = new Aggregator[Any, T] {
    type B = Unit
    def prepare(in: Any) = ()
    def reduce(l: Unit, r: Unit) = ()
    def present(red: Unit) = t
    override def reduce(us: TraversableOnce[Unit]) = ()
  }
  /**
   * Using Aggregator.prepare,present you can add to this aggregator
   */
  def fromReduce[T](red: (T, T) => T): Aggregator[T, T] = new Aggregator[T, T] {
    type B = T
    def prepare(input: T) = input
    def reduce(l: T, r: T) = red(l, r)
    def present(reduction: T) = reduction
  }
  def fromSemigroup[T](implicit sg: Semigroup[T]): Aggregator[T, T] = new Aggregator[T, T] {
    type B = T
    def prepare(input: T) = input
    def reduce(l: T, r: T) = sg.plus(l, r)
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

trait Aggregator[-A, +C] extends java.io.Serializable { self =>
  type B
  def prepare(input: A): B
  def reduce(l: B, r: B): B
  def present(reduction: B): C

  def reduce(items: TraversableOnce[B]): B = items.reduce{ reduce(_, _) }
  def apply(inputs: TraversableOnce[A]): C = present(reduce(inputs.map{ prepare(_) }))

  def append(l: B, r: A): B = reduce(l, prepare(r))

  def appendAll(old: B, items: TraversableOnce[A]): B =
    if (items.isEmpty) old else reduce(old, reduce(items.map(prepare)))

  /** Like calling andThen on the present function */
  def andThenPresent[D](present2: C => D): Aggregator[A, D] =
    new Aggregator[A, D] {
      type B = self.B
      def prepare(input: A) = self.prepare(input)
      def reduce(l: B, r: B) = self.reduce(l, r)
      def present(reduction: B) = present2(self.present(reduction))
    }
  /** Like calling compose on the prepare function */
  def composePrepare[A1](prepare2: A1 => A): Aggregator[A1, C] =
    new Aggregator[A1, C] {
      type B = self.B
      def prepare(input: A1) = self.prepare(prepare2(input))
      def reduce(l: B, r: B) = self.reduce(l, r)
      def present(reduction: B) = self.present(reduction)
    }

  /**
   * This allows you to run two aggregators on the same data with a single pass
   */
  def join[A2 <: A, C2](that: Aggregator[A2, C2]): Aggregator[A2, (C, C2)] =
    new Aggregator[A2, (C, C2)] {
      type B = (self.B, that.B)
      def prepare(in: A2) = (self.prepare(in), that.prepare(in))
      def reduce(l: B, r: B) = (self.reduce(l._1, r._1), that.reduce(l._2, r._2))
      // TODO override def reduce(t: TraversableOnce[B]) to call the underlying methods
      def present(red: B) = (self.present(red._1), that.present(red._2))
    }

  /**
   * An Aggregator can be converted to a Fold, but not vice-versa
   * Note, a Fold is more constrained so only do this if you require
   * joining a Fold with an applicative.
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
}

trait MonoidAggregator[-A, +C] extends Aggregator[A, C] {
  def monoid: Monoid[B]
  final def reduce(l: B, r: B): B = monoid.plus(l, r)
  final override def reduce(items: TraversableOnce[B]): B =
    monoid.sum(items)

  def appendAll(items: TraversableOnce[A]): B = appendAll(monoid.zero, items)
}

trait RingAggregator[-A, +C] extends Aggregator[A, C] {
  def ring: Ring[B]
  final def reduce(l: B, r: B): B = ring.times(l, r)
  final override def reduce(items: TraversableOnce[B]): B =
    if (items.isEmpty) ring.one // There are several pseudo-rings, so avoid one if you can
    else items.reduceLeft(reduce _)

  def appendAll(items: TraversableOnce[A]): B = appendAll(ring.one, items)
}
