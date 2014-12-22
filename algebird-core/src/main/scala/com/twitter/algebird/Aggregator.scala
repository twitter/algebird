package com.twitter.algebird

import java.util.PriorityQueue
import scala.collection.TraversableLike
import scala.collection.generic.CanBuildFrom

/**
 * Aggregators compose well.
 *
 * To create a parallel aggregator that operates on a single
 * input in parallel, use:
 * GeneratedTupleAggregator.from2((agg1, agg2))
 */
object Aggregator extends java.io.Serializable {
  implicit def applicative[I]: Applicative[({ type L[O] = Aggregator[I, _, O] })#L] = new AggregatorApplicative[I]

  /**
   * This is a trivial aggregator that always returns a single value
   */
  def const[T](t: T): MonoidAggregator[Any, Unit, T] =
    prepareMonoid { _: Any => () }
      .andThenPresent(_ => t)
  /**
   * Using Aggregator.prepare,present you can add to this aggregator
   */
  def fromReduce[T](red: (T, T) => T): Aggregator[T, T, T] = fromSemigroup(Semigroup.from(red))
  def fromSemigroup[T](implicit sg: Semigroup[T]): Aggregator[T, T, T] = new Aggregator[T, T, T] {
    def prepare(input: T) = input
    def semigroup = sg
    def present(reduction: T) = reduction
  }
  def fromMonoid[T](implicit mon: Monoid[T]): MonoidAggregator[T, T, T] = prepareMonoid(identity[T])
  // Uses the product from the ring
  def fromRing[T](implicit rng: Ring[T]): RingAggregator[T, T, T] = fromRing[T, T](rng, identity[T])

  def fromMonoid[F, T](implicit mon: Monoid[T], prep: F => T): MonoidAggregator[F, T, T] =
    prepareMonoid(prep)(mon)

  def prepareMonoid[F, T](prep: F => T)(implicit m: Monoid[T]): MonoidAggregator[F, T, T] = new MonoidAggregator[F, T, T] {
    def prepare(input: F) = prep(input)
    def monoid = m
    def present(reduction: T) = reduction
  }
  // Uses the product from the ring
  def fromRing[F, T](implicit rng: Ring[T], prep: F => T): RingAggregator[F, T, T] = new RingAggregator[F, T, T] {
    def prepare(input: F) = prep(input)
    def ring = rng
    def present(reduction: T) = reduction
  }

  /**
   * How many items satisfy a predicate
   */
  def count[T](pred: T => Boolean): MonoidAggregator[T, Long, Long] =
    prepareMonoid { t: T => if (pred(t)) 1L else 0L }

  /**
   * Do any items satisfy some predicate
   */
  def exists[T](pred: T => Boolean): MonoidAggregator[T, Boolean, Boolean] =
    prepareMonoid(pred)(OrVal.unboxedMonoid)
  /**
   * Do all items satisfy a predicate
   */
  def forall[T](pred: T => Boolean): MonoidAggregator[T, Boolean, Boolean] =
    prepareMonoid(pred)(AndVal.unboxedMonoid)
  /**
   * Take the first (left most in reduce order) item found
   */
  def head[T]: Aggregator[T, T, T] = fromReduce[T] { (l, r) => l }
  /**
   * Take the last (right most in reduce order) item found
   */
  def last[T]: Aggregator[T, T, T] = fromReduce[T] { (l, r) => r }
  /**
   * Get the maximum item
   */
  def max[T: Ordering]: Aggregator[T, T, T] = new MaxAggregator[T]
  def maxBy[U, T: Ordering](fn: U => T): Aggregator[U, U, U] = {
    implicit val ordU = Ordering.by(fn)
    max[U]
  }
  /**
   * Get the minimum item
   */
  def min[T: Ordering]: Aggregator[T, T, T] = new MinAggregator[T]
  def minBy[U, T: Ordering](fn: U => T): Aggregator[U, U, U] = {
    implicit val ordU = Ordering.by(fn)
    min[U]
  }
  /**
   * This returns the number of items we find
   */
  def size: MonoidAggregator[Any, Long, Long] =
    prepareMonoid { (_: Any) => 1L }
  /**
   * Take the smallest `count` items using a heap
   */
  def sortedTake[T: Ordering](count: Int): MonoidAggregator[T, PriorityQueue[T], Seq[T]] =
    new mutable.PriorityQueueToListAggregator[T](count)
  /**
   * Take the largest `count` items using a heap
   */
  def sortedReverseTake[T: Ordering](count: Int): MonoidAggregator[T, PriorityQueue[T], Seq[T]] =
    new mutable.PriorityQueueToListAggregator[T](count)(implicitly[Ordering[T]].reverse)
  /**
   * Put everything in a List. Note, this could fill the memory if the List is very large.
   */
  def toList[T]: MonoidAggregator[T, List[T], List[T]] =
    prepareMonoid { t: T => List(t) }
  /**
   * Put everything in a Set. Note, this could fill the memory if the Set is very large.
   */
  def toSet[T]: MonoidAggregator[T, Set[T], Set[T]] =
    prepareMonoid { t: T => Set(t) }

  /**
   * This builds an in-memory Set, and then finally gets the size of that set.
   * This may not be scalable if the Uniques are very large. You might check the
   * HyperLogLog Aggregator to get an approximate version of this that is scalable.
   */
  def uniqueCount[T]: MonoidAggregator[T, Set[T], Int] =
    toSet[T].andThenPresent(_.size)
}

/**
 * This is a type that models map/reduce(map). First each item is mapped,
 * then we reduce with a semigroup, then finally we present the results.
 *
 * Unlike Fold, Aggregator keeps it's middle aggregation type externally visible.
 * This is because Aggregators are useful in parallel map/reduce systems where
 * there may be some additional types needed to cross the map/reduce boundary
 * (such a serialization and intermediate storage). If you don't care about the
 * middle type, an _ may be used and the main utility of the instance is still
 * preserved (e.g. def operate[T](ag: Aggregator[T, _, Int]): Int)
 *
 * Note, join is very useful to combine multiple aggregations with one pass.
 * Also GeneratedTupleAggregator.fromN((agg1, agg2, ... aggN)) can glue these
 * together well.
 *
 * This type is the the Fold.M from Haskell's fold package:
 * https://hackage.haskell.org/package/folds-0.6.2/docs/Data-Fold-M.html
 */
trait Aggregator[-A, B, +C] extends java.io.Serializable { self =>
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
  /**
   * This is the safe version of the above. If the input in empty, return None,
   * else reduce the items
   */
  def reduceOption(items: TraversableOnce[B]): Option[B] = semigroup.sumOption(items)

  /**
   * This may error if inputs are empty (for Monoid Aggregators it never will, instead
   * you see present(Monoid.zero[B])
   */
  def apply(inputs: TraversableOnce[A]): C = present(reduce(inputs.map(prepare)))
  /**
   * This returns None if the inputs are empty
   */
  def applyOption(inputs: TraversableOnce[A]): Option[C] =
    reduceOption(inputs.map(prepare))
      .map(present)

  /**
   * This returns the cumulative sum of its inputs, in the same order.
   * If the inputs are empty, the result will be empty too.
   */
  def applyCumulative[In <: TraversableLike[A, In], Out](inputs: In)(implicit bf: CanBuildFrom[In, C, Out]): Out = {
    val results =
      inputs
        .toIterator
        .scanLeft(None: Option[B]) {
          case (None, a) => Some(prepare(a))
          case (Some(b), a) => Some(append(b, a))
        }
        .collect { case Some(b) => present(b) }

    val builder = bf()
    builder ++= results
    builder.result
  }

  def append(l: B, r: A): B = reduce(l, prepare(r))

  def appendAll(old: B, items: TraversableOnce[A]): B =
    if (items.isEmpty) old else reduce(old, reduce(items.map(prepare)))

  /** Like calling andThen on the present function */
  def andThenPresent[D](present2: C => D): Aggregator[A, B, D] =
    new Aggregator[A, B, D] {
      def prepare(input: A) = self.prepare(input)
      def semigroup = self.semigroup
      def present(reduction: B) = present2(self.present(reduction))
    }
  /** Like calling compose on the prepare function */
  def composePrepare[A1](prepare2: A1 => A): Aggregator[A1, B, C] =
    new Aggregator[A1, B, C] {
      def prepare(input: A1) = self.prepare(prepare2(input))
      def semigroup = self.semigroup
      def present(reduction: B) = self.present(reduction)
    }

  /**
   * This allows you to run two aggregators on the same data with a single pass
   */
  def join[A2 <: A, B2, C2](that: Aggregator[A2, B2, C2]): Aggregator[A2, (B, B2), (C, C2)] =
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

  def lift: MonoidAggregator[A, Option[B], Option[C]] =
    new MonoidAggregator[A, Option[B], Option[C]] {
      def prepare(input: A): Option[B] = Some(self.prepare(input))
      def present(reduction: Option[B]): Option[C] = reduction.map(self.present)
      def monoid = new OptionMonoid[B]()(self.semigroup)
    }
}

/**
 * Aggregators are Applicatives, but this hides the middle type. If you need a join that
 * does not hide the middle type use join on the trait, or GeneratedTupleAggregator.fromN
 */
class AggregatorApplicative[I] extends Applicative[({ type L[O] = Aggregator[I, _, O] })#L] {
  override def map[T, U](mt: Aggregator[I, _, T])(fn: T => U): Aggregator[I, _, U] =
    mt.andThenPresent(fn)
  override def apply[T](v: T): Aggregator[I, _, T] =
    Aggregator.const(v)
  override def join[T, U](mt: Aggregator[I, _, T], mu: Aggregator[I, _, U]): Aggregator[I, _, (T, U)] =
    mt.join(mu)
  override def join[T1, T2, T3](m1: Aggregator[I, _, T1],
    m2: Aggregator[I, _, T2],
    m3: Aggregator[I, _, T3]): Aggregator[I, _, (T1, T2, T3)] =
    GeneratedTupleAggregator.from3(m1, m2, m3)

  override def join[T1, T2, T3, T4](m1: Aggregator[I, _, T1],
    m2: Aggregator[I, _, T2],
    m3: Aggregator[I, _, T3],
    m4: Aggregator[I, _, T4]): Aggregator[I, _, (T1, T2, T3, T4)] =
    GeneratedTupleAggregator.from4(m1, m2, m3, m4)

  override def join[T1, T2, T3, T4, T5](m1: Aggregator[I, _, T1],
    m2: Aggregator[I, _, T2],
    m3: Aggregator[I, _, T3],
    m4: Aggregator[I, _, T4],
    m5: Aggregator[I, _, T5]): Aggregator[I, _, (T1, T2, T3, T4, T5)] =
    GeneratedTupleAggregator.from5(m1, m2, m3, m4, m5)
}

trait MonoidAggregator[-A, B, +C] extends Aggregator[A, B, C] { self =>
  def monoid: Monoid[B]
  def semigroup = monoid
  final override def reduce(items: TraversableOnce[B]): B =
    monoid.sum(items)

  def appendAll(items: TraversableOnce[A]): B = reduce(items.map(prepare))

  override def andThenPresent[D](present2: C => D): MonoidAggregator[A, B, D] = {
    val self = this
    new MonoidAggregator[A, B, D] {
      def prepare(a: A) = self.prepare(a)
      def monoid = self.monoid
      def present(b: B) = present2(self.present(b))
    }
  }
  override def composePrepare[A2](prepare2: A2 => A): MonoidAggregator[A2, B, C] = {
    val self = this
    new MonoidAggregator[A2, B, C] {
      def prepare(a: A2) = self.prepare(prepare2(a))
      def monoid = self.monoid
      def present(b: B) = self.present(b)
    }
  }

  def sumBefore: MonoidAggregator[TraversableOnce[A], B, C] =
    new MonoidAggregator[TraversableOnce[A], B, C] {
      def monoid: Monoid[B] = self.monoid
      def prepare(input: TraversableOnce[A]): B = monoid.sum(input.map(self.prepare))
      def present(reduction: B): C = self.present(reduction)
    }
}

trait RingAggregator[-A, B, +C] extends MonoidAggregator[A, B, C] {
  def ring: Ring[B]
  def monoid = Ring.asTimesMonoid(ring)
}
