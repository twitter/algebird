package com.twitter.algebird

/**
 * Aggregators compose well.
 *
 * To create a parallel aggregator that operates on a single
 * input in parallel, use:
 * GeneratedTupleAggregator.from2((agg1, agg2))
 */
object Aggregator extends java.io.Serializable {
  /** Using Aggregator.prepare,present you can add to this aggregator
   */
  def fromReduce[T](red: (T,T) => T): Aggregator[T,T,T] = new Aggregator[T,T,T] {
    def prepare(input : T) = input
    def reduce(l : T, r : T) = red(l, r)
    def present(reduction : T) = reduction
  }
  def fromSemigroup[T](implicit sg: Semigroup[T]): Aggregator[T,T,T] = new Aggregator[T,T,T] {
    def prepare(input : T) = input
    def reduce(l : T, r : T) = sg.plus(l, r)
    def present(reduction : T) = reduction
  }
  def fromMonoid[T](implicit mon: Monoid[T]): MonoidAggregator[T,T,T] = new MonoidAggregator[T,T,T] {
    def prepare(input : T) = input
    def monoid = mon
    def present(reduction : T) = reduction
  }
  // Uses the product from the ring
  def fromRing[T](implicit rng: Ring[T]): RingAggregator[T,T,T] = new RingAggregator[T,T,T] {
    def prepare(input : T) = input
    def ring = rng
    def present(reduction : T) = reduction
  }
}

trait Aggregator[-A,B,+C] extends Function1[TraversableOnce[A], C] with java.io.Serializable { self =>
  def prepare(input : A) : B
  def reduce(l : B, r : B) : B
  def present(reduction : B) : C

  def reduce(items : TraversableOnce[B]) : B = items.reduce{reduce(_,_)}
  def apply(inputs : TraversableOnce[A]) : C = present(reduce(inputs.map{prepare(_)}))

  /** Like calling andThen on the present function */
  def andThenPresent[D](present2: C => D): Aggregator[A,B,D] =
    new Aggregator[A,B,D] {
      def prepare(input : A) = self.prepare(input)
      def reduce(l : B, r : B) = self.reduce(l, r)
      def present(reduction : B) = present2(self.present(reduction))
    }
  /** Like calling compose on the prepare function */
  def composePrepare[A1](prepare2: A1 => A): Aggregator[A1,B,C] =
    new Aggregator[A1,B,C] {
      def prepare(input : A1) = self.prepare(prepare2(input))
      def reduce(l : B, r : B) = self.reduce(l, r)
      def present(reduction : B) = self.present(reduction)
    }
}

trait MonoidAggregator[-A,B,+C] extends Aggregator[A,B,C] {
  def monoid : Monoid[B]
  final def reduce(l : B, r : B) : B = monoid.plus(l, r)
  final override def reduce(items : TraversableOnce[B]) : B =
    monoid.sum(items)
}

trait RingAggregator[-A,B,+C] extends Aggregator[A,B,C] {
  def ring: Ring[B]
  final def reduce(l: B, r: B): B = ring.times(l,r)
  final override def reduce(items : TraversableOnce[B]) : B =
    if(items.isEmpty) ring.one // There are several pseudo-rings, so avoid one if you can
    else items.reduceLeft(reduce _)
}
