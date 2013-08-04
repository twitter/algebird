package com.twitter.algebird

object Aggregator extends java.io.Serializable {
  /** We could autogenerate 22 of these, and if find ourselves using them we should.
   * in the mean time, if you need 3, then compose twice: ((C1,C2),C3) and
   * use andThenPresent to map to (C1, C2, C3)
   */
  def compose[A,B1,C1,B2,C2](agg1: Aggregator[A,B1,C1], agg2: Aggregator[A,B2,C2]): Aggregator[A,(B1,B2),(C1,C2)] =
    new Aggregator2(agg1, agg2)

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

/** We make a non-anonymous class with public access to the underlying aggregators as
 * they may be useful for a runtime optimization
 */
class Aggregator2[A,B1,B2,C1,C2](val agg1: Aggregator[A,B1,C1], val agg2: Aggregator[A,B2,C2]) extends Aggregator[A,(B1,B2),(C1,C2)] {
  def prepare(input : A) = (agg1.prepare(input), agg2.prepare(input))
  def reduce(l : (B1,B2), r : (B1,B2)) = {
    val b1next = agg1.reduce(l._1, r._1)
    val b2next = agg2.reduce(l._2, r._2)
    (b1next, b2next)
  }
  def present(reduction : (B1,B2)) = (agg1.present(reduction._1), agg2.present(reduction._2))
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
    if(items.isEmpty) monoid.zero
    else items.reduceLeft(reduce _)
}

trait RingAggregator[-A,B,+C] extends Aggregator[A,B,C] {
  def ring: Ring[B]
  final def reduce(l: B, r: B): B = ring.times(l,r)
  final override def reduce(items : TraversableOnce[B]) : B =
    if(items.isEmpty) ring.one // There are several pseudo-rings, so avoid one if you can
    else items.reduceLeft(reduce _)
}
