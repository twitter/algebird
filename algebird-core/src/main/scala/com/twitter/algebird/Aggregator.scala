package com.twitter.algebird

trait Aggregator[-A,B,+C] extends Function1[TraversableOnce[A], C] with java.io.Serializable {
  def prepare(input : A) : B
  def reduce(l : B, r : B) : B
  def present(reduction : B) : C

  def reduce(items : TraversableOnce[B]) : B = items.reduce{reduce(_,_)}
  def apply(inputs : TraversableOnce[A]) : C = present(reduce(inputs.map{prepare(_)}))
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
