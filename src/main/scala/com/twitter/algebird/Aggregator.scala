package com.twitter.algebird

trait Aggregator[A,B,C] extends Function1[TraversableOnce[A], C] with java.io.Serializable {
  def prepare(input : A) : B
  def reduce(l : B, r : B) : B
  def present(reduction : B) : C

  def reduce(items : TraversableOnce[B]) : B = items.reduce{reduce(_,_)}
  def apply(inputs : TraversableOnce[A]) : C = present(reduce(inputs.map{prepare(_)}))
}

trait MonoidAggregator[A,B,C] extends Aggregator[A,B,C] {
  def monoid : Monoid[B]
  def reduce(l : B, r : B) : B = monoid.plus(l, r)
  override def reduce(items : TraversableOnce[B]) : B = items.foldLeft(monoid.zero){reduce(_,_)}
}

object Averager extends MonoidAggregator[Double, AveragedValue, Double] {
  val monoid = AveragedGroup
  def prepare(value : Double) = AveragedValue(value)
  def present(average : AveragedValue) = average.value
}