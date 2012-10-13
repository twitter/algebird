package com.twitter.algebird

trait Aggregator[A,B,C] {
  def prepare(input : A) : B
  def reduce(l : B, r : B) : B
  def present(reduction : B) : C

  def aggregate(inputs : Seq[A]) : C = present(reduce(inputs.map{prepare(_)}))
  def reduce(items : Seq[B]) : B = items.reduce{reduce(_,_)}
}

abstract class MonoidAggregator[A,B,C](monoid : Monoid[B]) extends Aggregator[A,B,C] {
  def reduce(l : B, r : B) : B = monoid.plus(l, r)
  override def reduce(items : Seq[B]) : B = items.foldLeft(monoid.zero){reduce(_,_)}
}

object Averager extends MonoidAggregator[Double, AveragedValue, Double](AveragedGroup) {
  def prepare(value : Double) = AveragedValue(value)
  def present(average : AveragedValue) = average.value
}